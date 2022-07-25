package topology

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowRequest struct {
	Option *VolumeGrowOption
	Count  int
	ErrCh  chan error
}

type VolumeGrowOption struct {
	Collection         string                        `json:"collection,omitempty"`
	ReplicaPlacement   *super_block.ReplicaPlacement `json:"replication,omitempty"`
	Ttl                *needle.TTL                   `json:"ttl,omitempty"`
	DiskType           types.DiskType                `json:"disk,omitempty"`
	Preallocate        int64                         `json:"preallocate,omitempty"`
	DataCenter         string                        `json:"dataCenter,omitempty"`
	Rack               string                        `json:"rack,omitempty"`
	DataNode           string                        `json:"dataNode,omitempty"`
	MemoryMapMaxSizeMb uint32                        `json:"memoryMapMaxSizeMb,omitempty"`
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

func (o *VolumeGrowOption) String() string {
	blob, _ := json.Marshal(o)
	return string(blob)
}

func (o *VolumeGrowOption) Threshold() float64 {
	v := util.GetViper()
	return v.GetFloat64("master.volume_growth.threshold")
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count int) {
	v := util.GetViper()
	switch copyCount {
	case 1:
		count = v.GetInt("master.volume_growth.copy_1")
	case 2:
		count = v.GetInt("master.volume_growth.copy_2")
	case 3:
		count = v.GetInt("master.volume_growth.copy_3")
	default:
		count = v.GetInt("master.volume_growth.copy_other")
	}
	return
}

//根据类型扩容volumes，具体就是根据当前的副本策略和拓扑结构，判断增加的volume应该所处的位置，并向相应的服务发送扩容volume的请求
func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, grpcDialOption grpc.DialOption, topo *Topology, targetCount int) (result []*master_pb.VolumeLocation, err error) {
	if targetCount == 0 {
		targetCount = vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount())
	}
	result, err = vg.GrowByCountAndType(grpcDialOption, targetCount, option, topo)
	if len(result) > 0 && len(result)%option.ReplicaPlacement.GetCopyCount() == 0 {
		return result, nil
	}
	return result, err
}

//按照volume增长个数和类型添加volume
func (vg *VolumeGrowth) GrowByCountAndType(grpcDialOption grpc.DialOption, targetCount int, option *VolumeGrowOption, topo *Topology) (result []*master_pb.VolumeLocation, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if res, e := vg.findAndGrow(grpcDialOption, topo, option); e == nil {
			result = append(result, res...)
		} else {
			glog.V(0).Infof("create %d volume, created %d: %v", targetCount, len(result), e)
			return result, e
		}
	}
	return
}

//给volume找到一个槽位，然后增加新的volume
func (vg *VolumeGrowth) findAndGrow(grpcDialOption grpc.DialOption, topo *Topology, option *VolumeGrowOption) (result []*master_pb.VolumeLocation, err error) {
	//找到所有符合要求的server信息
	servers, e := vg.findEmptySlotsForOneVolume(topo, option)
	if e != nil {
		return nil, e
	}
	vid, raftErr := topo.NextVolumeId()
	if raftErr != nil {
		return nil, raftErr
	}
	//通知指定的server增加volume，并更新内存结构
	if err = vg.grow(grpcDialOption, topo, vid, option, servers...); err == nil {
		for _, server := range servers {
			result = append(result, &master_pb.VolumeLocation{
				Url:       server.Url(),
				PublicUrl: server.PublicUrl,
				NewVids:   []uint32{uint32(vid)},
			})
		}
	}
	return
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
//为volume查找一个合适的槽位，返回多个合适的位置
func (vg *VolumeGrowth) findEmptySlotsForOneVolume(topo *Topology, option *VolumeGrowOption) (servers []*DataNode, err error) {
	//find main datacenter and other data centers
	rp := option.ReplicaPlacement
	mainDataCenter, otherDataCenters, dc_err := topo.PickNodesByWeight(rp.DiffDataCenterCount+1, option, func(node Node) error { //匿名函数：做一些容量与槽位的检查
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}
		if node.AvailableSpaceFor(option) < int64(rp.DiffRackCount+rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), rp.DiffRackCount+rp.SameRackCount+1)
		}
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if n.AvailableSpaceFor(option) >= 1 {
					possibleDataNodesCount++
				}
			}
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				possibleRacksCount++
			}
		}
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		return nil
	})
	if dc_err != nil {
		return nil, dc_err
	}

	//find main rack and other racks
	mainRack, otherRacks, rackErr := mainDataCenter.(*DataCenter).PickNodesByWeight(rp.DiffRackCount+1, option, func(node Node) error {
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}
		if node.AvailableSpaceFor(option) < int64(rp.SameRackCount+1) {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), rp.SameRackCount+1)
		}
		if len(node.Children()) < rp.SameRackCount+1 {
			// a bit faster way to test free racks
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if n.AvailableSpaceFor(option) >= 1 {
				possibleDataNodesCount++
			}
		}
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rackErr != nil {
		return nil, rackErr
	}

	//find main server and other servers
	mainServer, otherServers, serverErr := mainRack.(*Rack).PickNodesByWeight(rp.SameRackCount+1, option, func(node Node) error {
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}
		if node.AvailableSpaceFor(option) < 1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.AvailableSpaceFor(option), 1)
		}
		return nil
	})
	if serverErr != nil {
		return nil, serverErr
	}

	servers = append(servers, mainServer.(*DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Int63n(rack.AvailableSpaceFor(option))
		if server, e := rack.ReserveOneVolume(r, option); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	for _, datacenter := range otherDataCenters {
		r := rand.Int63n(datacenter.AvailableSpaceFor(option))
		if server, e := datacenter.ReserveOneVolume(r, option); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	return
}

//向所有该volume所在上级结构发送增加特定volume的请求
func (vg *VolumeGrowth) grow(grpcDialOption grpc.DialOption, topo *Topology, vid needle.VolumeId, option *VolumeGrowOption, servers ...*DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, grpcDialOption, vid, option); err == nil {
			vi := storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          needle.CurrentVersion,
				DiskType:         option.DiskType.String(),
			}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server.NodeImpl.String())
		} else {
			glog.V(0).Infoln("Failed to assign volume", vid, "to", servers, "error", err)
			return fmt.Errorf("Failed to assign %d: %v", vid, err)
		}
	}
	return nil
}
