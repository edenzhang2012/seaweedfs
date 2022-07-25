package cluster

import (
	"math"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

const (
	MasterType       = "master"
	VolumeServerType = "volumeServer"
	FilerType        = "filer"
	BrokerType       = "broker"
)

type FilerGroup string
type Filers struct {
	filers  map[pb.ServerAddress]*ClusterNode
	leaders *Leaders
}
type Leaders struct {
	leaders [3]pb.ServerAddress
}

type ClusterNode struct {
	Address   pb.ServerAddress
	Version   string
	counter   int
	CreatedTs time.Time
}

type Cluster struct {
	filerGroup2filers map[FilerGroup]*Filers
	filersLock        sync.RWMutex
	brokers           map[pb.ServerAddress]*ClusterNode
	brokersLock       sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		filerGroup2filers: make(map[FilerGroup]*Filers),
		brokers:           make(map[pb.ServerAddress]*ClusterNode),
	}
}

func (cluster *Cluster) getFilers(filerGroup FilerGroup, createIfNotFound bool) *Filers {
	filers, found := cluster.filerGroup2filers[filerGroup]
	if !found && createIfNotFound {
		filers = &Filers{
			filers:  make(map[pb.ServerAddress]*ClusterNode),
			leaders: &Leaders{},
		}
		cluster.filerGroup2filers[filerGroup] = filers
	}
	return filers
}

//cluster node添加事件
func (cluster *Cluster) AddClusterNode(ns, nodeType string, address pb.ServerAddress, version string) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroup(ns)
	switch nodeType {
	//filer
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		filers := cluster.getFilers(filerGroup, true)
		//if exist
		if existingNode, found := filers.filers[address]; found {
			existingNode.counter++
			return nil
		}
		//添加当前地址到filers中
		filers.filers[address] = &ClusterNode{
			Address:   address,
			Version:   version,
			counter:   1,
			CreatedTs: time.Now(),
		}
		return cluster.ensureFilerLeaders(filers, true, filerGroup, nodeType, address)
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()
		if existingNode, found := cluster.brokers[address]; found {
			existingNode.counter++
			return nil
		}
		cluster.brokers[address] = &ClusterNode{
			Address:   address,
			Version:   version,
			counter:   1,
			CreatedTs: time.Now(),
		}
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    true,
				},
			},
		}
	}
	return nil
}

//移除clusternode
func (cluster *Cluster) RemoveClusterNode(ns string, nodeType string, address pb.ServerAddress) []*master_pb.KeepConnectedResponse {
	filerGroup := FilerGroup(ns)
	switch nodeType {
	case FilerType:
		cluster.filersLock.Lock()
		defer cluster.filersLock.Unlock()
		filers := cluster.getFilers(filerGroup, false)
		if filers == nil {
			return nil
		}
		if existingNode, found := filers.filers[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(filers.filers, address)
				return cluster.ensureFilerLeaders(filers, false, filerGroup, nodeType, address)
			}
		}
	case BrokerType:
		cluster.brokersLock.Lock()
		defer cluster.brokersLock.Unlock()
		if existingNode, found := cluster.brokers[address]; !found {
			return nil
		} else {
			existingNode.counter--
			if existingNode.counter <= 0 {
				delete(cluster.brokers, address)
				return []*master_pb.KeepConnectedResponse{
					{
						ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
							NodeType: nodeType,
							Address:  string(address),
							IsAdd:    false,
						},
					},
				}
			}
		}
	case MasterType:
		return []*master_pb.KeepConnectedResponse{
			{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					NodeType: nodeType,
					Address:  string(address),
					IsAdd:    false,
				},
			},
		}
	}
	return nil
}

func (cluster *Cluster) ListClusterNode(filerGroup FilerGroup, nodeType string) (nodes []*ClusterNode) {
	switch nodeType {
	case FilerType:
		cluster.filersLock.RLock()
		defer cluster.filersLock.RUnlock()
		filers := cluster.getFilers(filerGroup, false)
		if filers == nil {
			return
		}
		for _, node := range filers.filers {
			nodes = append(nodes, node)
		}
	case BrokerType:
		cluster.brokersLock.RLock()
		defer cluster.brokersLock.RUnlock()
		for _, node := range cluster.brokers {
			nodes = append(nodes, node)
		}
	case MasterType:
	}
	return
}

func (cluster *Cluster) IsOneLeader(filerGroup FilerGroup, address pb.ServerAddress) bool {
	filers := cluster.getFilers(filerGroup, false)
	if filers == nil {
		return false
	}
	return filers.leaders.isOneLeader(address)
}

//确认filer leader操作，据事件类型组合response
func (cluster *Cluster) ensureFilerLeaders(filers *Filers, isAdd bool, filerGroup FilerGroup, nodeType string, address pb.ServerAddress) (result []*master_pb.KeepConnectedResponse) {
	if isAdd {
		if filers.leaders.addLeaderIfVacant(address) { //add address to leaders
			// has added the address as one leader
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      true,
				},
			})
		} else {
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      true,
				},
			})
		}
	} else {
		if filers.leaders.removeLeaderIfExists(address) {

			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   true,
					IsAdd:      false,
				},
			})

			// pick the freshest one, since it is less likely to go away
			var shortestDuration int64 = math.MaxInt64
			now := time.Now()
			var candidateAddress pb.ServerAddress
			//找到时间间隔最短的节点最为新的leader
			for _, node := range filers.filers {
				if filers.leaders.isOneLeader(node.Address) {
					continue
				}
				duration := now.Sub(node.CreatedTs).Nanoseconds()
				if duration < shortestDuration {
					shortestDuration = duration
					candidateAddress = node.Address
				}
			}
			if candidateAddress != "" {
				filers.leaders.addLeaderIfVacant(candidateAddress)
				// added a new leader
				result = append(result, &master_pb.KeepConnectedResponse{
					ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
						NodeType: nodeType,
						Address:  string(candidateAddress),
						IsLeader: true,
						IsAdd:    true,
					},
				})
			}
		} else { //非leader
			result = append(result, &master_pb.KeepConnectedResponse{
				ClusterNodeUpdate: &master_pb.ClusterNodeUpdate{
					FilerGroup: string(filerGroup),
					NodeType:   nodeType,
					Address:    string(address),
					IsLeader:   false,
					IsAdd:      false,
				},
			})
		}
	}
	return
}

//存在空位时将address加入leader
func (leaders *Leaders) addLeaderIfVacant(address pb.ServerAddress) (hasChanged bool) {
	if leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == "" {
			leaders.leaders[i] = address
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) removeLeaderIfExists(address pb.ServerAddress) (hasChanged bool) {
	if !leaders.isOneLeader(address) {
		return
	}
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			leaders.leaders[i] = ""
			hasChanged = true
			return
		}
	}
	return
}
func (leaders *Leaders) isOneLeader(address pb.ServerAddress) bool {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] == address {
			return true
		}
	}
	return false
}
func (leaders *Leaders) GetLeaders() (addresses []pb.ServerAddress) {
	for i := 0; i < len(leaders.leaders); i++ {
		if leaders.leaders[i] != "" {
			addresses = append(addresses, leaders.leaders[i])
		}
	}
	return
}
