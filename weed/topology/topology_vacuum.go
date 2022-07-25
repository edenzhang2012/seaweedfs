package topology

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

//检查volume已删除大小占总大小的比例，返回超过阈值的VolumeLocationList
func (t *Topology) batchVacuumVolumeCheck(grpcDialOption grpc.DialOption, vid needle.VolumeId,
	locationlist *VolumeLocationList, garbageThreshold float64) (*VolumeLocationList, bool) {
	ch := make(chan int, locationlist.Length())
	errCount := int32(0)
	//检查所有关联volume，副本与EC机制
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			err := operation.WithVolumeServerClient(false, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				//调用volume server的RPC接口，获取已删除文件总大小占volume总大小的百分比
				resp, err := volumeServerClient.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
					VolumeId: uint32(vid),
				})
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					ch <- -1
					return err
				}
				//如果达到vacuum的阈值，发送信号
				if resp.GarbageRatio >= garbageThreshold {
					ch <- index
				} else {
					ch <- -1
				}
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Checking vacuuming %d on %s: %v", vid, url, err)
			}
		}(index, dn.ServerAddress(), vid)
	}
	vacuumLocationList := NewVolumeLocationList()

	waitTimeout := time.NewTimer(time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case index := <-ch:
			if index != -1 {
				vacuumLocationList.list = append(vacuumLocationList.list, locationlist.list[index])
			}
		case <-waitTimeout.C:
			return vacuumLocationList, false
		}
	}
	return vacuumLocationList, errCount == 0 && len(vacuumLocationList.list) > 0
}

//批量压缩相关联的所有volume，平均每个volume 3min时间，超过volumes *3min则认为失败
//压缩操作的实质是建立新的idx文件和dat文件，将旧的dat文件和idx文件迁移过去，迁移的过程中会剔除掉已经被删除或者过期的块
func (t *Topology) batchVacuumVolumeCompact(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, preallocate int64) bool {
	vl.accessLock.Lock()
	//TODO 需要vacuum的volume先置为不可写，该不可写的意思是新的数据不会被写到这个volume上，但旧的数据可以修改（猜测，需要后面确认）
	vl.removeFromWritable(vid)
	vl.accessLock.Unlock()

	ch := make(chan bool, locationlist.Length())
	//针对多个副本，或者多个EC分片
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			err := operation.WithVolumeServerClient(true, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				/*
					向volume server发送compact请求，压缩volume：
					本质是建立新的volume文件，将旧的volume文件迁移到新的volume文件，迁移的过程中去掉已经被删除的块。此时新旧volume还同时存在
				*/
				stream, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumeId:    uint32(vid),
					Preallocate: preallocate,
				})
				if err != nil {
					return err
				}

				//进度展示
				for {
					resp, recvErr := stream.Recv()
					if recvErr != nil {
						if recvErr == io.EOF {
							break
						} else {
							return recvErr
						}
					}
					glog.V(0).Infof("%d vacuum %d on %s processed %d bytes", index, vid, url, resp.ProcessedBytes)
				}
				return nil
			})
			if err != nil {
				glog.Errorf("Error when vacuuming %d on %s: %v", vid, url, err)
				ch <- false
			} else {
				glog.V(0).Infof("Complete vacuuming %d on %s", vid, url)
				ch <- true
			}
		}(index, dn.ServerAddress(), vid)
	}
	isVacuumSuccess := true

	//平均每个volume 3min时间，超时则认为失败
	waitTimeout := time.NewTimer(3 * time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case canCommit := <-ch:
			isVacuumSuccess = isVacuumSuccess && canCommit
		case <-waitTimeout.C:
			return false
		}
	}
	return isVacuumSuccess
}

//接着compact步骤之后，最后检查是否旧的volume数据内有新的提交，若有，同步到新的volume数据内，同时删除旧的volume数据，同时将新的volume管理数据加载进内存
//最后将所有volume置为可写状态
func (t *Topology) batchVacuumVolumeCommit(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) bool {
	isCommitSuccess := true
	isReadOnly := false
	//遍历所有关联的volume（多副本或者EC）
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start Committing vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			//发送请求到对应的volume server，最后检查是否旧的volume数据内有新的提交，若有，同步到新的volume数据内，同时删除旧的volume数据，同时将新的volume管理数据加载进内存
			resp, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: uint32(vid),
			})
			if resp != nil && resp.IsReadOnly {
				isReadOnly = true
			}
			return err
		})
		if err != nil {
			glog.Errorf("Error when committing vacuum %d on %s: %v", vid, dn.Url(), err)
			isCommitSuccess = false
		} else {
			glog.V(0).Infof("Complete Committing vacuum %d on %s", vid, dn.Url())
		}
	}
	if isCommitSuccess {
		for _, dn := range locationlist.list {
			vl.SetVolumeAvailable(dn, vid, isReadOnly)
		}
	}
	return isCommitSuccess
}

//compact失败，清理临时文件，等待下一次compact
func (t *Topology) batchVacuumVolumeCleanup(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			//发送请求到volume server
			_, err := volumeServerClient.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: uint32(vid),
			})
			return err
		})
		if err != nil {
			glog.Errorf("Error when cleaning up vacuum %d on %s: %v", vid, dn.Url(), err)
		} else {
			glog.V(0).Infof("Complete cleaning up vacuum %d on %s", vid, dn.Url())
		}
	}
}

//对volume中已经删除的数据进行清理
func (t *Topology) Vacuum(grpcDialOption grpc.DialOption, garbageThreshold float64, volumeId uint32, collection string, preallocate int64) {

	// if there is vacuum going on, return immediately
	swapped := atomic.CompareAndSwapInt64(&t.vacuumLockCounter, 0, 1)
	if !swapped {
		return
	}
	defer atomic.StoreInt64(&t.vacuumLockCounter, 0)

	// now only one vacuum process going on

	glog.V(1).Infof("Start vacuum on demand with threshold: %f collection: %s volumeId: %d",
		garbageThreshold, collection, volumeId)
	for _, col := range t.collectionMap.Items() {
		//get collection
		c := col.(*Collection)
		if collection != "" && collection != c.Name {
			continue
		}
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				//get volume layout
				volumeLayout := vl.(*VolumeLayout)
				if volumeId > 0 {
					if volumeLayout.Lookup(needle.VolumeId(volumeId)) != nil {
						t.vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, preallocate)
					}
				} else {
					t.vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, preallocate)
				}
			}
		}
	}
}

func (t *Topology) vacuumOneVolumeLayout(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, preallocate int64) {

	volumeLayout.accessLock.RLock()
	tmpMap := make(map[needle.VolumeId]*VolumeLocationList)
	for vid, locationList := range volumeLayout.vid2location {
		tmpMap[vid] = locationList.Copy()
	}
	volumeLayout.accessLock.RUnlock()

	for vid, locationList := range tmpMap {

		volumeLayout.accessLock.RLock()
		isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
		volumeLayout.accessLock.RUnlock()

		if isReadOnly {
			continue
		}

		glog.V(2).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)
		if vacuumLocationList, needVacuum := t.batchVacuumVolumeCheck(grpcDialOption, vid, locationList, garbageThreshold); needVacuum {
			if t.batchVacuumVolumeCompact(grpcDialOption, volumeLayout, vid, vacuumLocationList, preallocate) {
				t.batchVacuumVolumeCommit(grpcDialOption, volumeLayout, vid, vacuumLocationList)
			} else {
				t.batchVacuumVolumeCleanup(grpcDialOption, volumeLayout, vid, vacuumLocationList)
			}
		}
	}
}
