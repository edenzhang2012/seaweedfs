package storage

import (
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

// volume管理
type Volume struct {
	Id                 needle.VolumeId            // volume id
	dir                string                     // 数据文件夹
	dirIdx             string                     //idx文件目录，idx可以放到单独的目录中
	Collection         string                     // 集合
	DataBackend        backend.BackendStorageFile // 数据文件
	nm                 NeedleMapper               // 索引文件
	needleMapKind      NeedleMapKind              // 索引文件类型
	noWriteOrDelete    bool                       // if readonly, either noWriteOrDelete or noWriteCanDelete
	noWriteCanDelete   bool                       // if readonly, either noWriteOrDelete or noWriteCanDelete
	noWriteLock        sync.RWMutex
	hasRemoteFile      bool   // if the volume has a remote file
	MemoryMapMaxSizeMb uint32 // 内存映射最大值

	super_block.SuperBlock // 超级块

	dataFileAccessLock    sync.RWMutex              //dat文件访问锁
	asyncRequestsChan     chan *needle.AsyncRequest //异步请求通道
	lastModifiedTsSeconds uint64                    // unix time in seconds
	lastAppendAtNs        uint64                    // unix time in nanoseconds

	lastCompactIndexOffset uint64 //	最后一次compact的索引偏移量
	lastCompactRevision    uint16 //	最后一次compact的版本号

	isCompacting       bool // 是否正在compact
	isCommitCompacting bool // 是否正在commit compact

	volumeInfo *volume_server_pb.VolumeInfo // volume信息
	location   *DiskLocation                //磁盘位置

	lastIoError error //最后一次io错误
}

func NewVolume(dirname string, dirIdx string, collection string, id needle.VolumeId, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, memoryMapMaxSizeMb uint32) (v *Volume, e error) {
	// if replicaPlacement is nil, the superblock will be loaded from disk
	v = &Volume{dir: dirname, dirIdx: dirIdx, Collection: collection, Id: id, MemoryMapMaxSizeMb: memoryMapMaxSizeMb,
		asyncRequestsChan: make(chan *needle.AsyncRequest, 128)}
	v.SuperBlock = super_block.SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	v.needleMapKind = needleMapKind
	//从文件夹中的各种文件中加载volume信息到内存
	e = v.load(true, true, needleMapKind, preallocate)
	v.startWorker()
	return
}

func (v *Volume) String() string {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return fmt.Sprintf("Id:%v dir:%s dirIdx:%s Collection:%s dataFile:%v nm:%v noWrite:%v canDelete:%v", v.Id, v.dir, v.dirIdx, v.Collection, v.DataBackend, v.nm, v.noWriteOrDelete || v.noWriteCanDelete, v.noWriteCanDelete)
}

func VolumeFileName(dir string, collection string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func (v *Volume) DataFileName() (fileName string) {
	return VolumeFileName(v.dir, v.Collection, int(v.Id))
}

func (v *Volume) IndexFileName() (fileName string) {
	return VolumeFileName(v.dirIdx, v.Collection, int(v.Id))
}

// 根据后缀获取完整的文件名
func (v *Volume) FileName(ext string) (fileName string) {
	switch ext {
	case ".idx", ".cpx", ".ldb":
		return VolumeFileName(v.dirIdx, v.Collection, int(v.Id)) + ext
	}
	// .dat, .cpd, .vif
	return VolumeFileName(v.dir, v.Collection, int(v.Id)) + ext
}

func (v *Volume) Version() needle.Version {
	if v.volumeInfo.Version != 0 {
		v.SuperBlock.Version = needle.Version(v.volumeInfo.Version)
	}
	return v.SuperBlock.Version
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	if v.DataBackend == nil {
		return
	}

	datFileSize, modTime, e := v.DataBackend.GetStat()
	if e == nil {
		return uint64(datFileSize), v.nm.IndexFileSize(), modTime
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.DataBackend.Name(), e)
	return // -1 causes integer overflow and the volume to become unwritable.
}

func (v *Volume) ContentSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.ContentSize()
}

func (v *Volume) DeletedSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.DeletedSize()
}

func (v *Volume) FileCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.FileCount())
}

func (v *Volume) DeletedCount() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return uint64(v.nm.DeletedCount())
}

func (v *Volume) MaxFileKey() types.NeedleId {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.MaxFileKey()
}

func (v *Volume) IndexFileSize() uint64 {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0
	}
	return v.nm.IndexFileSize()
}

func (v *Volume) DiskType() types.DiskType {
	return v.location.DiskType
}

func (v *Volume) SetStopping() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume SetStopping fail to sync volume idx %d", v.Id)
		}
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Sync(); err != nil {
			glog.Warningf("Volume SetStopping fail to sync volume %d", v.Id)
		}
	}
}

func (v *Volume) SyncToDisk() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
	}
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	for v.isCommitCompacting {
		time.Sleep(521 * time.Millisecond)
		glog.Warningf("Volume Close wait for compaction %d", v.Id)
	}

	if v.nm != nil {
		if err := v.nm.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume idx %d", v.Id)
		}
		v.nm.Close()
		v.nm = nil
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Sync(); err != nil {
			glog.Warningf("Volume Close fail to sync volume %d", v.Id)
		}
		_ = v.DataBackend.Close()
		v.DataBackend = nil
		stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Dec()
	}
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
func (v *Volume) expired(contentSize uint64, volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		// skip if we don't know size limit
		return false
	}
	if contentSize <= super_block.SuperBlockSize {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(2).Infof("volume %d now:%v lastModified:%v", v.Id, time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(2).Infof("volume %d ttl:%v lived:%v", v.Id, v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) expiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTsSeconds < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

func (v *Volume) collectStatus() (maxFileKey types.NeedleId, datFileSize int64, modTime time.Time, fileCount, deletedCount, deletedSize uint64, ok bool) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	glog.V(3).Infof("collectStatus volume %d", v.Id)

	if v.nm == nil || v.DataBackend == nil {
		return
	}

	ok = true

	maxFileKey = v.nm.MaxFileKey()
	datFileSize, modTime, _ = v.DataBackend.GetStat()
	fileCount = uint64(v.nm.FileCount())
	deletedCount = uint64(v.nm.DeletedCount())
	deletedSize = v.nm.DeletedSize()
	fileCount = uint64(v.nm.FileCount())

	return
}

func (v *Volume) ToVolumeInformationMessage() (types.NeedleId, *master_pb.VolumeInformationMessage) {

	maxFileKey, volumeSize, modTime, fileCount, deletedCount, deletedSize, ok := v.collectStatus()

	if !ok {
		return 0, nil
	}

	volumeInfo := &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             uint64(volumeSize),
		Collection:       v.Collection,
		FileCount:        fileCount,
		DeleteCount:      deletedCount,
		DeletedByteCount: deletedSize,
		ReadOnly:         v.IsReadOnly(),
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
		DiskType:         string(v.location.DiskType),
	}

	volumeInfo.RemoteStorageName, volumeInfo.RemoteStorageKey = v.RemoteStorageNameKey()

	return maxFileKey, volumeInfo
}

func (v *Volume) RemoteStorageNameKey() (storageName, storageKey string) {
	if v.volumeInfo == nil {
		return
	}
	if len(v.volumeInfo.GetFiles()) == 0 {
		return
	}
	return v.volumeInfo.GetFiles()[0].BackendName(), v.volumeInfo.GetFiles()[0].GetKey()
}

func (v *Volume) IsReadOnly() bool {
	v.noWriteLock.RLock()
	defer v.noWriteLock.RUnlock()
	return v.noWriteOrDelete || v.noWriteCanDelete || v.location.isDiskSpaceLow
}
