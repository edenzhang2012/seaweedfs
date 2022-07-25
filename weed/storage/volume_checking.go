package storage

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage/super_block"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func CheckAndFixVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, err error) {
	var indexSize int64
	//验证完整性
	if indexSize, err = verifyIndexFileIntegrity(indexFile); err != nil {
		return 0, fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), err)
	}
	if indexSize == 0 {
		return 0, nil
	}
	healthyIndexSize := indexSize
	for i := 1; i <= 10 && indexSize >= int64(i)*NeedleMapEntrySize; i++ {
		// check and fix last 10 entries, from the end to the start
		lastAppendAtNs, err = doCheckAndFixVolumeData(v, indexFile, indexSize-int64(i)*NeedleMapEntrySize)
		if err == io.EOF {
			healthyIndexSize = indexSize - int64(i)*NeedleMapEntrySize
			continue
		}
		if err != ErrorSizeMismatch {
			break
		}
	}
	if healthyIndexSize < indexSize {
		glog.Warningf("CheckAndFixVolumeDataIntegrity truncate idx file %s from %d to %d", indexFile.Name(), indexSize, healthyIndexSize)
		err = indexFile.Truncate(healthyIndexSize)
		if err != nil {
			glog.Warningf("CheckAndFixVolumeDataIntegrity truncate idx file %s from %d to %d: %v", indexFile.Name(), indexSize, healthyIndexSize, err)
		}
	}
	return
}

func doCheckAndFixVolumeData(v *Volume, indexFile *os.File, indexOffset int64) (lastAppendAtNs uint64, err error) {
	var lastIdxEntry []byte
	if lastIdxEntry, err = readIndexEntryAtOffset(indexFile, indexOffset); err != nil {
		return 0, fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), err)
	}
	key, offset, size := idx.IdxFileEntry(lastIdxEntry)
	if offset.IsZero() {
		return 0, nil
	}
	if size < 0 { //size < 0表示当前entry已被删除
		// read the deletion entry
		if lastAppendAtNs, err = verifyDeletedNeedleIntegrity(v.DataBackend, v.Version(), key); err != nil {
			return lastAppendAtNs, fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), err)
		}
	} else { //表示entry存在文件
		if lastAppendAtNs, err = verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset(), key, size); err != nil {
			return lastAppendAtNs, err
		}
	}
	return lastAppendAtNs, nil
}

// 验证idx文件完整性，实际idx文件中存放的全部都是NeedleMapEntry，所以其大小一定是NeedleMapEntrySize的整数倍，若非整数倍，说明文件不完整
func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%NeedleMapEntrySize != 0 {
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 {
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	bytes = make([]byte, NeedleMapEntrySize)
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

/*
校验needle完整性
NOTE:所谓校验，其实就是检测数据完整性，查看写下去的数据与预期的元数据是否一致
*/
func verifyNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key NeedleId, size Size) (lastAppendAtNs uint64, err error) {
	n, _, _, err := needle.ReadNeedleHeader(datFile, v, offset)
	if err == io.EOF {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("read %s at %d", datFile.Name(), offset)
	}
	if n.Size != size {
		return 0, ErrorSizeMismatch
	}
	if v == needle.Version3 {
		bytes := make([]byte, TimestampSize)
		_, err = datFile.ReadAt(bytes, offset+NeedleHeaderSize+int64(size)+needle.NeedleChecksumSize)
		if err == io.EOF {
			return 0, err
		}
		if err != nil {
			return 0, fmt.Errorf("verifyNeedleIntegrity check %s entry offset %d size %d: %v", datFile.Name(), offset, size, err)
		}
		n.AppendAtNs = util.BytesToUint64(bytes)
		fileTailOffset := offset + needle.GetActualSize(size, v)
		fileSize, _, err := datFile.GetStat()
		if err != nil {
			return 0, fmt.Errorf("stat file %s: %v", datFile.Name(), err)
		}
		if fileSize == fileTailOffset {
			return n.AppendAtNs, nil
		}
		if fileSize > fileTailOffset {
			glog.Warningf("Truncate %s from %d bytes to %d bytes!", datFile.Name(), fileSize, fileTailOffset)
			err = datFile.Truncate(fileTailOffset)
			if err == nil {
				return n.AppendAtNs, nil
			}
			return n.AppendAtNs, fmt.Errorf("truncate file %s: %v", datFile.Name(), err)
		}
		glog.Warningf("data file %s has %d bytes, less than expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
	}
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+int64(size), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}

/*
校验删除操作的needle的完整性
NOTE: 删除时会在idx文件和dat文件中都插入一个删除标记，dat文件中的删除标记是一个空的needle
*/
func verifyDeletedNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, key NeedleId) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	size := n.DiskSize(v)
	var fileSize int64
	fileSize, _, err = datFile.GetStat()
	if err != nil {
		return 0, fmt.Errorf("GetStat: %v", err)
	}
	if err = n.ReadData(datFile, fileSize-size, Size(0), v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", fileSize-size, size, err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}

func (v *Volume) checkIdxFile() error {
	datFileSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		return fmt.Errorf("get stat %s: %v", v.FileName(".dat"), err)
	}
	if datFileSize <= super_block.SuperBlockSize {
		return nil
	}
	indexFileName := v.FileName(".idx")
	if util.FileExists(indexFileName) {
		return nil
	}
	return fmt.Errorf("idx file %s does not exists", indexFileName)
}
