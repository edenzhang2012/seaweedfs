//go:build linux
// +build linux

package backend

import (
	"os"
	"syscall"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

//创建volume文件，如果指定了prealloc会直接分配空间
func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if e != nil {
		return nil, e
	}
	if preallocate != 0 {
		syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)
		glog.V(1).Infof("Preallocated %d bytes disk space for %s", preallocate, fileName)
	}
	return NewDiskFile(file), nil
}
