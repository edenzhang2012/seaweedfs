package filer

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

//事件回放
func Replay(filerStore FilerStore, resp *filer_pb.SubscribeMetadataResponse) error {
	message := resp.EventNotification
	var oldPath util.FullPath
	var newEntry *Entry
	//delete old entry
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
		glog.V(4).Infof("deleting %v", oldPath)
		if err := filerStore.DeleteEntry(context.Background(), oldPath); err != nil {
			return err
		}
	}

	//create new entry
	if message.NewEntry != nil {
		dir := resp.Directory
		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		key := util.NewFullPath(dir, message.NewEntry.Name)
		glog.V(4).Infof("creating %v", key)
		newEntry = FromPbEntry(dir, message.NewEntry)
		if err := filerStore.InsertEntry(context.Background(), newEntry); err != nil {
			return err
		}
	}

	return nil
}
