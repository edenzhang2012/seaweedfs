package needle_map

import (
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/google/btree"
)

type NeedleValue struct {
	Key    types.NeedleId
	Offset types.Offset `comment:"Volume offset"`            //since aligned to 8 bytes, range is 4G*8=32G needle数据在dat文件中的位置
	Size   types.Size   `comment:"Size of the data portion"` //needle size -1表示墓碑，其他负数表示文件已经被删除但空间没有被回收
}

func (nv NeedleValue) Less(than btree.Item) bool {
	that := than.(NeedleValue)
	return nv.Key < that.Key
}

func (nv NeedleValue) ToBytes() []byte {
	return ToBytes(nv.Key, nv.Offset, nv.Size)
}

func ToBytes(key types.NeedleId, offset types.Offset, size types.Size) []byte {
	bytes := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(bytes[0:types.NeedleIdSize], key)
	types.OffsetToBytes(bytes[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	util.Uint32toBytes(bytes[types.NeedleIdSize+types.OffsetSize:types.NeedleIdSize+types.OffsetSize+types.SizeSize], uint32(size))
	return bytes
}
