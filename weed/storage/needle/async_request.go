package needle

/*
needle 数据异步落盘
*/

type AsyncRequest struct {
	N              *Needle          //needle数据
	IsWriteRequest bool             //是否是写请求
	ActualSize     int64            //实际大小
	offset         uint64           //偏移量
	size           uint64           //大小
	doneChan       chan interface{} //完成通道
	isUnchanged    bool             //是否未改变
	err            error            //错误
}

func NewAsyncRequest(n *Needle, isWriteRequest bool) *AsyncRequest {
	return &AsyncRequest{
		offset:         0,
		size:           0,
		ActualSize:     0,
		doneChan:       make(chan interface{}),
		N:              n,
		isUnchanged:    false,
		IsWriteRequest: isWriteRequest,
		err:            nil,
	}
}

func (r *AsyncRequest) WaitComplete() (uint64, uint64, bool, error) {
	<-r.doneChan
	return r.offset, r.size, r.isUnchanged, r.err
}

func (r *AsyncRequest) Complete(offset uint64, size uint64, isUnchanged bool, err error) {
	r.offset = offset
	r.size = size
	r.isUnchanged = isUnchanged
	r.err = err
	close(r.doneChan)
}

func (r *AsyncRequest) UpdateResult(offset uint64, size uint64, isUnchanged bool, err error) {
	r.offset = offset
	r.size = size
	r.isUnchanged = isUnchanged
	r.err = err
}

func (r *AsyncRequest) Submit() {
	close(r.doneChan)
}

func (r *AsyncRequest) IsSucceed() bool {
	return r.err == nil
}
