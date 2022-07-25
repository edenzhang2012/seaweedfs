package util

import "sync"

//简单的队列
type UnboundedQueue struct {
	outbound     []string
	outboundLock sync.RWMutex
	inbound      []string
	inboundLock  sync.RWMutex
}

func NewUnboundedQueue() *UnboundedQueue {
	q := &UnboundedQueue{}
	return q
}

//向in队列中入队，每次添加一个或多个
func (q *UnboundedQueue) EnQueue(items ...string) {
	q.inboundLock.Lock()
	defer q.inboundLock.Unlock()

	q.inbound = append(q.inbound, items...)

}

/*
消费out队列中的元素，每次将in中现存的全部消费完
若out队列长度为0，将out队列与in队列互换；
若互换后长度依然为0，什么都不做；若长度不为0，执行fn函数，将整个out队列传递给fn，fn需要保证将out队列执行完再返回
*/
func (q *UnboundedQueue) Consume(fn func([]string)) {
	q.outboundLock.Lock()
	defer q.outboundLock.Unlock()

	if len(q.outbound) == 0 {
		q.inboundLock.Lock()
		inbountLen := len(q.inbound)
		if inbountLen > 0 {
			t := q.outbound
			q.outbound = q.inbound
			q.inbound = t
		}
		q.inboundLock.Unlock()
	}

	if len(q.outbound) > 0 {
		fn(q.outbound)
		q.outbound = q.outbound[:0]
	}

}
