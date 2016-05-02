package gedis

import (
	"errors"
	"strings"
	"sync"
)

// 多个哨兵监控一个主从组合

const (
	SENTINEL_GET_MASTER_ADDR_BY_NAME = "get-master-addr-by-name"
	SENTINEL_SUBSCRIBE_CHANNEL = "+switch-master"
)

type SentinelGedisPool struct {
	size              int
	// 存放主节点的连接对象
	pool              chan *Gedis
	// 主节点
	currentHostMaster HostAndPort
	// 哨兵监听
	sentinelListeners []*SentinelListener

	builder           PoolBuilder

	mutex             sync.Mutex
}

func NewSentinelPool(masterName string, sentinels []HostAndPort, size int) (*SentinelGedisPool, error) {
	master := getMasterBySentinels(masterName, sentinels)
	if master == nil {
		return nil, errors.New("Can connect to sentinel, but " + masterName + " seems to be not monitored...")
	}

	sgp := &SentinelGedisPool{
		size : size,
		currentHostMaster : master,
		builder: NewGedis(master.GetHost(), master.GetPort()),
		pool:make(chan *Gedis, size),
		sentinelListeners: make([] *SentinelListener, 0, len(sentinels)),
	}
	sentinelErr := sgp.initSentinels(masterName, sentinels)
	if sentinelErr != nil {
		return nil, sentinelErr
	}
	err := sgp.initPool(master)
	if err != nil {
		return nil, err
	}
	return sgp, nil
}

func (sgp *SentinelGedisPool)Get() (*Gedis, error) {
	select {
	case g := <-sgp.pool:
		g.Pool = sgp
		return g, nil
	default:
		return nil, errors.New("cannt get gedis in pool")
	}
}

func (sgp *SentinelGedisPool)Put(g *Gedis) {
	select {
	case sgp.pool <- g:
	default:
		g.Close()
	}
}

func (sgp *SentinelGedisPool)Close() {
	poolSize := len(sgp.pool)
	for i := 0; i < poolSize; i++ {
		gedis := <-sgp.pool
		gedis.Close()
	}

	// 停止监听
	for _, listener := range sgp.sentinelListeners {
		listener.stop()
	}
}

// 根据sentinel获取master
func getMasterBySentinels(masterName string, sentinels []HostAndPort) HostAndPort {
	var master HostAndPort
	for _, sentinel := range sentinels {
		gedis, _ := NewGedis(sentinel.GetHost(), sentinel.GetPort())
		if gedis != nil {
			reply, sErr := gedis.Sentinel(SENTINEL_GET_MASTER_ADDR_BY_NAME, masterName)
			if reply != nil && len(reply) == 2 && sErr == nil {
				master = HostAndPort{
					host:reply[0],
					port: int(reply[1]),
				}
				gedis.Close()
				break
			}
			gedis.Close()
		}
	}
	return master
}

func (sgp *SentinelGedisPool)initSentinels(masterName string, sentinels []HostAndPort) error {
	for i, sentinel := range sentinels {
		// 创建到Sentinel的连接对象
		g, err := sgp.builder(sentinel.GetHost(), sentinel.GetPort())// NewGedis(sentinel.GetHost(), sentinel.GetPort())
		if err != nil {
			return errors.New("cannt connect to sentinel(" + sentinel.Str() + "):" + err)
		}
		// 创建一个订阅客户端
		subClient := NewPubSubClient(g)
		// 订阅 master切换channel
		r := subClient.Subscribe(SENTINEL_SUBSCRIBE_CHANNEL)
		if r.Err != nil {
			return errors.New("subsribe switch master error:" + err)
		}
		listener := &SentinelListener{
			pool: sgp,
			masterName: masterName,
			subClient: subClient,
			closeChannel:        make(chan struct{}),
			switchMasterChannel: make(chan *switchMaster),
		}
		listener.start()
		sgp.sentinelListeners[i] = listener
	}
	return nil
}

func (sgp *SentinelGedisPool) initPool(master HostAndPort) error {
	gs := make([]*Gedis, 0, sgp.size)
	for i := 0; i < sgp.size; i++ {
		gedis, err := sgp.builder(master.GetHost(), master.GetPort())
		if err != nil {
			for _, g := range gs {
				g.Close()
			}
			return errors.New("cannt connect to master(" + master.Str() + "),error:" + err)
		}
		if gedis != nil {
			gs = append(gs, gedis)
		}
	}
	sgp.pool = make(chan *Gedis, len(gs))
	for i := range gs {
		sgp.pool <- gs[i]
	}
	return nil
}

// TODO listener
type SentinelListener struct {
	pool                *SentinelGedisPool
	masterName          string
	running             bool
	subClient           *PubSubClient

	closeChannel        chan struct{}
	switchMasterChannel chan *switchMaster
}

func (l *SentinelListener)start() {
	l.running = true
	go func() {
		for l.running {
			// 获取主从切换消息
			r := l.subClient.Receive()
			if r.Timeout() {
				continue
			}
			if r.Err != nil {
				// TODO logging
				return
			}
			sMsg := strings.Split(r.Message, " ")
			name := sMsg[0]
			if name == l.masterName {
				newAddr := HostAndPort{sMsg[3], sMsg[4]}
				select {
				case l.switchMasterChannel <- &switchMaster{name, newAddr}:
				case <-l.closeChannel:
					return
				}
			}

		}
	}()

	go func() {
		for l.running {
			select {
			case sm := <-l.switchMasterChannel:
				l.pool.mutex.Lock()
				l.pool.initPool(sm.addr, l.pool.size)
				l.pool.mutex.Unlock()
			case <-l.closeChannel:
				l.pool.Close()
				return
			}
		}
	}()
}

func (l *SentinelListener)stop() {
	l.running = false
	l.subClient.Unsubscribe(SENTINEL_SUBSCRIBE_CHANNEL)
	l.subClient.Close()
}

type switchMaster struct {
	name string
	addr HostAndPort
}
