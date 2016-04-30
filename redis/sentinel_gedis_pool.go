package gedis

import "errors"

// 多个哨兵监控一个主从组合

const (
	Default_Timeout = 2000
	Default_Database = 0
	SENTINEL_GET_MASTER_ADDR_BY_NAME = "get-master-addr-by-name"
)

type SentinelGedisPool struct {
	pool              chan *Gedis
	// 主节点
	currentHostMaster HostAndPort
	// 哨兵地址列表
	sentinelListeners []SentinelListener

	builder           PoolBuilder
}

func NewSentinelPool(masterName string, sentinels []HostAndPort, size int) (*SentinelGedisPool, error) {

	p := &SentinelGedisPool{}
	master := p.initSentinels(masterName, sentinels)
	if master == nil {
		return nil, errors.New("Can connect to sentinel, but " + masterName + " seems to be not monitored...")
	}
	p.initPool(master, size)
	return p, nil
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

// 初始化哨兵，并返回一个主节点地址
func (sgp *SentinelGedisPool)initSentinels(masterName string, sentinels []HostAndPort) (HostAndPort) {
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
	for _, sentinel := range sentinels {
		// TODO 监听Sentinel
	}
	return master
}

// 设置currentHostMaster,builder
func (sgp *SentinelGedisPool)initPool(master HostAndPort, size int) {
	if !sgp.currentHostMaster.equal(master) {
		sgp.currentHostMaster = master
		if sgp.builder == nil {
			sgp.builder = NewGedis(master.GetHost(), master.GetPort())
		}
		gs := make([]*Gedis, 0, size)
		g, err := NewGedis(master.GetHost(), master.GetPort())
		if err != nil {

		}
		if g != nil {
			gs = append(gs, g)
		}
		for i := range gs {
			sgp.pool <- gs[i]
		}
	}
}

// TODO listener
type SentinelListener  func()
