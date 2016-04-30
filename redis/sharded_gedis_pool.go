package gedis

type ShardedGedisPool struct {
	shards  []ShardInfo

	pool    chan *ShardedGedis

	builder ShardedPoolBuilder
}

type ShardedPoolBuilder func(shards []ShardInfo) (*ShardedGedis, error)

// 默认使用
func NewShardedPool(shards []ShardInfo, size int) (*ShardedGedisPool, error) {
	return NewShardedPoolWithCustom(shards, size, NewShardedGedis(shards))
}

func NewShardedPoolWithCustom(shards []ShardInfo, size int, builder ShardedPoolBuilder) (*ShardedGedisPool, error) {
	gs := make([]*ShardedGedis, 0, size)
	for i := 0; i < size; i++ {
		g, err := builder(shards)
		if err != nil {
			for _, g := range gs {
				g.Close()
			}
			return nil, err
		}
		if g != nil {
			gs = append(gs, g)
		}
	}
	p := ShardedGedisPool{
		shards: shards,
		pool: make(chan *ShardedGedis, len(gs)),
		builder: builder,
	}
	for i := range gs {
		p.pool <- gs[i]
	}
	return &p, nil
}

func (p *ShardedGedisPool)Get() (*ShardedGedis, error) {
	select {
	case sg := <-p.pool:
		sg.Pool = p
		return sg, nil
	default:
		return p.builder(p.shards), nil
	}
}

func (sgp *ShardedGedisPool)Put(sg *ShardedGedis) {
	select {
	case sgp <- sg:
	default:
		sg.Close()
	}
}

func (sgp *ShardedGedisPool)Empty() {
	var g ShardedGedis
	for {
		select {
		case g <- sgp.pool:
			g.Close()
		default:
			return
		}
	}
}