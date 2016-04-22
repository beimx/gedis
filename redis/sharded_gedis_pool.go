package gedis

type ShardedGedisPool struct {
	shards  []*ShardInfo

	pool    chan *ShardedGedis

	builder ShardedPoolBuilder
}

type ShardedPoolBuilder func(shards []*ShardInfo) (*ShardedGedis, error)

// 默认使用
func NewShardedGedisPool(shards []*ShardInfo, size int) (*ShardedGedisPool, error) {
	return NewShardedGedisPoolWithCustom(shards, size, NewShardedGedis(shards))
}

func NewShardedGedisPoolWithCustom(shards []*ShardInfo, size int, builder ShardedPoolBuilder) (*ShardedGedisPool, error) {
	gedises := make([]*ShardedGedis, 0, size)
	for i := 0; i < size; i++ {
		gedis, err := builder(shards)
		if err != nil {
			for _, g := range gedises {
				g.Close()
			}
		}
		if gedis != nil {
			gedises = append(gedises, gedis)
		}
	}
	p := ShardedGedisPool{
		shards: shards,
		pool: make(chan *ShardedGedis, len(gedises)),
		builder: builder,
	}
	for i := range gedises {
		p.pool <- gedises[i]
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