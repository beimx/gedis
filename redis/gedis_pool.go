package gedis

type GedisPool struct {
	host    string

	port    int

	// 用一个chan来存放连接对象
	pool    chan *Gedis

	// 自定义连接创建方法，默认使用Dial(host,port)
	//
	builder PoolBuilder
}

type PoolBuilder func(host string, port int) (*Gedis, error)

func NewGedisPool(host string, port, size int) (*GedisPool, error) {
	return NewGedisPoolWithCustom(host, port, size, NewGedis(host, port))
}

func NewGedisPoolWithCustom(host string, port, size int, builder PoolBuilder) (*GedisPool, error) {
	gedises := make([]*Gedis, 0, size)
	for i := 0; i < size; i++ {
		gedis, err := builder(host, port)
		if err != nil {
			// 如果有一个连接创建失败，则关闭其它所有已经创建的连接，然后返回nil
			for _, g := range gedises {
				g.Close()
			}
			return nil, err
		}
		if gedis != nil {
			gedises = append(gedises, gedis)
		}
	}
	p := GedisPool{
		host: host,
		port : port,
		pool: make(chan *Gedis, len(gedises)),
		builder: builder,
	}
	for i := range gedises {
		p.pool <- gedises[i]
	}
	return &p, nil
}

// 从连接池中获取连接对象
func (p *GedisPool) Get() (*Gedis, error) {
	select {
	case g := <-p.pool:
	// 如果是从pool中获取的Gedis，则设置其pool属性
		g.Pool = p
		return g, nil
	default:
	// TODO 此处没有想清楚，所以直接创建一个连接返回
		return p.builder(p.host, p.port), nil
	}
}

func (p *GedisPool)Put(g *Gedis) {
	select {
	case p.pool <- g:
	default:
		g.Close()
	}
}

func (p *GedisPool)Empty() {
	var g *Gedis
	for {
		select {
		case g <- p.pool:
			g.Close()
		default:
			return
		}
	}
}

//type PoolConfig struct {
//	size    int
//
//	maxIdle int
//
//	minIdle int
//}
