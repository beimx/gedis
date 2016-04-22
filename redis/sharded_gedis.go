package gedis

// 一个ShardedJedis实例含有多个分片主体(ShardInfo)
type ShardedGedis struct {
	nodes     map[int32]*ShardInfo

	resources map[*ShardInfo]*Gedis

	Pool      *ShardedGedisPool
}

func NewShardedGedis(shards []*ShardInfo) {

}

func (sg *ShardedGedis)Close() {
	if sg.Pool != nil {
		sg.Pool.Put(sg) // 还回连接对象
	}else {
		for _, g := range sg.resources {
			g.Close()// 关闭连接
		}
	}
}

// TODO 如果此处可以优雅的实现一个返回 Gedis实例的方法，
// 那么就不需要在ShardedGedis中再封装Set,Get,Del等操作了
// 调用方直接使用如 shardedGedis.GetGedis(key).Set(...)的方式
// 这样可以少了很多封装的代码，但是无法保证GetGedis(key)中的参数key和具体操作，如Get(key)中的参数key一致
func (s *ShardedGedis)GetGedis(key string) (*Gedis) {
	return s.getShard(key)
}

func (s *ShardedGedis)Get(key string) *Reply {
	gedis := s.getShard(key)
	return gedis.Get(key)
}

func (s *ShardedGedis)Set(key string, value interface{}) (string, error) {
	gedis := s.getShard(key)
	return gedis.Set(key, value)
}

func (s *ShardedGedis)Del(key string) (int, error) {
	gedis := s.getShard(key)
	return gedis.Del(key)
}

// TODO 其它API待补充


// 获取分片实例
func (s *ShardedGedis)getShard(key string) *Gedis {
	// TODO
	g := Gedis{

	}
	return &g
}

// 对Redis分片的封装，即一个ShardInfo实例代表一个分片主体

const (
	Default_Weight int = 1
)

type ShardInfo struct {
	host   string

	port   int

	name   string

	weight int
}

func NewShardInfo(host string, port int) (*ShardInfo) {
	return NewShardInfoWithNameAndWeight("", host, port, Default_Weight)
}

func NewShardInfoWithName(name, host string, port int) (*ShardInfo) {
	return NewShardInfoWithNameAndWeight(name, host, port, Default_Weight)
}

func NewShardInfoWithWeight(host string, port, weight int) {
	return NewShardInfoWithNameAndWeight("", host, port, weight)
}

func NewShardInfoWithNameAndWeight(name, host string, port, weight int) (*ShardInfo) {
	info := ShardInfo{
		host: host,
		port: port,
		name: name,
		weight: weight,
	}
	return &info
}
