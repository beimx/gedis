package gedis

import (
	"strconv"
	"hash/crc32"
	"sort"
	"sync"
	"errors"
)

const (
	Default_Weight int = 1
	Virtual_Node_Magic = 160
)

// 一个ShardedJedis实例含有多个分片主体(ShardInfo)
type ShardedGedis struct {
	Nodes     map[uint32]ShardInfo

	HashCodes SortNumber

	resources map[ShardInfo]*Gedis

	Pool      *ShardedGedisPool

	sync.RWMutex
}

func NewShardedGedis(shards []ShardInfo) (*ShardedGedis, error) {
	if shards != nil {
		nodes := make(map[uint32]ShardInfo)
		hashCodes := SortNumber{}
		resources := make(map[ShardInfo]*Gedis)
		for i, s := range shards {
			g, err := NewGedis(s.host, s.port)
			if err != nil {
				if s.name == nil || s.name == "" {
					for n := 0; n < Virtual_Node_Magic * s.Weight; n++ {
						hashCode := hash("SHARD-" + strconv.Itoa(i) + "-NODE-" + strconv.Itoa(n))
						nodes[hashCode] = s
						hashCodes = append(hashCodes, hashCode)
					}
				}else {
					for n := 0; n < 160 * s.Weight; n++ {
						hashCode := hash(s.name + "*" + strconv.Itoa(s.Weight)) + strconv.Itoa(n)
						nodes[hashCode] = s
						hashCodes = append(hashCodes, hashCode)
					}
				}
				resources[s] = g
			}
		}
		sort.Sort(hashCodes)
		sg := ShardedGedis{
			Nodes: nodes,
			HashCodes:hashCodes,
			resources:resources,
		}
		return &sg, nil
	}else {
		return nil, errors.New("shards cannt not be null nil to initiailize ShardedGedis")
	}
}

// 获取分片实例
func (sg *ShardedGedis)getShard(key string) *Gedis {
	sg.RLock()
	defer sg.RUnlock()
	hashCode := hash(key)
	// 从hash环中获取hash code的索引值
	i := sg.getPosition(hashCode)
	// 根据索引值获取排序号的hash code
	// 根据 hash code 获取ShardInfo
	// 根据 ShardInfo 获取真实的Gedis实例
	gedis := sg.resources[sg.Nodes[sg.HashCodes[i]]]

	return gedis
}

// 一致性hash的相关数据存放在一个hash环中
// 获取hash code在hash环中的位置
func (sg *ShardedGedis)getPosition(hashCode uint32) int {
	// 使用二分法获取hash code的索引值
	// 前提是ShardedGedis的属性HashCodes已经经过排序
	i := sort.Search(len(sg.HashCodes), func(i int) bool {
		return sg.HashCodes[i] >= hashCode
	})
	// 返回该索引值在hash环中的位置
	if i < len(sg.HashCodes) {
		// 索引值在hash环索引范围内
		if i == len(sg.HashCodes) - 1 {
			// 若等于hash环的最后一个索引值，则返回第一个值:0
			// 因为是一个环形，且是顺时针搜索
			return 0;
		}else {
			// 所是hash环索引范围内的其它值，则直接返回
			return i
		}
	}else {
		// 若超出hash环的索引范围,则返回最后一个索引值
		return len(sg.HashCodes) - 1
	}
}

func hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

type SortNumber []uint32

func (sk SortNumber) Len() int {
	return len(sk)
}

func (sk SortNumber) Less(i, j int) bool {
	return sk[i] < sk[j]
}

​func (sk SortNumber) Swap(i, j int) {
	sk[i], sk[j] = sk[j], sk[i]
}

// 对Redis分片的封装，即一个ShardInfo实例代表一个分片主体
type ShardInfo struct {
	host   string

	port   int

	name   string

	Weight int
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
		Weight: weight,
	}
	return &info
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
