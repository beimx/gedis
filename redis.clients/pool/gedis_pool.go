package pool

import (
	"redis.clients/gedis"
)

type GedisPool struct {
	network string

	address string

	pool    chan *gedis.Gedis

	df      DialFunc
}

type DialFunc func(host string, port int) (*gedis.Gedis, error)

type PoolConfig struct {
	size    int

	maxIdle int

	minIdle int
}
