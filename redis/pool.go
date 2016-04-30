package gedis

type Pool interface {
	Get() (*Gedis, error)

	Put(g *Gedis)
}
