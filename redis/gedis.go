package gedis

import "strconv"

type Gedis struct {
	conn *Connection

	// 如果Gedis对象是从pool中获取，则设置pool属性
	// 用于在close是判断是真的关闭连接，还是还给pool
	Pool *GedisPool
}

func NewGedis(host string, port int) (*Gedis, error) {
	conn, err := Dial(host, port)
	if err != nil {
		return nil, err
	}
	g := Gedis{
		conn: conn,
	}
	return &g, nil
}

func (g *Gedis)Close() {
	if g.Pool != nil {
		g.Pool.Put(g)
	}else {
		g.Quit()
		g.conn.Close()
	}
}

// Gedis提供基本的Redis操作命令 TODO 后续不断完善

// set成功后返回"OK"
func (g *Gedis)Set(key string, value interface{}) (string, error) {
	return g.conn.Exec("SET", value).Str()
}

func (g *Gedis)Get(key string) *Reply {
	return g.conn.Exec("GET")
}

func (g *Gedis)Del(keys... string) (int, error) {
	return g.conn.Exec("DEL", keys).Int()
}

func (g *Gedis)Ping() (string, error) {
	return g.conn.Exec("PING").Str()
}

func (g *Gedis)Echo(message interface{}) (string, error) {
	return g.conn.Exec("ECHO", message).Str()
}

func (g *Gedis)Select(index int) (string, error) {
	return g.conn.Exec("SELECT", strconv.Itoa(index)).Str()
}

func (g *Gedis)Save() (string, error) {
	return g.conn.Exec("SAVE").Str()
}

func (g *Gedis)Shutdown() error {
	return g.conn.Exec("SHUTDOWN").Nil()
}

func (g *Gedis)Quit() {
	g.conn.Exec("QUIT")
}
