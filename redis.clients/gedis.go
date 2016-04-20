package gedis

import (
	"net"
	"time"
	"bufio"
	"redis.clients/resp"
	"strings"
	"errors"
)

var LoadingError string = errors.New("server is busy to loading data")

const (
	bufSize int = 4096
	default_host = "localhost"
	default_port = 6379
)

// 一个Gedis实例表示一个到Redis Server的连接
type Gedis struct {
	Conn      net.Conn

	// 作为与Redis Server通信时的 读/写 超时时间
	timeout   time.Duration

	reader    *bufio.Reader

	pending   []*request

	writeBuf  []byte

	completed []*Reply
}

type request struct {
	cmd  string

	args []interface{}
}

func DefaultDial() (*Gedis, error) {
	return Dial(default_host, default_port)
}

func Dial(host string, port int) (*Gedis, error) {
	return DialWithTimeout(host, port, time.Duration(0))
}

func DialWithTimeout(host string, port int, timeout time.Duration) (*Gedis, error) {
	conn, err := net.DialTimeout("tcp", host + ":" + port, timeout)
	if err != nil {
		return nil, err
	}
	c := new(Gedis)
	c.Conn = conn
	c.timeout = timeout
	c.reader = bufio.NewReaderSize(conn, bufSize)
	c.writeBuf = make([]byte, 0, 1024)
	return c, err
}

// 关闭连接
func (g *Gedis) Close() error {
	return g.Conn.Close()
}

// 执行Redis命令
func (g *Gedis) Exec(cmd string, args...interface{}) *Reply {
	err := g.writeRequest(&request{cmd, args})
	if err != nil {
		return &Reply{Type:ErrorReply, Err:err}
	}
	return g.ReadReply()
}

func (g *Gedis)ReadReply() *Reply {
	g.setReadTimeout()
	return g.parse()
}

func (g *Gedis) Append(cmd string, args...interface{}) {
	g.pending = append(g.pending, &request{cmd, args})
}

func (g *Gedis) writeRequest(requests...*request) error {
	g.setWriteTimeout()
	for i := range requests {
		req := make([]interface{}, 0, len(requests[i].args) + 1)
		req = append(req, requests[i].cmd)
		req = append(req, requests[i].args...)
		buf := resp.AppendArbitraryAsFlattenedStrings()
		_, err := g.Conn.Write(buf)
		if err != nil {
			g.Close()
			return err
		}
	}
	return nil
}

func (g *Gedis) setReadTimeout() {
	if g.timeout != 0 {
		g.Conn.SetReadDeadline(time.Now().Add(g.timeout))
	}
}

func (g *Gedis) setWriteTimeout() {
	if g.timeout != 0 {
		g.Conn.SetWriteDeadline(time.Now().Add(g.timeout))
	}
}

func (g *Gedis)parse() *Reply {
	m, err := resp.ReadMessage(g.reader)
	if err != nil {
		if t, ok := err.(*net.OpError); !ok || t.Timeout() {
			g.Close()
		}
		return &Reply{Type:ErrorReply, Err:err}
	}
	r, err := messageToReply(m)
	if err != nil {
		return &Reply{Type:ErrorReply, Err:err}
	}
	return r
}

// 将Redis返回的消息转换成 Reply对象返回
func messageToReply(m *resp.Message) (*Reply, error) {
	reply := &Reply{}

	switch m.Type {
	case resp.Err:
		errMsg, err := m.Err()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(errMsg.Error(), "LOADING") {
			err = LoadingError
		} else {
			err = &Error{errMsg} // Reply Error
		}
		reply.Type = ErrorReply
		reply.Err = err
	case resp.SimpleStr:
		status, err := m.Bytes()
		if err != nil {
			return nil, err
		}
		reply.Type = StatusReply
		reply.buf = status
	case resp.Int:
		i, err := m.Int()
		if err != nil {
			return nil, err
		}
		reply.Type = IntegerReply
		reply.int = i
	case resp.BulkStr:
		bulk, err := m.Bytes()
		if err != nil {
			return nil, err
		}
		reply.Type = BulkReply
		reply.buf = bulk
	case resp.Nil:
		reply.Type = NilReply
	case resp.Array:
		ms, err := m.Array()
		if err != nil {
			return nil, err
		}
		reply.Type = MultiReply
		reply.Sub = make([]*Reply, len(ms))
		for i := range ms {
			reply.Sub[i], err = messageToReply(ms[i])
			if err != nil {
				return nil, err
			}
		}
	}

	return reply, nil
}