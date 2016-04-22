package gedis

import (
	"net"
	"time"
	"bufio"
	"redis/resp"
	"strings"
	"errors"
)

var LoadingError string = errors.New("server is busy to loading data")

const (
	bufSize int = 4096
	default_host = "localhost"
	default_port = 6379
)

// 一个实例表示一个到Redis Server的连接
type Connection struct {
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

func DefaultDial() (*Connection, error) {
	return Dial(default_host, default_port)
}

func Dial(host string, port int) (*Connection, error) {
	return DialWithTimeout(host, port, time.Duration(0))
}

func DialWithTimeout(host string, port int, timeout time.Duration) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", host + ":" + port, timeout)
	if err != nil {
		return nil, err
	}
	c := new(Connection)
	c.Conn = conn
	c.timeout = timeout
	c.reader = bufio.NewReaderSize(conn, bufSize)
	c.writeBuf = make([]byte, 0, 1024)
	return c, err
}

// 关闭连接
func (c *Connection) Close() error {
	return c.Conn.Close()
}

// 执行Redis命令
func (c *Connection) Exec(cmd string, args...interface{}) *Reply {
	err := c.writeRequest(&request{cmd, args})
	if err != nil {
		return &Reply{Type:ErrorReply, Err:err}
	}
	return c.ReadReply()
}

func (c *Connection)ReadReply() *Reply {
	c.setReadTimeout()
	return c.parse()
}

func (c *Connection) Append(cmd string, args...interface{}) {
	c.pending = append(c.pending, &request{cmd, args})
}

func (c *Connection) writeRequest(requests...*request) error {
	c.setWriteTimeout()
	for i := range requests {
		req := make([]interface{}, 0, len(requests[i].args) + 1)
		req = append(req, requests[i].cmd)
		req = append(req, requests[i].args...)
		buf := resp.AppendArbitraryAsFlattenedStrings()
		_, err := c.Conn.Write(buf)
		if err != nil {
			c.Close()
			return err
		}
	}
	return nil
}

func (c *Connection) setReadTimeout() {
	if c.timeout != 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
}

func (c *Connection) setWriteTimeout() {
	if c.timeout != 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
}

func (c *Connection)parse() *Reply {
	m, err := resp.ReadMessage(c.reader)
	if err != nil {
		if t, ok := err.(*net.OpError); !ok || t.Timeout() {
			c.Close()
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
		reply.Children = make([]*Reply, len(ms))
		for i := range ms {
			reply.Children[i], err = messageToReply(ms[i])
			if err != nil {
				return nil, err
			}
		}
	}

	return reply, nil
}