package gedis

import (
	"strings"
	"errors"
)

type Error struct {
	Err error
}

func (err *Error) Error() string {
	return err.Err.Error()
}

func (err *Error) ReadOnly() bool {
	return strings.HasPrefix(err.Error(), "READONLY")
}

type ReplyType int8

const (
	StatusReply ReplyType = iota
	ErrorReply
	IntegerReply
	NilReply
	BulkReply
	MultiReply
)

// Redis reply 的封装
type Reply struct {
	Type ReplyType

	Err  error

	buf  []byte

	int  int64

	Sub  []*Reply
}

func (r *Reply) Bytes() ([] byte, error) {
	if r.Type == ErrorReply {
		return nil, r.Err
	}
	if (r.Type == StatusReply || r.Type == BulkReply) {
		return r.buf, nil
	}
	return nil, errors.New("string value is not available for this reply type")
}
