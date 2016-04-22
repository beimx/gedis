package gedis

import (
	"strings"
	"errors"
	"strconv"
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
	Type     ReplyType

	Err      error

	buf      []byte

	int      int64

	Children []*Reply
}

/////////////////////一下方法用于将Reply对象转换成不同类型的值返回/////////////////

func (r *Reply) Bytes() ([] byte, error) {
	if r.Type == ErrorReply {
		return nil, r.Err
	}
	if (r.Type == StatusReply || r.Type == BulkReply) {
		return r.buf, nil
	}
	return nil, errors.New("string value is not available for this reply type")
}

func (r *Reply)Str() (string, error) {
	bytes, err := r.Bytes()
	if err != nil {
		return nil, err
	}
	return string(bytes), nil
}

func (r *Reply)Int64() (int64, error) {
	if r.Type == ErrorReply {
		return 0, r.Err
	}
	if r.Type == IntegerReply {
		str, err := r.Str()
		if err == nil {
			i64, err := strconv.ParseInt(str, 0, 64)
			if err != nil {
				return 0, errors.New("failed to parse int64 from string value")
			}
			return i64, nil
		}
		return 0, errors.New("integer value is not available for this reply type")
	}
	return r.int, nil
}

func (r *Reply)Int() (int, error) {
	i64, err := r.Int64()
	if err != nil {
		return 0, err
	}
	return int(i64), nil
}

func (r *Reply)Float64() (float64, error) {
	if r.Type == ErrorReply {
		return 0, r.Err
	}
	if r.Type == BulkReply {
		str, err := r.Str()
		if err != nil {
			return 0, err
		}
		f64, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return 0, errors.New("failed to parse float64 from string value")
		}
		return f64, nil
	}

	return 0, errors.New("float value is not available for this reply type")
}

func (r *Reply)Bool() (bool, error) {
	if r.Type == ErrorReply {
		return false, r.Err
	}
	i, err := r.Int()
	if err == nil {
		if i == 0 {
			return false, nil
		}
		return true, nil
	}
	s, err := r.Str()
	if err == nil {
		if s == "0" {
			return false, nil
		}
		return true, nil
	}
	return false, errors.New("bool value is not available for this reply type")
}

func (r *Reply)List() ([]string, error) {
	if r.Type == ErrorReply {
		return nil, r.Err
	}
	if r.Type != MultiReply {
		return nil, errors.New("reply type is not MultiReply")
	}
	list := make([]string, len(r.Children))
	for i, v := range r.Children {
		if r.Type == BulkReply {
			list[i] = string(v.buf)
		}else if r.Type == NilReply {
			list[i] = ""
		}else {
			return nil, errors.New("children reply type is not BulkReply or NilReply")
		}
	}
	return list, nil
}

func (r *Reply)ListBytes() ([]byte, error) {
	if r.Type == ErrorReply {
		return nil, r.Err
	}
	if r.Type != MultiReply {
		return nil, errors.New("reply type is not MultiReply")
	}
	list := make([]byte, len(r.Children))
	for i, v := range r.Children {
		if r.Type == BulkReply {
			list[i] = v.buf
		}else if r.Type == NilReply {
			list[i] = nil
		}else {
			return nil, errors.New("children reply type is not BulkReply or NilReply")
		}
	}
	return list, nil
}

func (r *Reply) Hash() (map[string]string, error) {
	if r.Type == ErrorReply {
		return nil, r.Err
	}
	hash := map[string]string{}
	if r.Type != MultiReply {
		return nil, errors.New("reply type is not MultiReply")
	}
	if len(r.Children) % 2 != 0 {
		return nil, errors.New("reply has odd number of children")
	}
	for i := 0; i <= len(r.Children) / 2; i++ {
		var value string
		key, err := r.Children[i * 2].Str() // 取下标为偶数的child作为key
		if err != nil {
			return nil, errors.New("key child is not string")
		}
		val := r.Children[i * 2 + 1] // 取奇数child作为value
		if val.Type == BulkReply {
			value = string(val.buf)
			hash[key] = value
		}else if val.Type == NilReply {
			//hash[key] == ""
		}else {
			return nil, errors.New("value child type is not BulkReply or NilReply")
		}
	}
	return hash
}

func (r *Reply)Nil() error {
	if r.Type == ErrorReply {
		return r.Err
	}else if r.Type == NilReply {
		return nil
	}else {
		return errors.New("value type is not NilReply")
	}
}

func (r *Reply)String() string {
	switch r.Type {
	case ErrorReply:
		return r.Err.Error()
	case StatusReply:
		fallthrough
	case BulkReply:
		return string(r.buf)
	case IntegerReply:
		return strconv.FormatInt(r.int, 10)
	case NilReply:
		return "<nil>"
	case MultiReply:
		s := "[ "
		for _, e := range r.Children {
			s = s + e.String() + " "
		}
		return s + "]"
	}

	return ""
}
