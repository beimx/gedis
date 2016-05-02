package gedis

import (
	"container/list"
	"errors"
	"net"
)

type PubPubSubReplyType uint8

const (
	ErrorReply PubPubSubReplyType = iota
	SubscribeReply
	UnSubscribeReply
	MessageReply
)

type PubSubClient struct {
	gedis    *Gedis

	messages *list.List
}

func NewPubSubClient(gedis *Gedis) *PubSubClient {
	return &PubSubClient{gedis, &list.List{}}
}

// Subscribe makes a Redis "SUBSCRIBE" command on the provided channels
func (c *PubSubClient) Subscribe(channels ...interface{}) *PubSubReply {
	return c.filterMessages("SUBSCRIBE", channels...)
}

// PSubscribe makes a Redis "PSUBSCRIBE" command on the provided patterns
func (c *PubSubClient) PSubscribe(patterns ...interface{}) *PubSubReply {
	return c.filterMessages("PSUBSCRIBE", patterns...)
}

// Unsubscribe makes a Redis "UNSUBSCRIBE" command on the provided channels
func (c *PubSubClient) Unsubscribe(channels ...interface{}) *PubSubReply {
	return c.filterMessages("UNSUBSCRIBE", channels...)
}

// PUnsubscribe makes a Redis "PUNSUBSCRIBE" command on the provided patterns
func (c *PubSubClient) PUnsubscribe(patterns ...interface{}) *PubSubReply {
	return c.filterMessages("PUNSUBSCRIBE", patterns...)
}

func (c *PubSubClient) Receive() *PubSubReply {
	return c.receive(false)
}

func (c *PubSubClient)Close() {
	c.gedis.Close()
}

func (c *PubSubClient) receive(skipBuffer bool) *PubSubReply {
	if c.messages.Len() > 0 && !skipBuffer {
		v := c.messages.Remove(c.messages.Front())
		return v.(*PubSubReply)
	}
	r := c.gedis.ReadReply()
	return c.parseReply(r)
}

func (c *PubSubClient) filterMessages(cmd string, names ...interface{}) *PubSubReply {
	r := c.gedis.Cmd(cmd, names...)
	var sr *PubSubReply
	for i := 0; i < len(names); i++ {
		// If nil we know this is the first loop
		if sr == nil {
			sr = c.parseReply(r)
		} else {
			sr = c.receive(true)
		}
		if sr.Type == MessageReply {
			c.messages.PushBack(sr)
			i--
		}
	}
	return sr
}

func (c *PubSubClient) parseReply(reply *Reply) *PubSubReply {
	sr := &PubSubReply{Reply: reply}
	switch reply.Type {
	case MultiReply:
		if len(reply.Children) < 3 {
			sr.Err = errors.New("reply is not formatted as a subscription reply")
			return sr
		}
	case ErrorReply:
		sr.Err = reply.Err
		return sr
	default:
		sr.Err = errors.New("reply is not formatted as a subscription reply")
		return sr
	}

	rtype, err := reply.Children[0].Str()
	if err != nil {
		sr.Err = errors.New("subscription multireply does not have string value for type")
		sr.Type = ErrorReply
		return sr
	}

	//first element
	switch rtype {
	case "subscribe", "psubscribe":
		sr.Type = SubscribeReply
		count, err := reply.Children[2].Int()
		if err != nil {
			sr.Err = errors.New("subscribe reply does not have int value for sub count")
			sr.Type = ErrorReply
		} else {
			sr.SubCount = count
		}
	case "unsubscribe", "punsubscribe":
		sr.Type = UnSubscribeReply
		count, err := reply.Children[2].Int()
		if err != nil {
			sr.Err = errors.New("unsubscribe reply does not have int value for sub count")
			sr.Type = ErrorReply
		} else {
			sr.SubCount = count
		}
	case "message", "pmessage":
		var chanI, msgI int

		if rtype == "message" {
			chanI, msgI = 1, 2
		} else {
			// "pmessage"
			chanI, msgI = 2, 3
			pattern, err := reply.Children[1].Str()
			if err != nil {
				sr.Err = errors.New("subscription multireply does not have string value for pattern")
				sr.Type = ErrorReply
				return sr
			}
			sr.Pattern = pattern
		}

		sr.Type = MessageReply
		channel, err := reply.Children[chanI].Str()
		if err != nil {
			sr.Err = errors.New("subscription multireply does not have string value for channel")
			sr.Type = ErrorReply
			return sr
		}
		sr.Channel = channel
		msg, err := reply.Children[msgI].Str()
		if err != nil {
			sr.Err = errors.New("message reply does not have string value for body")
			sr.Type = ErrorReply
		} else {
			sr.Message = msg
		}
	default:
		sr.Err = errors.New("suscription multireply has invalid type: " + rtype)
		sr.Type = ErrorReply
	}
	return sr
}

type PubSubReply struct {
	Type     PubPubSubReplyType // PubSubReply type
	Channel  string             // channel of pub/sub (MessageReply)
	Pattern  string             // The pattern that was matched by this reply, if PSubscribe was used
	SubCount int                // Count of subs active after this action (SubscribeReply or UnsubscribeReply)
	Message  string             // Publish message (MessageReply)
	Err      error
	Reply    *Reply             // Reply of origin Gedis (MessageReply)
}

func (r *PubSubReply) Timeout() bool {
	if r.Err == nil {
		return false
	}
	t, ok := r.Err.(*net.OpError)
	return ok && t.Timeout()
}
