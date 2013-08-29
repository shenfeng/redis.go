package redis

import (
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strconv"
	"sync"
)

type Client struct {
	Addr   string
	Db     int
	MaxCon int

	mu   sync.Mutex //  protect conns
	cons []*RedisConn
}

func toBytes(value interface{}) []byte {
	switch v := value.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	case int:
		return []byte(strconv.Itoa(v))
	}
	panic("Only []byte, string is understandable")
}

func copyBytes(b []byte) (r []byte) {
	r = make([]byte, len(b))
	copy(r, b)
	return
	// return append(r, b...)
}

func (client *Client) returnCon(c *RedisConn) {
	client.mu.Lock()
	if len(client.cons) >= client.MaxCon {
		c.conn.Close()
	} else {
		client.cons = append(client.cons, c)
	}
	client.mu.Unlock()
}

func (client *Client) closeAll() {
	client.mu.Lock()
	for _, c := range client.cons {
		c.conn.Close()
	}
	client.cons = nil
	client.mu.Unlock()
}

func (client *Client) getCon() (con *RedisConn, err error) {
	client.mu.Lock()
	if len(client.cons) > 0 {
		con = client.cons[len(client.cons)-1]
		client.cons = client.cons[0 : len(client.cons)-1]
	}
	client.mu.Unlock()
	if con != nil {
		return
	}

	return client.openConn()
}

func (client *Client) openConn() (*RedisConn, error) {
	c, err := net.Dial("tcp", client.Addr)
	if err == nil {
		rb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		wb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		c := &RedisConn{conn: c, rbuf: rb, wbuf: wb}
		if client.Db > 0 {
			c.send("SELECT", false, []byte(strconv.Itoa(client.Db)))
		}
		if client.MaxCon == 0 {
			client.MaxCon = DefaultMaxCon
		}
		return c, nil
	}
	return nil, err
}

func (client *Client) sendCommand(cmd string, newRbuf bool, args ...[]byte) (interface{}, error) {
	if c, err := client.getCon(); err != nil {
		return nil, err
	} else {
		r, err := c.send(cmd, newRbuf, args...)
		if err == nil { // TODO, only network, retry if network error
			client.returnCon(c)
		}
		return r, err
	}
}

func (client *Client) simple(cmd string, args ...[]byte) error {
	_, err := client.sendCommand(cmd, false, args...)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) blockPop(cmd string, key interface{}, seconds int) ([]byte, string, error) {
	var args [][]byte
	switch v := key.(type) {
	case string:
		args = append(args, []byte(v))
	case []string:
		for _, s := range v {
			args = append(args, []byte(s))
		}
	default:
		panic("Only string or []string is allowed in blocking pop")
	}
	args = append(args, []byte(strconv.Itoa(seconds)))

	value, err := client.sendCommand(cmd, true, args...)
	if err != nil {
		return nil, "", err
	}

	if value != nil {
		vs := value.([]interface{})
		return vs[1].([]byte), string(vs[0].([]byte)), nil
	}
	return nil, "", nil
}

func (client *Client) listPush(cmd string, key string, values []interface{}) (int, error) {
	args := make([][]byte, len(values) + 1)
	args[0] = []byte(key)
	for i, v := range values {
		args[i+1] = toBytes(v)
	}
	value, err := client.sendCommand(cmd, false, args...)
	if err != nil {
		return 0, err
	}

	return value.(int), nil
}

func mappingToArgs(v reflect.Value, args *[][]byte) error {
	switch v.Kind() {
	case reflect.Ptr:
		return mappingToArgs(reflect.Indirect(v), args)
	case reflect.Interface:
		return mappingToArgs(v.Elem(), args)
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return errors.New("Unsupported type - map key must be a string")
		}
		keys := v.MapKeys()
		for _, k := range keys {
			*args = append(*args, []byte(k.String()))
			bs, err := json.Marshal(v.MapIndex(k).Interface())
			if err != nil {
				return err
			}
			*args = append(*args, bs)
		}
		return nil
	}
	panic("v")
}

func NewClient(addr string, db int) (*Client, error) {
	client := &Client{Addr: addr, Db: db}
	c, err := client.getCon()
	if err != nil {
		return nil, err
	} else {
		client.returnCon(c)
		return client, nil
	}
}
