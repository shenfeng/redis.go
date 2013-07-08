package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	// "log"
	"net"
	"reflect"
	"strconv"
)

const (
	defaultPoolSize = 5
	bufferSize      = 1024 * 24
)

type RedisError string

func (err RedisError) Error() string { return "Redis Error: " + string(err) }

type ByteBuffer struct {
	buffer     []byte
	pos, limit int
}

type RedisConn struct {
	conn net.Conn
	rbuf *ByteBuffer
	wbuf *ByteBuffer
}

func (p *ByteBuffer) writeInt(i int) {
	if i < 10 {
		p.buffer[p.pos] = byte(i + '0')
		p.pos += 1
	} else {
		pos := p.pos
		start := pos
		for ; i > 0; i = i / 10 {
			p.buffer[pos] = byte(i%10 + '0')
			pos += 1
		}

		p.pos = pos
		pos -= 1
		for start < pos {
			p.buffer[start], p.buffer[pos] = p.buffer[pos], p.buffer[start]
			start += 1
			pos -= 1
		}
	}
}

func toBytes(value interface{}) []byte {
	switch v := value.(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	}
	panic("Only []byte, string is understandable")
}

func (p *ByteBuffer) writeCRLF() {
	p.buffer[p.pos] = '\r'
	p.buffer[p.pos+1] = '\n'
	p.pos += 2
}

func (p *ByteBuffer) writeString(s string) {
	p.buffer[p.pos] = '$'
	p.pos += 1
	p.writeInt(len(s))
	p.writeCRLF()
	copy(p.buffer[p.pos:], []byte(s))
	p.pos += len(s)
}

func (p *ByteBuffer) writeBytes(bs []byte) {
	p.buffer[p.pos] = '$'
	p.pos += 1
	p.writeInt(len(bs))
	p.writeCRLF()
	copy(p.buffer[p.pos:], bs)
	p.pos += len(bs)
}

func (buf *ByteBuffer) encodeRequest(cmd string, args [][]byte) {
	buf.buffer[0] = '*'
	buf.pos = 1
	buf.writeInt(len(args) + 1)
	buf.writeCRLF()

	buf.writeString(cmd)
	buf.writeCRLF()

	for _, s := range args {
		if buf.pos+len(s)+20 > cap(buf.buffer) {
			buf.buffer = make([]byte, cap(buf.buffer)*2+len(s))
		}
		buf.writeBytes(s)
		buf.writeCRLF()
	}
}

type Client struct {
	Addr string
	Db   int
	pool chan *RedisConn
}

func (client *Client) getCon() (*RedisConn, error) {
	c := <-client.pool
	if c == nil {
		return client.openConn()
	}
	return c, nil
}

func (c *RedisConn) readMore() error {
	if c.rbuf.limit+10 > cap(c.rbuf.buffer) {
		copy(c.rbuf.buffer, c.rbuf.buffer[c.rbuf.pos:c.rbuf.limit])
		pos := c.rbuf.pos
		c.rbuf.pos = 0
		c.rbuf.limit = c.rbuf.limit - pos
	}
	n, err := c.conn.Read(c.rbuf.buffer[c.rbuf.limit:])
	c.rbuf.limit += n
	if err != nil {
		return err
	}
	return nil
}

func (c *RedisConn) readLine() ([]byte, error) {
	start := c.rbuf.pos
	for {
		if c.rbuf.pos >= c.rbuf.limit {
			err := c.readMore()
			if err != nil {
				return nil, err
			}
		}
		if c.rbuf.buffer[c.rbuf.pos] == '\r' {
			c.rbuf.pos += 2
			break
		}
		c.rbuf.pos += 1
	}
	return c.rbuf.buffer[start : c.rbuf.pos-2], nil
}

func (c *RedisConn) readResponse() (interface{}, error) {
	if c.rbuf.pos == c.rbuf.limit {
		err := c.readMore()
		if err != nil {
			return nil, err
		}
	}
	pos := c.rbuf.pos
	c.rbuf.pos += 1
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	switch c.rbuf.buffer[pos] {
	case '+':
		return line, nil
	case '-':
		return nil, errors.New(string(line))
	case ':':
		return strconv.Atoi(string(line))
	case '$':
		length, err := strconv.Atoi(string(line))
		if length > 0 {
			if err != nil {
				return nil, err
			}
			if c.rbuf.pos+length+2 > cap(c.rbuf.buffer) {
				tmp := make([]byte, c.rbuf.pos+length+2)
				copy(tmp, c.rbuf.buffer[:c.rbuf.limit])
				c.rbuf.buffer = tmp
			}
			for c.rbuf.pos+length+2 > c.rbuf.limit {
				err := c.readMore()
				if err != nil {
					return nil, err
				}
			}
			start := c.rbuf.pos
			c.rbuf.pos += length + 2
			return c.rbuf.buffer[start : c.rbuf.pos-2], nil
		} else {
			return nil, errors.New("No such key")
		}
	case '*':
		size, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, RedisError("MultiBulk reply expected a number")
		}
		rets := make([]interface{}, size)
		for i := 0; i < size; i++ {
			rets[i], err = c.readResponse()
			if err != nil {
				return nil, err
			}
		}
		return rets, nil
	}
	return nil, fmt.Errorf("Unkown %s", c.rbuf.buffer[0:1])
}

func (c *RedisConn) sendCommand(cmd string, args ...[]byte) (interface{}, error) {
	c.wbuf.encodeRequest(cmd, args)
	pos := 0
	for pos < c.wbuf.pos {
		n, err := c.conn.Write(c.wbuf.buffer[pos:c.wbuf.pos])
		if err != nil {
			return nil, err
		}
		pos += n
	}
	c.rbuf.pos, c.rbuf.limit = 0, 0
	return c.readResponse()
}

func (client *Client) returnCon(c *RedisConn) {
	client.pool <- c
}

func (client *Client) openConn() (*RedisConn, error) {
	c, err := net.Dial("tcp", client.Addr)
	if err == nil {
		rb := &ByteBuffer{buffer: make([]byte, bufferSize)}
		wb := &ByteBuffer{buffer: make([]byte, bufferSize)}
		return &RedisConn{conn: c, rbuf: rb, wbuf: wb}, nil
	}
	return nil, err
}

func NewClient(addr string, db int) (*Client, error) {
	poolSize := defaultPoolSize
	pool := make(chan *RedisConn, poolSize)
	for i := 0; i < poolSize; i++ {
		pool <- nil
	}
	client := &Client{Addr: addr, Db: db, pool: pool}
	_, err := client.getCon()
	if err != nil {
		return nil, err
	} else {
		return client, nil
	}
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

func (client *Client) Hmset(key string, mapping map[string]interface{}) error {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	var args [][]byte
	args = append(args, []byte(key))
	// err := mappingToArgs(reflect.ValueOf(mapping), &args)

	if err != nil {
		return err
	}

	_, err = c.sendCommand("HMSET", args...)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) Hgetall(key string) (m map[string]string, err error) {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	rets, err := c.sendCommand("HGETALL", []byte(key))
	if err != nil {
		return m, err
	}

	for i := 0; i < len(rets.([]interface{})); i += 2 {
		// m[rets
	}
	return m, err
}

func (client *Client) Setnx(key string, data interface{}) (bool, error) {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	v, err := c.sendCommand("SETNX", []byte(key), toBytes(data))
	if err != nil {
		return false, err
	}
	return v.(int) == 1, nil
}

func (client *Client) Setex(key string, seconds int, data interface{}) error {
	c, err := client.getCon()
	_, err = c.sendCommand("SET", []byte(key), []byte(strconv.Itoa(seconds)), toBytes(data))
	client.returnCon(c)

	if err != nil {
		return err
	}
	return nil
}

func (client *Client) Set(key string, data interface{}) error {
	c, err := client.getCon()
	_, err = c.sendCommand("SET", []byte(key), toBytes(data))
	client.returnCon(c)

	if err != nil {
		return err
	}
	return nil
}

func (client *Client) Get(key string) ([]byte, error) {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	value, err := c.sendCommand("GET", []byte(key))
	if err != nil {
		return nil, err
	}
	val := value.([]byte)
	copied := make([]byte, len(val))
	// value is just a reference to a buffer, need copy to protect buffer
	copy(copied, val)
	return val, err
}

func (client *Client) GetString(key string) (string, error) {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	value, err := c.sendCommand("GET", []byte(key))
	if err != nil {
		return "", err
	}
	return string(value.([]byte)), err
}

func (client *Client) Del(key string) error {
	c, err := client.getCon()
	_, err = c.sendCommand("Del", []byte(key))
	client.returnCon(c)
	if err != nil {
		return err
	}
	return nil
}
