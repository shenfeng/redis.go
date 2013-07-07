package redis

import (
	"net"
	"errors"
	"strconv"
	//	"log"
	"log"
)

const (
	defaultPoolSize = 5
	bufferSize      = 1024*24
)

type ByteBuffer struct {
	buffer                                                                    []byte
	pos,                                                                limit int
}

type RedisConn struct {
	conn    net.Conn
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
		for ; i > 0; i = i/10 {
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

func (p *ByteBuffer) writeCRLF() {
	p.buffer[p.pos] = '\r'
	p.buffer[p.pos + 1] = '\n'
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

func (buf *ByteBuffer) encodeRequest(cmd string, args []string) {
	buf.buffer[0] = '*'
	buf.pos = 1
	buf.writeInt(len(args) + 1)
	buf.writeCRLF()

	buf.writeString(cmd)
	buf.writeCRLF()

	for _, s := range args {
		if buf.pos + len(s) + 20 > cap(buf.buffer) {
			buf.buffer = make([]byte, cap(buf.buffer)*2 + len(s))
		}
		buf.writeString(s)
		buf.writeCRLF()
	}
}

type Client struct {
	Addr string
	Db   int
	pool chan *RedisConn
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

func (client *Client) getCon() (*RedisConn, error) {
	c := <-client.pool
	if c == nil {
		return client.openConn()
	}
	return c, nil
}

func (c *RedisConn) readMore() error {
	if c.rbuf.limit + 10 > cap(c.rbuf.buffer) {
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
			c.rbuf.pos += 1
			break
		}
		c.rbuf.pos += 1
	}
	return c.rbuf.buffer[start:c.rbuf.pos - 2], nil
}

func (client *Client) sendCommand(cmd string, args ...string) (interface {}, error) {
	c, err := client.getCon()
	defer func() {
		client.returnCon(c)
	}()

	if err != nil {
		return nil, err
	}

	c.wbuf.encodeRequest(cmd, args)

	pos := 0
	for pos < c.wbuf.pos {
		n, err := c.conn.Write(c.wbuf.buffer[pos:c.wbuf.pos])
		if err != nil {
			return nil, err
		}
		pos += n
	}

	n, err := c.conn.Read(c.rbuf.buffer)
	log.Println(cmd, c.rbuf.buffer[:n], string(c.rbuf.buffer[:n]), n)

	if err != nil {
		return nil, err
	}
	c.rbuf.pos, c.rbuf.limit = 1, n
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	log.Println(c.rbuf.buffer[0], string(c.rbuf.buffer[0:1]), line)
	switch c.rbuf.buffer[0] {
	case '+':
		return line, nil
	case '-':
		return nil, errors.New(string(line))
	case ':':
		return strconv.Atoi(string(line))
	case '$':
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}

		if c.rbuf.pos + length + 2 < cap(c.rbuf.buffer) {
			tmp := make([]byte, c.rbuf.pos + length + 2)
			copy(tmp, c.rbuf.buffer[:c.rbuf.limit])
			c.rbuf.buffer = tmp
		}

		for c.rbuf.pos + length + 2 > c.rbuf.limit {
			n, err := c.conn.Read(c.rbuf.buffer[c.rbuf.limit:])
			if err != nil {
				return nil, err
			}
			c.rbuf.limit += n
		}

		return c.rbuf.buffer[c.rbuf.pos:c.rbuf.limit], nil
	}

	//
	return nil, nil
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
