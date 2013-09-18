package redis

import (
	"errors"
	"fmt"
	"net"
	"strconv"
)

var needNewReadBuffer map[string]bool = map[string]bool{
	"LRANGE":  true,
	"MGET":    true,
	"HGETALL": true,
}

type ByteBuffer struct {
	buffer     []byte
	pos, limit int
}

type RedisConn struct {
	conn net.Conn
	rbuf *ByteBuffer
	wbuf *ByteBuffer
}

func (p *ByteBuffer) moreSpace(n int, write bool) {
	if p.pos+n > cap(p.buffer) {
		size := cap(p.buffer) * 2
		if size < p.pos+n {
			size = p.pos + n
		}
		tmp := make([]byte, size)
		if write {
			copy(tmp, p.buffer[:p.pos]) // for write buffer
		} else {
			copy(tmp, p.buffer[p.pos:p.limit])
		}
		p.buffer = tmp
	}
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
	p.buffer[p.pos] = '\r'
	p.buffer[p.pos+1] = '\n'
	p.pos += 2
}

func (p *ByteBuffer) writeBytes(bs []byte) {
	p.moreSpace(len(bs)+10, true)
	p.buffer[p.pos] = '$'
	p.pos += 1
	p.writeInt(len(bs))
	copy(p.buffer[p.pos:], bs)
	p.pos += len(bs) + 2 //  including \r\n

	p.buffer[p.pos-2] = '\r'
	p.buffer[p.pos-1] = '\n'
}

func (buf *ByteBuffer) encodeRequest(cmd string, args [][]byte) {
	buf.buffer[0] = '*'
	buf.pos = 1
	buf.writeInt(len(args) + 1)
	buf.writeBytes([]byte(cmd))

	for _, s := range args {
		buf.writeBytes(s)
	}
}

func (c *RedisConn) readMore() error {
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
			c.rbuf.moreSpace(128, false)
			err := c.readMore()
			if err != nil {
				return nil, err
			}
		}
		if c.rbuf.buffer[c.rbuf.pos] == '\r' {
			if c.rbuf.pos+2 > c.rbuf.limit {
				c.rbuf.moreSpace(128, false)
				err := c.readMore()
				if err != nil {
					return nil, err
				}
			}
			c.rbuf.pos += 2
			break
		}
		c.rbuf.pos += 1
	}
	return c.rbuf.buffer[start : c.rbuf.pos-2], nil
}

func (c *RedisConn) readResponse() (interface{}, error) {
	if c.rbuf.pos == c.rbuf.limit {
		c.rbuf.moreSpace(128, false)
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
	// Status Reply, eg: +OK
	case '+':
		return line, nil
		//  Error Reply, eg: -ERR unknown command 'foobar'
	case '-':
		return nil, errors.New(string(line))
		//  Integer Reply, eg: 1
	case ':':
		return strconv.Atoi(string(line))
		// Bulk replies, $6\r\nfoobar\r\n
	case '$':
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, err
		}
		if length > 0 {
			c.rbuf.moreSpace(length+2, false)
			for c.rbuf.pos+length+2 > c.rbuf.limit {
				err := c.readMore()
				if err != nil {
					return nil, err
				}
			}
			start := c.rbuf.pos
			c.rbuf.pos += length + 2
			return c.rbuf.buffer[start : c.rbuf.pos-2], nil
		} else if length == 0 {
			return []byte(""), nil
		} else {
			// NULL Bulk Reply, do not return an empty string, but a nil object,
			return nil, nil
		}
		// Multi-bulk replies, LRANGE mylist 0 3
	case '*':
		size, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, RedisError("MultiBulk reply expected a number")
		}
		if size > 0 {
			rets := make([]interface{}, size)
			for i := 0; i < size; i++ {
				rets[i], err = c.readResponse()
				if err != nil {
					return nil, err
				}
			}
			return rets, nil
		} else {
			return nil, nil
		}
	}
	return nil, fmt.Errorf("Unkown %s", c.rbuf.buffer[0:1])
}

func (c *RedisConn) send(cmd string, rbuf bool, args ...[]byte) (interface{}, error) {
	c.wbuf.encodeRequest(cmd, args) // reuse wbuf
	pos := 0
	for pos < c.wbuf.pos {
		n, err := c.conn.Write(c.wbuf.buffer[pos:c.wbuf.pos])
		if err != nil {
			return nil, err
		}
		pos += n
	}
	if rbuf { // using a new buffer, avoid copy
		old := c.rbuf
		c.rbuf = &ByteBuffer{buffer: make([]byte, BufferSize)}
		defer func() { c.rbuf = old }() // restore
		return c.readResponse()
	} else {
		c.rbuf.pos, c.rbuf.limit = 0, 0
		return c.readResponse()
	}
}
