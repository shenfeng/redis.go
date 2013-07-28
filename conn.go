package redis

import (
	"errors"
	"fmt"
	"net"
	"strconv"
)

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

func (p *ByteBuffer) writeCRLF() {
	p.buffer[p.pos] = '\r'
	p.buffer[p.pos+1] = '\n'
	p.pos += 2
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

	buf.writeBytes([]byte(cmd))
	buf.writeCRLF()

	for _, s := range args {
		if buf.pos+len(s)+20 > cap(buf.buffer) {
			buf.buffer = make([]byte, cap(buf.buffer)*2+len(s))
		}
		buf.writeBytes(s)
		buf.writeCRLF()
	}
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
			return nil, nil
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
