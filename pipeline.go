package redis

import (
	"strconv"
)

type Pipeline struct {
	client *Client
	con    *RedisConn
	count  int
}

func (pipe *Pipeline) Hincrby(key, field string, inc int) {
	pipe.count += 1
	wbuf := pipe.con.wbuf
	wbuf.buffer[wbuf.pos] = '*'
	wbuf.pos += 1
	wbuf.writeInt(4)
	wbuf.writeBytes([]byte("HINCRBY"))

	wbuf.writeBytes([]byte(key))
	wbuf.writeBytes([]byte(field))
	wbuf.writeBytes([]byte(strconv.Itoa(inc)))
}

func (pipe *Pipeline) Expire(key string, seconds int) {
	pipe.count += 1
	wbuf := pipe.con.wbuf
	wbuf.buffer[wbuf.pos] = '*'
	wbuf.pos += 1
	wbuf.writeInt(3)
	wbuf.writeBytes([]byte("EXPIRE"))
	wbuf.writeBytes([]byte(key))
	wbuf.writeBytes([]byte(strconv.Itoa(seconds)))
}

func (pipe *Pipeline) Ping() {
	pipe.count += 1
	wbuf := pipe.con.wbuf
	wbuf.buffer[wbuf.pos] = '*'
	wbuf.pos += 1
	wbuf.writeInt(1)
	wbuf.writeBytes([]byte("PING"))
}

func (pipe *Pipeline) Execute() error {
	defer pipe.client.returnCon(pipe.con)
	c := pipe.con
	pos := 0
	for pos < c.wbuf.pos {
		n, err := c.conn.Write(c.wbuf.buffer[pos:c.wbuf.pos])
		if err != nil {
			return err
		}
		pos += n
	}
	var e error
	for i := 0; i < pipe.count; i++ {
		if _, err := c.readResponse(); err != nil {
			e = err
		}
	}
	return e
}
