package redis

import (
	"testing"
	"log"
	"strconv"
)

var client *Client

func init() {
	c, err := NewClient("localhost:6379", 0)
	if err != nil {
		log.Fatal(err)
	}
	client = c
}

func TestWriteInt(t *testing.T) {
	for i := 23; i < 40; i++ {
		b := &ByteBuffer{buffer: make([]byte, 1024)}
		b.writeInt(i)
		if string(b.buffer[:b.pos]) != strconv.Itoa(i) {
			t.Errorf("itoa %d get %s\n", i, string(b.buffer[:b.pos]))
		}
	}
}

func TestEncodingRequest(t *testing.T) {
	buf := &ByteBuffer{buffer: make([]byte, 1024)}
	buf.encodeRequest("SET", []string{"mykey", "myvalue"})
	encoded := string(buf.buffer[:buf.pos])
	expect := "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
	if encoded != expect {
		t.Errorf("get %s, but should be %s\n", encoded, expect)
	}
}

func TestGet(t *testing.T) {
	log.Println(client.sendCommand("SET", "myKey", "myValue"))
	log.Println(client.sendCommand("GET", "myKey"))
	log.Println(client.sendCommand("INCR", "myIncK"))
	log.Println(client.sendCommand("foolbar", "myIncK"))
	log.Println(client.sendCommand("HMSET", "myhkey", "key1", "value1", "key2", "value2"))
	//	client.sendCommand("HGETALL", "myhkey")
}

func BenchmarkEncodingRequest(b *testing.B) {
	buf := &ByteBuffer{buffer: make([]byte, 1024)}
	buf.encodeRequest("SET", []string{"mykey", "myvalue"})
	b.SetBytes(int64(buf.pos))

	for i := 0; i < b.N; i++ {
		buf.encodeRequest("SET", []string{"mykey", "myvalue"})
	}
}

func BenchmarkGetConn(b *testing.B) {
	for i := 0; i < b.N; i ++ {
		c, _ := client.getCon()
		client.returnCon(c)
	}
}

func BenchmarkStringCopy(b *testing.B) {
	s := "GET"
	buffer := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		copy(buffer[:], []byte(s))
	}
}
