package redis

import (
	"testing"
	"log"
	"strconv"
)

var client *Client

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	client.Set("myKey", "myValue")
	for i := 0; i < 10; i ++ {
		v, _ := client.Get("myKey")
		if v != "myValue" {
			t.Errorf("get %s, but shoule be %s\n", v, "myValue")
		}
	}
	client.Del("myKey")
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

func BenchmarkGet(b *testing.B) {
	client.Set("myKey", "myValue")
	for i := 0; i < b.N; i ++ {
		client.Get("myKey")
	}
	client.Del("myKey")
}

func BenchmarkStringCopy(b *testing.B) {
	s := "GET"
	buffer := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		copy(buffer[:], []byte(s))
	}
}
