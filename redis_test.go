package redis

import (
	"fmt"
	"log"
	"strconv"
	"testing"
)

const (
	myKey   = "mykey"
	myValue = "myValue10000000000000000"
)

var client *Client

var testObj map[string]interface{} = map[string]interface{}{"key1": "value1", "key2": "value2", "key3": 101}

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
	buf.encodeRequest("SET", [][]byte{[]byte(myKey), []byte(myValue)})
	encoded := string(buf.buffer[:buf.pos])

	expect := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(myKey), myKey, len(myValue), myValue)
	if encoded != expect {
		t.Errorf("get %s, but should be %s\n", encoded, expect)
	}
}

func TestSet(t *testing.T) {
	client.Set(myKey, myValue)
	for i := 0; i < 4; i++ {
		v, _ := client.GetString(myKey)
		if v != myValue {
			t.Errorf("get %s, but shoule be %s\n", v, myValue)
		}
	}
	client.Del(myKey)

	client.Set(myKey, []byte(myValue))
	v, _ := client.Get(myKey)
	if string(v) != myValue {
		t.Errorf("get %s, but shoule be %s\n", v, myValue)
	}

	for i := 0; i < 2; i++ {
		client.Del(myKey)
		r, _ := client.Setnx(myKey, myValue)
		if !r {
			t.Error("setnx should ok the first time")
		}
		r, _ = client.Setnx(myKey, myValue)
		if r {
			t.Error("setnx should fail the second time")
		}
	}

	client.Del(myKey)
	_, err := client.Get(myKey)
	if err != doesNotExist {
		t.Error("should return Key does not exits")
	}

	vs, _ := client.MGet(myKey, myKey)
	for _, v := range vs {
		if v != nil {
			t.Error("Mget not exits key should return nil")
		}
	}

	client.Setex(myKey, 10, []byte(myValue))
	v, _ = client.Get(myKey)
	if string(v) != myValue {
		t.Errorf("get %s, but shoule be %s\n", v, myValue)
	}

	vs, _ = client.MGet(myKey, myKey)
	for _, v := range vs {
		if string(v) != myValue {
			t.Errorf("Mget does not return the right value, get %s, should be %s", string(v), myValue)
		}
	}

	client.Del(myKey)
}

func TestPing(t *testing.T) {
	err := client.Ping()
	if err != nil {
		t.Errorf("ping err", err)
	}
}

func TestHmset(t *testing.T) {
	client.Del(myKey)
	client.Hmset(myKey, testObj)
	client.Hgetall(myKey)
}

func BenchmarkEncodingRequest(b *testing.B) {
	buf := &ByteBuffer{buffer: make([]byte, 1024)}
	tmp := [][]byte{[]byte(myKey), []byte(myValue)}
	buf.encodeRequest("SET", tmp)
	b.SetBytes(int64(buf.pos))

	for i := 0; i < b.N; i++ {
		buf.encodeRequest("SET", tmp)
	}
}

func BenchmarkGetConn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c, _ := client.getCon()
		client.returnCon(c)
	}
}

func BenchmarkPing(b *testing.B) {
	for i := 0; i < b.N; i ++ {
		client.Ping()
	}
}

func BenchmarkGet(b *testing.B) {
	client.Set(myKey, myValue)
	for i := 0; i < b.N; i++ {
		client.Get(myKey)
	}
	client.Del(myKey)
}

func BenchmarkM10Get(b *testing.B) {
	client.Set(myKey, myValue)
	for i := 0; i < b.N; i++ {
		client.MGet(myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey)
	}
	client.Del(myKey)
}

func BenchmarkM10GetString(b *testing.B) {
	client.Set(myKey, myValue)
	for i := 0; i < b.N; i++ {
		client.MGetString(myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey, myKey)
	}
	client.Del(myKey)
}

func BenchmarkGetString(b *testing.B) {
	client.Set(myKey, myValue)
	for i := 0; i < b.N; i++ {
		client.GetString(myKey)
	}
	client.Del(myKey)
}

func BenchmarkSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		client.Set(myKey, myValue)
	}
	client.Del(myKey)
}

func BenchmarkHmset(b *testing.B) {
	client.Del(myKey)
	for i := 0; i < b.N; i++ {
		client.Hmset(myKey, testObj)
	}
	client.Del(myKey)
}
