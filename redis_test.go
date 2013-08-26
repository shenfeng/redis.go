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

var bytes []byte = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	c, err := NewClient("localhost:6379", 0)
	if err != nil {
		log.Fatal(err)
	}
	client = c
}

func TestWriteInt(t *testing.T) {
	for i := 0; i < 400; i += 13 {
		b := &ByteBuffer{buffer: make([]byte, 1024)}
		b.writeInt(i)
		if string(b.buffer[:b.pos-2]) != strconv.Itoa(i) {
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

func TestBlocking(t *testing.T) {
	client.Del(myKey)

	go func() {
		client.Lpush(myKey, myValue)
	}()

	v, key, _ := client.Brpop(myKey, 0)
	if string(v) != myValue || key != myKey {
		t.Errorf("Brpop get %s", string(v))
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
	if err != KeyDoesNotExist {
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
		t.Errorf("ping err: %v", err)
	}
}

func TestSelect(t *testing.T) {
	err := client.Select(10)
	if err != nil {
		t.Errorf("select err: %v", err)
	}
}

func TestZset(t *testing.T) {
	const KEY = "test_key"
	client.Del(KEY)
	r, err := client.Smembers(KEY)
	if r != nil && err != nil {
		t.Errorf("Failed")
	}
	if r, _ := client.Sadd(KEY, "----------"); !r {
		t.Errorf("Sadd Failed")
	}
	client.Sadd(KEY, "abc")
	if r, _ := client.Smembers(KEY); len(r) != 2 {
		t.Errorf("Error, should be len 2")
	}
}

func TestHmset(t *testing.T) {
	client.Del(myKey)
	client.Hmset(myKey, testObj)
	// client.Hgetall(myKey)
}
