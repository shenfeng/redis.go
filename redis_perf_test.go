package redis

import (
	// "log"
	"math/rand"
	"net"
	"runtime"
	"testing"
)

const (
	TestServer   = "localhost:6379"
	TESTBYTESIZE = 1024 * 15
	test_key     = "test_key"
)

var (
	rand_data []byte // test data for writing; same as data
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand_data = make([]byte, TESTBYTESIZE)
	for i := 0; i < TESTBYTESIZE; i++ {
		rand_data[i] = byte((i + 'a') % 255)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	buf := &ByteBuffer{buffer: make([]byte, 1024)}
	tmp := [][]byte{[]byte(myKey), []byte(test_key)}
	buf.encodeRequest("SET", tmp)
	b.SetBytes(int64(buf.pos))

	for i := 0; i < b.N; i++ {
		buf.encodeRequest("SET", tmp)
	}

	// c, _ := net.Dial("tcp", TestServer)
	// log.Println(c.Write(buf.buffer[:buf.pos + 1]))
	// buffer := make([]byte, TESTBYTESIZE)

	// n, _ := c.Read(buffer)
	// log.Println(string(buffer[:n+1]))

}

func BenchmarkGetConn(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c, _ := client.getCon()
		client.returnCon(c)
	}
}

func BenchmarkGetConnWithContention(b *testing.B) {
	go func(N int) { //  fake some contention
		for i := 0; i < N; i++ {
			c, _ := client.getCon()
			client.returnCon(c)
		}
	}(b.N)

	for i := 0; i < b.N; i++ {
		c, _ := client.getCon()
		client.returnCon(c)
	}
}

func BenchmarkPing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		client.Ping()
	}
}

func BenchmarkPipelinePing(b *testing.B) {
	var pipe *Pipeline
	pipe, _ = client.Pipeline()
	for i := 0; i < b.N; i++ {
		pipe.Ping()
		if rand.Intn(300) == 1 {
			pipe.Execute()
			pipe, _ = client.Pipeline()
		}
	}
	pipe.Execute()
}

// func BenchmarkBlockingPushPop(b *testing.B) {
// 	client.Del(myKey)
// 	go func(N int) {
// 		for i := 0; i < N*2; i++ {
// 			client.Rpush(myKey, myValue)
// 		}
// 	}(b.N)
// 	for i := 0; i < b.N; i++ {
// 		client.Blpop(myKey, 0)
// 	}
// }

func BenchmarkGetBytes(b *testing.B) {
	client.Set(myKey, rand_data)
	b.ResetTimer()
	b.SetBytes(TESTBYTESIZE)
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

func BenchmarkRawConnPing(b *testing.B) {
	c, err := net.Dial("tcp", TestServer)

	if err != nil {
		b.Error(err)
	} else {
		buffer := make([]byte, 1024)
		ping := []byte("*1\r\n$4\r\nPING\r\n")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Write(ping)
			c.Read(buffer)
		}
	}
	c.Close()
}

func BenchmarkRawRedisConnPing(b *testing.B) {
	c, err := net.Dial("tcp", TestServer)
	if err != nil {
		b.Error(err)
	} else {
		rb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		wb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		c := &RedisConn{conn: c, rbuf: rb, wbuf: wb}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.send("PING", false)
		}
		c.conn.Close()
	}
}

func BenchmarkRawRedisConnSet(b *testing.B) {
	c, err := net.Dial("tcp", TestServer)
	if err != nil {
		b.Error(err)
	} else {
		rb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		wb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		c := &RedisConn{conn: c, rbuf: rb, wbuf: wb}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.send("SET", false, []byte(test_key), rand_data)
		}
		c.conn.Close()
	}
}

func BenchmarkRawRedisConnGet(b *testing.B) {
	c, err := net.Dial("tcp", TestServer)
	if err != nil {
		b.Error(err)
	} else {
		rb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		wb := &ByteBuffer{buffer: make([]byte, BufferSize)}
		c := &RedisConn{conn: c, rbuf: rb, wbuf: wb}
		b.ResetTimer()
		b.SetBytes(TESTBYTESIZE)
		for i := 0; i < b.N; i++ {
			c.send("GET", false, []byte(test_key))
		}
		c.conn.Close()
	}
}
