package redis

import (
	"net"
	"testing"
)

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

func BenchmarkRawUpperLimit(b *testing.B) {
	c, err := net.Dial("tcp", "localhost:6379")

	if err != nil {
		b.Error(err)
	} else {
		buffer := make([]byte, 1024)
		ping := []byte("*1\r\n$4\r\nPING\r\n")

		for i := 0; i < b.N; i++ {
			c.Write(ping)
			c.Read(buffer)
		}
	}
	c.Close()
}
