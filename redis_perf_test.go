package redis

import (
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
