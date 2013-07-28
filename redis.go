package redis

import (
	"strconv"
)

const (
	DefaultMaxCon = 5
	BufferSize    = 1024 * 24
)

type RedisError string

func (err RedisError) Error() string { return "Redis Error: " + string(err) }

var KeyDoesNotExist = RedisError("Key does not exist")

// var hsetKey = RedisError("Key does not exist")

func (client *Client) Hmset(key string, mapping map[string]interface{}) error {
	c, err := client.getCon()
	defer client.returnCon(c)

	var args [][]byte
	args = append(args, []byte(key))
	// err := mappingToArgs(reflect.ValueOf(mapping), &args)

	if err != nil {
		return err
	}

	_, err = c.sendCommand("HMSET", args...)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) Hgetall(key string) (m map[string]string, err error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	rets, err := c.sendCommand("HGETALL", []byte(key))
	if err != nil {
		return m, err
	}

	for i := 0; i < len(rets.([]interface{})); i += 2 {
		// m[rets
	}
	return m, err
}

func (client *Client) Setnx(key string, data interface{}) (bool, error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	v, err := c.sendCommand("SETNX", []byte(key), toBytes(data))
	if err != nil {
		return false, err
	}
	return v.(int) == 1, nil
}

func (client *Client) Get(key string) ([]byte, error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	value, err := c.sendCommand("GET", []byte(key))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, KeyDoesNotExist
	}
	val := copyBytes(value.([]byte))
	return val, err
}

func (client *Client) MGetString(keys ...string) ([]string, error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	ks := make([][]byte, len(keys))
	for i, v := range keys {
		ks[i] = []byte(v)
	}
	values, err := c.sendCommand("MGET", ks...)
	if err != nil {
		return nil, err
	}
	rets := make([]string, len(values.([]interface{})))
	for i, v := range values.([]interface{}) {
		if v != nil {
			rets[i] = string(v.([]byte))
		} else {
			rets[i] = ""
		}
	}

	return rets, err
}

func (client *Client) MGet(keys ...string) ([][]byte, error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	ks := make([][]byte, len(keys))
	for i, v := range keys {
		ks[i] = []byte(v)
	}
	values, err := c.sendCommand("MGET", ks...)
	if err != nil {
		return nil, err
	}
	rets := make([][]byte, len(values.([]interface{})))
	for i, v := range values.([]interface{}) {
		if v != nil {
			rets[i] = copyBytes(v.([]byte))
		} else {
			rets[i] = nil
		}
	}

	return rets, err
}

func (client *Client) Brpop(lists interface{}, seconds int) ([]byte, string, error) {
	return client.blockPop("BRPOP", lists, seconds)
}

func (client *Client) Blpop(lists interface{}, seconds int) ([]byte, string, error) {
	return client.blockPop("BLPOP", lists, seconds)
}

func (client *Client) Lpush(key string, values interface{}) (int, error) {
	return client.listPush("LPUSH", key, values)
}

func (client *Client) Rpush(key string, values interface{}) (int, error) {
	return client.listPush("RPUSH", key, values)
}

func (client *Client) GetString(key string) (string, error) {
	c, err := client.getCon()
	defer client.returnCon(c)

	value, err := c.sendCommand("GET", []byte(key))
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", KeyDoesNotExist
	}
	return string(value.([]byte)), err
}

func (client *Client) Ping() error {
	return client.simple("PING")
}

func (client *Client) Select(db int) error {
	return client.simple("SELECT", []byte(strconv.Itoa(db)))
}

func (client *Client) Setex(key string, seconds int, data interface{}) error {
	return client.simple("SETEX", []byte(key), []byte(strconv.Itoa(seconds)), toBytes(data))
}

func (client *Client) Set(key string, data interface{}) error {
	return client.simple("SET", []byte(key), toBytes(data))
}

func (client *Client) Del(key string) error {
	return client.simple("Del", []byte(key))
}
