package alpaca

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

type Cache struct {
	pool *redis.Pool
}

var ErrNil = redis.ErrNil

func InitPool(server string) *Cache {
	return &Cache{
		pool: &redis.Pool{
			MaxIdle:   20,
			MaxActive: 20,
			Dial: func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", server,
					redis.DialReadTimeout(time.Second*10),
					redis.DialConnectTimeout(time.Second*30),
					// redis.DialPassword(pass),
					// redis.DialDatabase(database),
				)
				if err != nil {
					return nil, err
				}
				return conn, err
			},
		},
	}
}

func (r *Cache) SetInt64(key string, value int64) error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, value)
	return err
}

func (r *Cache) GetInt64(key string) (int64, error) {
	conn := r.pool.Get()
	defer conn.Close()
	v, err := redis.Int64(conn.Do("Get", key))
	return v, err
}
func (r *Cache) SAdd(key string, value string) error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("sadd", key, value)
	return err
}
func (r *Cache) Srem(key string, value string) error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("srem", key, value)
	return err
}
func (r *Cache) SGet(key string) ([]interface{}, error) {
	conn := r.pool.Get()
	defer conn.Close()
	values, err := redis.Values(conn.Do("smembers", key))
	if err != nil {
		return nil, err
	}
	return values, nil
}
func (r *Cache) Exists(key string) (bool, error) {
	conn := r.pool.Get()
	defer conn.Close()
	is, err := redis.Bool(conn.Do("EXISTS", key))
	return is, err
}
