package g

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

var RedisConnPool *redis.Pool

func InitRedisConnPool() {
	addr := Config().Redis.Address
	maxIdle := Config().Redis.MaxIdle
	idleTimeout := 240 * time.Second

	connTimeout := time.Duration(Config().Redis.ConnTimeout) * time.Millisecond
	readTimeout := time.Duration(Config().Redis.ReadTimeout) * time.Millisecond
	writeTimeout := time.Duration(Config().Redis.WriteTimeout) * time.Millisecond

	RedisConnPool = &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: idleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", addr, connTimeout, readTimeout, writeTimeout)
			if err != nil {
				return nil, err
			}
			_, err = c.Do("SELECT", Config().Redis.RealFeedDBIndex)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: PingRedis,
	}
}

func PingRedis(c redis.Conn, t time.Time) error {
	_, err := c.Do("ping")
	if err != nil {
		log.Println("[ERROR] ping redis fail", err)
	}
	return err
}
