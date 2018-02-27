package main

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/luci/go-render/render"
)

var pool *redis.Pool

func init() {
	pool = &redis.Pool{
		MaxIdle:     16,
		MaxActive:   1024,
		IdleTimeout: 300,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
}

func main() {
	c := pool.Get()
	defer c.Close()

	_, err := c.Do("HMSET", "top10", "three", "heqikuang", "four", "xuefengfei")
	if err != nil {
		fmt.Println(err)
		return
	}

	//values, err := redis.Values(c.Do("HMGET", "top10", "three", "four"))
	values, err := redis.Values(c.Do("HGETALL", "top10"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("value : " + render.Render(values))

	var rankList []struct {
		Rank string
		Name string
	}

	//if _, err = redis.Scan(values, &name1, &name2); err != nil {
	if err = redis.ScanSlice(values, &rankList); err != nil {
		fmt.Println(err)
	}

	fmt.Println("nameList ", render.Render(rankList))
}
