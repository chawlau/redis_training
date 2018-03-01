package main

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
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

	/*
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
	*/
	var p1, p2 struct {
		Title  string `redis:"title"`
		Author string `redis:"author"`
		Body   string `redis:"body"`
	}

	p1.Title = "Example"
	p1.Author = "Gary"
	p1.Body = "Hello"

	if _, err := c.Do("HMSET", redis.Args{}.Add("id1").AddFlat(&p1)...); err != nil {
		panic(err)
	}

	m := map[string]string{
		"title":  "Example2",
		"author": "Steve",
		"body":   "Map",
	}
	if _, err := c.Do("HMSET", redis.Args{}.Add("id2").AddFlat(m)...); err != nil {
		panic(err)
	}

	for _, id := range []string{"id1", "id2"} {

		v, err := redis.Values(c.Do("HGETALL", id))
		if err != nil {
			panic(err)
		}

		if err := redis.ScanStruct(v, &p2); err != nil {
			panic(err)
		}

		fmt.Printf("%+v\n", p2)
	}
}
