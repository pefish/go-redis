package main

import (
	"fmt"
	"time"

	i_logger "github.com/pefish/go-interface/i-logger"
	go_redis "github.com/pefish/go-redis"
)

func main() {
	instance := go_redis.NewRedisInstance(&i_logger.DefaultLogger)
	instance.Connect(&go_redis.Configuration{
		Url:      "127.0.0.1",
		Password: "password",
	})

	go func() {
		for {
			time.Sleep(time.Second)
			instance.Publish("test", "1")
		}
	}()

	for {
		select {
		case msg := <-instance.Subscribe("test"):
			fmt.Println(msg.String())
		}
	}
}
