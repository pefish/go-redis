package main

import (
	"fmt"
	"log"

	i_logger "github.com/pefish/go-interface/i-logger"
	go_redis "github.com/pefish/go-redis"
)

func main() {
	err := do()
	if err != nil {
		log.Fatal(err)
	}
}

func do() error {
	instance := go_redis.NewRedisInstance(&i_logger.DefaultLogger)
	err := instance.Connect(&go_redis.Configuration{
		Url:      "127.0.0.1",
		Password: "password",
	})
	if err != nil {
		return err
	}

	key := "sdfgadfg"
	r, err := instance.List.ListAllUint64(key)
	if err != nil {
		return err
	}
	if r == nil {
		instance.List.LPushUint64(key, 111)
	}
	instance.List.SetUint64(key, 0, 222)
	r, err = instance.List.ListAllUint64(key)
	if err != nil {
		return err
	}
	fmt.Println(r)

	err = instance.List.SetUint64(key, 1, 333)
	if err != nil {
		return err
	}
	return nil
}
