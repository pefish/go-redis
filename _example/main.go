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

	key := "sdfgadfg11"
	// instance.Hash.SetUint64(key, "111", 111)
	r, err := instance.Hash.GetUint64(key, "111")
	if err != nil {
		return err
	}
	fmt.Println(r)
	return nil
}
