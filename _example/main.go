package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	i_logger "github.com/pefish/go-interface/i-logger"
	go_redis "github.com/pefish/go-redis"
)

func main() {
	envMap, _ := godotenv.Read("./.env")
	for k, v := range envMap {
		os.Setenv(k, v)
	}

	err := do()
	if err != nil {
		log.Fatal(err)
	}
}

func do() error {
	instance := go_redis.NewRedisInstance(&i_logger.DefaultLogger)
	err := instance.Connect(&go_redis.Configuration{
		Url:      os.Getenv("REDIS_URL"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	if err != nil {
		return err
	}

	r, err := instance.String.Get("test")
	if err != nil {
		return err
	}
	fmt.Println(r)
	return nil
}
