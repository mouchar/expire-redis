package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	var client *redis.Client

	verbosePtr := flag.Bool("verbose", false, "Verbose output")
	hostnamePtr := flag.String("hostname", "", "RedisHostname")
	portPtr := flag.String("port", "6379", "Redis Port")
	sentinelPtr := flag.Bool("sentinel", false, "Use Redis Sentinel")
	flag.Parse()

	if *sentinelPtr {
		fmt.Println("Using Redis Sentinel connection")
		// port = "26379"
		sentinels := []string{
			*hostnamePtr + ":" + *portPtr,
		}
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       "mymaster",
			SentinelAddrs:    sentinels,
			SentinelPassword: "",
			DB:               0,
		})
	} else {
		fmt.Println("Using direct Redis connection")
		client = redis.NewClient(&redis.Options{
			Addr:     *hostnamePtr + ":" + *portPtr,
			Password: "",
			DB:       0,
		})
	}

	if *verbosePtr {
		fmt.Printf("Verbose logging enabled\n")
	}

	counter := 0
	ttlSet := 0

	ctx := context.Background()
	for iter := client.Scan(ctx, 0, "*", 0).Iterator(); iter.Next(ctx); {
		key := iter.Val()
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			panic(err)
		}
		if ttl == -1 {
			err := client.Expire(ctx, key, 1*time.Hour).Err()
			if err != nil {
				panic(err)
			}
			ttlSet++
			if *verbosePtr {
				fmt.Printf("Key %s didn't have a TTL and was set.\n", key)
			}
		}
		counter++
		if counter%10000 == 0 {
			fmt.Printf("Processed %d keys so far...\n", counter)
		}
	}
	fmt.Printf("Processed %d keys. Set %d keys.\n", counter, ttlSet)
}
