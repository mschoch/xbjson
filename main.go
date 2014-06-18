package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"time"
)

var cacheDuration = flag.Duration("cacheDuration", 5*time.Minute, "expire device revs after this duration")

func main() {

	flag.Parse()

	cache := NewTimeRevCache(*cacheDuration)

	if flag.NArg() < 1 {
		log.Fatalf("specify path to input source")
	}

	file, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	fileReader := bufio.NewReader(file)
	for {
		line, err := fileReader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		} else if err != nil {
			log.Printf("End of file.")
			break
		}

		var jsonData map[string]interface{}
		err = json.Unmarshal(line, &jsonData)
		if err != nil {
			log.Printf("Error parsing JSON object: %v", err)
			continue
		}

		device, ok := jsonData["d"].(string)
		if !ok {
			log.Printf("Message does not contain device ID")
			continue
		}

		rev, ok := jsonData["n"].(int)
		if !ok {
			log.Printf("Message does not contain rev")
			continue
		}

		ok = cache.CheckAndUpdate(device, rev)
		if ok {
			log.Printf("new message from %s at %d", device, rev)
		} else {
			log.Printf("duplicate message from %s at %d", device, rev)
		}
	}
}
