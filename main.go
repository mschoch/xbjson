package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"text/template"
	"time"
)

var cacheDuration = flag.Duration("cacheDuration", 5*time.Minute, "expire device revs after this duration")
var urlTepmlateString = flag.String("url", "http://localhost:4793/{{.d}}?rev={{.n}}", "url to post message to")
var removeDevice = flag.Bool("removeDevice", true, "remove device from message")
var removeRev = flag.Bool("removeRev", true, "remove rev from message")
var addTimestamp = flag.Bool("addTimestamp", true, "add timestamp received to message")

func main() {

	flag.Parse()

	urlTemplate, err := template.New("url").Parse(*urlTepmlateString)
	if err != nil {
		log.Fatalf("error parsing url template: %v", err)
	}

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

		rev, ok := jsonData["n"].(float64)
		if !ok {
			log.Printf("Message does not contain rev")
			continue
		}

		ok = cache.CheckAndUpdate(device, int(rev))
		if ok {
			buffer := new(bytes.Buffer)
			urlTemplate.Execute(buffer, jsonData)
			if *removeDevice {
				delete(jsonData, "d")
			}
			if *removeRev {
				delete(jsonData, "n")
			}
			if *addTimestamp {
				jsonData["ts"] = time.Now().Format(time.RFC3339)
			}

			jsonOut, err := json.Marshal(jsonData)
			if err != nil {
				log.Printf("error marshaling JSON: %v", err)
				continue
			}
			jsonBuffer := bytes.NewBuffer(jsonOut)
			resp, err := http.Post(buffer.String(), "application/json", jsonBuffer)
			if err != nil {
				log.Printf("error http post: %v", err)
				continue
			}
			log.Printf("sent message: %s, to %s, got %s", jsonOut, buffer.String(), resp.Status)
		}

	}
}
