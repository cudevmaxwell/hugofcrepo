package main

import (
	"bufio"
	"encoding/json"
	"gopkg.in/stomp.v1"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"fmt"
)

type JSONLD []struct {
	Httpfedorainfodefinitionsv4indexinghasIndexingTransformation []struct {
		Value string `json:"@value"`
	} `json:"http://fedora.info/definitions/v4/indexing#hasIndexingTransformation"`
	Httppurlorgdcelements11title []struct {
		Value string `json:"@value"`
	} `json:"http://purl.org/dc/elements/1.1/title"`
	Id   string   `json:"@id"`
	Type []string `json:"@type"`
}

func main() {
	conn, err := stomp.Dial("tcp", "localhost:61613", stomp.Options{})
	if err != nil {
		log.Fatal(err)
	}

	sub, err := conn.Subscribe("/topic/fedora", stomp.AckAuto)
	if err != nil {
		conn.Disconnect()
		log.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for {
		select {
		case <-c:
			sub.Unsubscribe()
			conn.Disconnect()
			log.Println("Exiting...")
			os.Exit(0)
		case msg := <-sub.C:
			node := msg.Header.Get("org.fcrepo.jms.identifier")
			log.Printf("%v\n", node)
			req, _ := http.NewRequest("GET", "http://localhost:8080/fcrepo/rest"+node, nil)
			req.Header.Set("Accept", "application/ld+json")
			req.Header.Set("Prefer", "return=minimal")
			client := &http.Client{}
			resp, _ := client.Do(req)
			var responseJSON JSONLD
			err = json.NewDecoder(resp.Body).Decode(&responseJSON)
			if err != nil {
				log.Println(err)
			}
			resp.Body.Close()
			f, _ := os.Create(strings.Replace(node, "/", "_", -1) + ".md")
			w := bufio.NewWriter(f)
			w.WriteString("+++\n+++\n")
			for _, x := range responseJSON {
				for _, y := range x.Httppurlorgdcelements11title {
					w.WriteString(fmt.Sprintf("%v\n\n", y.Value))
				}
			}
			w.Flush()
			f.Close()
		}
	}

}
