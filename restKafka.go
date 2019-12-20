package main

import (
		"fmt"
		"net/http"
		"encoding/json"
		"io/ioutil"

		log "github.com/sirupsen/logrus"
		"github.com/gorilla/mux"
		"github.com/confluentinc/confluent-kafka-go/kafka"
		)

type Job struct {
	Title string `json:"title"`
	Description string `json:"description"`
	Company string `json:"company"`
	Salary string `json:"salary"`
}

func main() {
	fmt.Println("App started on PORT 9090")

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/jobs", jobPostHandler).Methods("POST")
	log.Fatal(http.ListenAndServe(":9090", router))
}

func jobPostHandler(w http.ResponseWriter, r *http.Request) {

	// Get data from POST request
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Debug(err)
	}

	var _job Job
	err = json.Unmarshal(b, &_job)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Println(_job)

	// save to kafka cluster
	saveJobToKafka(_job)

	jobString , err := json.Marshal(_job)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Write(jobString)


}

func saveJobToKafka(job Job) {
	fmt.Println("saving to Kafka")

	jsonJob, err := json.Marshal(job)
	jobString := string(jsonJob)
	fmt.Println(jobString)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Println(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()


	// send to kafka topic
	topic := "jobs-1"
	for _, word := range []string{jobString} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	p.Flush(7 * 1000)
// 	curl -X POST localhost:9090/jobs -d '{"title":"abcd", "description":"desc", "company":"company", "salary":"salary"}'
// {"title":"abcd","description":"desc","company":"company","salary":"salary"}
}