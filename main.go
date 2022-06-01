package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	amqp "github.com/streadway/amqp"
)

//channel used to detect the connection close
var notifyClose = make(chan *amqp.Error)

//maxParallelism returns the maximum number of goroutines can be spawned.
func maxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

// newConsumer is a function create a rabbitMQ consumer
func newConsumer(connectionString string, fetchCount int) (*amqp.Connection, *amqp.Channel, func()) {
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("Error during amqp connection:", err)
	}
	channel, _ := connection.Channel()
	channel.Qos(fetchCount, 0, false)
	channel.NotifyClose(notifyClose)

	return connection, channel, func() {
		channel.Close()
		connection.Close()
	}
}

//worker creates the consumer and process the messages
func worker(channel *amqp.Channel, queueName, consumerName string, wg *sync.WaitGroup) {
	defer wg.Done()
	msgs, err := channel.Consume(
		queueName,    // queue
		consumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	if err != nil {
		log.Fatal("Error during channel connection:", err)
	}

	for m := range msgs {
		body := string(m.Body)
		log.Printf("Processing data %+v\n", body)
		log.Printf("Processing data %+v done\n", body)
		time.Sleep(5 * time.Second)
		m.Ack(false)
		log.Printf("Data %+v acked\n", body)
	}

}

func main() {
	var wg sync.WaitGroup
	url := "amqp://user:password@localhost:5672/"
	queue := "my_queue"
	name := "consumer"
	fetchSize := 10
	maxWorkers := maxParallelism()
	log.Printf("Connecting to %s queue %s fetch-size %d\n", url, queue, fetchSize)
	_, channel, closeFn := newConsumer(url, fetchSize)
	defer closeFn()
	defer log.Println("Closing qeueu channel and connection")
	defer log.Println("Program stopped successfu")
	log.Printf("Consumer %s is subscribing queue %s\n", name, queue)
	//Launching workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		i := i
		go worker(channel, queue, fmt.Sprintf("consumer-%d", i), &wg)
	}
	//Signal to detect the interruption
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	//Wait for signals
	select {
	case <-notifyClose:
		log.Println("rabbitmq server connection closed")
		closeWorkers(channel)
	case <-exit:
		log.Println("Got exit signal")
		closeWorkers(channel)
	}
	log.Println("Wait for worker procrss recieved message")
	wg.Wait() //wait for graceful shutdown
	log.Println("All workers closed successfully")
}

//closeWorkers closes all the consumers
func closeWorkers(channel *amqp.Channel) {
	maxWorkers := maxParallelism()
	for i := 0; i < maxWorkers; i++ {
		channel.Cancel(fmt.Sprintf("consumer-%d", i), false)
	}
}
