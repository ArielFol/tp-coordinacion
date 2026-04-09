package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/messagehandler"
)

type ConnectionConfig struct {
	host string
	port string
}

type ClientState struct {
	ClientConnection net.Conn
	MessageHandler   *messagehandler.MessageHandler
}

type SharedClientStates struct {
	Lock         sync.Mutex
	ClientStates []ClientState
}

var sharedClientStates = SharedClientStates{}

func loadConnectionConfig() (*ConnectionConfig, error) {
	connectionConfig := ConnectionConfig{
		host: os.Getenv("SERVER_HOST"),
		port: os.Getenv("SERVER_PORT"),
	}

	return &connectionConfig, nil
}

func handleSignals(open *bool, acceptConnection net.Listener) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	log.Println("SIGTERM signal received")
	*open = false
	acceptConnection.Close()
}

func main() {
	var open bool = true
	outputQueue, err := createQueue(os.Getenv("INPUT_QUEUE"))
	if err != nil {
		log.Println("While readding rabbitmq host and ports", err)
		return
	}
	go outputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		handleClientResponse(outputQueue, msg, ack, nack)
	})

	config, err := loadConnectionConfig()
	if err != nil {
		log.Println("Error when reading environment variables:", err)
		return
	}

	acceptConnection, err := net.Listen("tcp", config.host+":"+config.port)
	if err != nil {
		log.Println("Error when listening to connections:", err)
		return
	}
	defer acceptConnection.Close()
	go handleSignals(&open, acceptConnection)
	log.Println("Accepting connections...")

	for {
		clientConnection, err := acceptConnection.Accept()
		if err != nil {
			if !open {
				break
			}
			log.Println("While accepting connections: ", err)
			return
		}

		log.Println("Client connected...")

		sharedClientStates.Lock.Lock()
		messageHandler := messagehandler.NewMessageHandler()
		clientState := ClientState{ClientConnection: clientConnection, MessageHandler: &messageHandler}
		sharedClientStates.ClientStates = append(sharedClientStates.ClientStates, clientState)
		sharedClientStates.Lock.Unlock()

		go handleClientRequest(clientState)
	}

	outputQueue.StopConsuming()
	sharedClientStates.Lock.Lock()
	for _, clientState := range sharedClientStates.ClientStates {
		clientState.ClientConnection.Close()
	}
	sharedClientStates.Lock.Unlock()
}
