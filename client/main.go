package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var httpClient *http.Client
var sseClient *http.Client
var baseURL string = "http://localhost:8080"

func main() {

	httpClient = &http.Client {
		Timeout: 10 * time.Second,
	}

	sseClient = &http.Client{}

	fmt.Println("Welcome to REDS")
	fmt.Println("Type a command to get started")
	fmt.Print(">> ")

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		
		input := scanner.Text()

		processInput(input)
		fmt.Print(">> ")
	}

	err := scanner.Err()
	if err != nil {
		fmt.Println("Error: ", err)
	}
}

func processInput(input string) {
	inputSplit := strings.Split(input, "/")

	switch inputSplit[0] {
	case "subscribe":
		handleSubscribe(inputSplit)
	case "unsubscribe":
		handleUnsubscribe(inputSplit)
	case "publish":
		handlePublish(inputSplit)
	case "messages":
		handleGetMessages(inputSplit)
	case "help":
		logHelp()
	default:
		fmt.Println(`Invalid command, type "help" to view list of valid commands`)
	}
}

func handleSubscribe(inputArray []string) {
	if len(inputArray) != 2 {
		fmt.Println("Invalid number of arguments in command")
		return
	}

	topic := inputArray[1]

	if(topic == "") {
		fmt.Println("Invalid topic")
		return
	}

	url := fmt.Sprintf("%v/subscribe/%v", baseURL, topic)

	response := request(http.MethodGet, url)

	if response != "" {
		fmt.Println(response)
	}
}

func handleUnsubscribe(inputArray []string) {
	if len(inputArray) != 3 {
		fmt.Println("Invalid number of arguments in command")
		return
	}

	topic := inputArray[1]

	if(topic == "") {
		fmt.Println("Invalid topic")
		return
	}

	subscriberId := inputArray[2]

	if(subscriberId == "") {
		fmt.Println("Invalid subscriber id")
		return
	}

	url := fmt.Sprintf("%v/unsubscribe/%v/%v", baseURL, topic, subscriberId)

	response := request(http.MethodGet, url)

	if response != "" {
		fmt.Println(response)
	}
}

func handlePublish(inputArray []string) {
	if len(inputArray) != 3 {
		fmt.Println("Invalid number of arguments in command")
		return
	}

	topic := inputArray[1]

	if(topic == "") {
		fmt.Println("Invalid topic")
		return
	}

	message := inputArray[2]

	if(message == "") {
		fmt.Println("Invalid message")
		return
	}

	url := fmt.Sprintf("%v/publish/%v/%v", baseURL, topic, message)

	response := request(http.MethodGet, url)

	if response != "" {
		fmt.Println(response)
	}
}

func handleGetMessages(inputArray []string) {
	if len(inputArray) != 3 {
		fmt.Println("Invalid number of arguments in command")
		return
	}

	topic := inputArray[1]

	if(topic == "") {
		fmt.Println("Invalid topic")
		return
	}

	subscriberId := inputArray[2]

	if(subscriberId == "") {
		fmt.Println("Invalid subscriber id")
		return
	}

	url := fmt.Sprintf("%v/messages/%v/%v", baseURL, topic, subscriberId)

	sseRequest(http.MethodGet, url)
}

func request(method string, url string) string {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		fmt.Printf("Error: Cannot create request: %v", err)
		return ""
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Printf("Error: Cannot fetch response: %v", err)
		return ""
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error: Cannot read response: %v", err)
		return ""
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error %v: Cannot fetch response: %v", resp.StatusCode, string(responseBody))
		return ""
	}

	return string(responseBody)
}

func sseRequest(method string, url string) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		fmt.Printf("Error: Cannot create request: %v", err)
		return
	}

	resp, err := sseClient.Do(req)
	if err != nil {
		fmt.Printf("Error: Cannot fetch response: %v", err)
		return
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)

	for scanner.Scan() {
		respBody := scanner.Text()
		fmt.Println(respBody)
	}

	err = scanner.Err()
	if err != nil {
		fmt.Printf("Error: Cannot read streamed events: %v", err)
	}
}

func logHelp() {
	fmt.Println("Valid commands:\n1. subscribe/<topic-name>\n2. unsubscribe/<topic-name>/<subscriber-id>\n3. publish/<topic-name>/<message>\n4. messages/<topic-name>/<subscriber-id>")
}