package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	redsBroker "reds/broker"
	"strings"
)

func main() {

	broker := redsBroker.NewBroker()

	http.HandleFunc("/subscribe/{topic}", handleSubscribe(broker))
	http.HandleFunc("/unsubscribe/{topic}/{subscriberId}", handleUnsubscribe(broker))
	http.HandleFunc("/publish/{topic}/{message}", handlePublish(broker))
	http.HandleFunc("/messages/{topic}/{subscriberId}", handleGetMessages(broker))

	fmt.Println("Starting server at port 8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}

func handleSubscribe(broker *redsBroker.Broker) http.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")

		if len(topic) <= 0 {
			http.Error(w, "No topic provided to subscribe", http.StatusBadRequest)
			return
		}

		subscriber, err := broker.Subscribe(topic)
		if err != nil {
			slog.Error("ERROR:handleSubscribe: "+err.Error())
			errorHandler(w, err)
			return
		}

		successMessage := fmt.Sprintf("Subscribed successfully! Subscriber ID: %v", subscriber.Id)
		_, err = w.Write([]byte(successMessage))
	}
}

func handleUnsubscribe(broker *redsBroker.Broker) http.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		subscriberId := r.PathValue("subscriberId")

		if len(topic) <= 0 {
			http.Error(w, "No topic provided to unsubscribe", http.StatusBadRequest)
			return
		}
		if len(subscriberId) <= 0 {
			http.Error(w, "No subscriber id provided to unsubscribe", http.StatusBadRequest)
			return
		}

		err := broker.Unsubscribe(topic, subscriberId)
		if err != nil {
			slog.Error("ERROR:handleUnsubscribe:"+err.Error())
			errorHandler(w, err)
			return
		}

		successMessage := fmt.Sprintf("Successfully unsubscribed subscriber with id: %v from topic: %v", subscriberId, topic)
		_, err = w.Write([]byte(successMessage))
	}
}

func handlePublish(broker *redsBroker.Broker) http.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		message := r.PathValue("message")

		if len(topic) <= 0 {
			http.Error(w, "No topic provided to publish", http.StatusBadRequest)
			return
		}
		if len(message) <= 0 {
			http.Error(w, "No message provided to publish", http.StatusBadRequest)
			return
		}

		err := broker.Publish(topic, message)
		if err != nil {
			slog.Error("ERROR:handlePublish:"+err.Error())
			errorHandler(w, err)
			return
		}

		successMessage := fmt.Sprintf("Successfully published to topic: %v", topic)
		_, err = w.Write([]byte(successMessage))
	}
}

func handleGetMessages(broker *redsBroker.Broker) http.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		subscriberId := r.PathValue("subscriberId")

		if len(topic) <= 0 {
			http.Error(w, "No topic provided to fetch messages", http.StatusBadRequest)
			return
		}
		if len(subscriberId) <= 0 {
			http.Error(w, "No subscriber id provided to fetch messages", http.StatusBadRequest)
			return
		}

		queue, ch, err := broker.GetSubscriberMessagesForTopic(topic, subscriberId)
		if err != nil {
			slog.Error("ERROR:handleGetMessages:"+err.Error())
			errorHandler(w, err)
			return
		}

		notifySubscribers(w, r, queue, ch)
	}
}

func notifySubscribers(
	w http.ResponseWriter, 
	r *http.Request, 
	queue []string,
	ch *chan string,
) {
	clientDisconnection := r.Context().Done()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	rc := http.NewResponseController(w)

	if len(queue) > 0 {
		fmt.Fprintf(w, "data: %s\n\n", strings.Join(queue, ", "))

		err := rc.Flush()
		if err != nil {
			fmt.Println("Error flushing data: ", err)
			return
		}
	}

	for {
		select {
		case <-clientDisconnection:
			fmt.Println("Client Disconnected")
			redsBroker.DiconnectChannel(ch)
			return
		case message := <-*ch:

			fmt.Fprintf(w, "data: %s\n\n", message)

			err := rc.Flush()
			if err != nil {
				fmt.Println("Error flushing data: ", err)
				return
			}
		}
	}
}

func errorHandler(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, redsBroker.ErrInvalidTopic):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, redsBroker.ErrInvalidSubscriberId):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, redsBroker.ErrInvalidMessage):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, redsBroker.ErrTopicNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, redsBroker.ErrSubscriberIdNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, redsBroker.ErrNoSubscribers):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}