package broker

import (
	"errors"
	"log/slog"
	"math/rand"
)

var (
	ErrInvalidTopic = errors.New("topic missing/invalid")
	ErrInvalidSubscriberId = errors.New("subscriber id missing/invalid")
	ErrInvalidMessage = errors.New("message missing/invalid")
	ErrTopicNotFound = errors.New("no match found for topic")
	ErrSubscriberIdNotFound = errors.New("no match found for subscriber id")
	ErrNoSubscribers = errors.New("no subscribers found for topic")
)

type Subscriber struct {
	Id string
	queue []string
	ch chan string
}

type Broker struct {
	topics map[string][]*Subscriber
}

func NewBroker() *Broker {
	return &Broker{
		topics: map[string][]*Subscriber{},
	}
}

func (b *Broker) Subscribe(topic string) (*Subscriber, error) {

	if len(topic) == 0 {
		return nil, ErrInvalidTopic
	}
	
	_, ok := b.topics[topic]

	if !ok {
		slog.Info("Creating new topic", "topic", topic)
		b.topics[topic] = make([]*Subscriber, 0)
	}

	subscriber := Subscriber {
		Id: GenerateRandomId(),
		queue: make([]string, 0),
		ch: nil,
	}

	b.topics[topic] = append(b.topics[topic], &subscriber)

	return &subscriber, nil
}

func (b *Broker) Unsubscribe(topic string, subscriberId string) error {

	if len(topic) == 0 {
		return ErrInvalidTopic
	}

	if len(subscriberId) == 0 {
		return ErrInvalidSubscriberId
	}

	subscribers, ok := b.topics[topic]

	if(!ok) {
		return ErrTopicNotFound
	}

	subscriberFound := false
	for i,v := range subscribers {
		if v.Id == subscriberId {
			subscriberFound = true
			if len(subscribers) > 1 {
				subscribers[i] = subscribers[len(subscribers)-1]
				b.topics[topic] = subscribers[:len(subscribers)-1]
			} else {
				delete(b.topics, topic)
			}
			break
		}
	}

	if !subscriberFound {
		return ErrSubscriberIdNotFound
	}

	return nil
}

func (b *Broker) Publish(topic string, message string) error {

	if len(topic) == 0 {
		return ErrInvalidTopic
	}

	if len(message) == 0 {
		return ErrInvalidMessage
	}

	subscribers, ok := b.topics[topic]

	if !ok {
		return ErrTopicNotFound
	}

	if len(subscribers) <= 0 {
		return ErrNoSubscribers
	}

	for _,subscriber := range subscribers {
		if subscriber.ch != nil {
			subscriber.ch <- message
		} else {
			subscriber.queue = append(subscriber.queue, message)
		}
	}

	return nil
}

func (b *Broker) GetSubscriberMessagesForTopic(topic string, subscriberId string) ([]string, *chan string, error) {
	if len(topic) == 0 {
		return nil, nil, ErrInvalidTopic
	}

	if len(subscriberId) == 0 {
		return nil, nil, ErrInvalidSubscriberId
	}

	subscribers, ok := b.topics[topic]

	if(!ok) {
		return nil, nil, ErrTopicNotFound
	}

	if len(subscribers) <= 0 {
		return nil, nil, ErrNoSubscribers
	}


	for _,subscriber := range subscribers {
		if subscriber.Id == subscriberId {
			subscriber.ch = make(chan string)
			messageArray := subscriber.queue
			subscriber.queue = subscriber.queue[:0]
			return messageArray, &subscriber.ch, nil
		}
	}

	return nil, nil, ErrSubscriberIdNotFound
}

func DiconnectChannel(ch *chan string) {
	*ch = nil
}

func GenerateRandomId() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, 8)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}