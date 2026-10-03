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

	for _,v := range subscribers {
		v.queue = append(v.queue, message)
	}

	return nil
}

func (b *Broker) GetSubscriberMessagesForTopic(topic string, subscriberId string) ([]string, error) {
	if len(topic) == 0 {
		return nil, ErrInvalidTopic
	}

	if len(subscriberId) == 0 {
		return nil, ErrInvalidSubscriberId
	}

	subscribers, ok := b.topics[topic]

	if(!ok) {
		return nil, ErrTopicNotFound
	}

	if len(subscribers) <= 0 {
		return nil, ErrNoSubscribers
	}


	for _,v := range subscribers {
		if v.Id == subscriberId {
			return v.queue, nil
		}
	}

	return nil, ErrSubscriberIdNotFound
}

func GenerateRandomId() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	result := make([]byte, 8)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}