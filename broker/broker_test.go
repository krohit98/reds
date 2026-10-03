package broker

import (
	"reflect"
	"testing"
)

func beforeTest() (*Broker, *Subscriber) {
	testSubscriber := Subscriber {
		Id: GenerateRandomId(),
		queue: []string{},
	}

	testBroker := Broker {
		topics: map[string][]*Subscriber {
			"Test_Topic_1": {&testSubscriber},
		},
	}

	return &testBroker, &testSubscriber
}

func TestSubscribeExistingTopic(t *testing.T) {

	testBroker,_ := beforeTest()

	newSubscriber, err := testBroker.Subscribe("Test_Topic_1")

	if err != nil {
		t.Fatalf("Got error subscribing to topic: %v", err)
	}

	if len(testBroker.topics["Test_Topic_1"]) != 2 {
		t.Fatalf("Expected %v subscribers, found %v", 2, len(testBroker.topics["Test_Topic_1"]))
	}

	topicQueueLength := len(testBroker.topics["Test_Topic_1"])
	if !reflect.DeepEqual(testBroker.topics["Test_Topic_1"][topicQueueLength-1], newSubscriber) {
		t.Fatalf("Subscriber is different than expected.\nExpected: %v\nFound: %v",
			testBroker.topics["Test_Topic_1"][topicQueueLength-1], newSubscriber)
	}

	if len(newSubscriber.queue) != 0 {
		t.Fatalf("Expected queue length for new subscriber to be %v, found %v", 0, len(newSubscriber.queue))
	}
}

func TestSubscribeNewTopic(t *testing.T) {

	testBroker,_ := beforeTest()

	newSubscriber, err := testBroker.Subscribe("Test_Topic_2")

	if err != nil {
		t.Fatalf("Got error subscribing to topic: %v", err)
	}

	if len(testBroker.topics["Test_Topic_2"]) != 1 {
		t.Fatalf("Expected %v subscribers, found %v", 1, len(testBroker.topics["Test_Topic_2"]))
	}

	topicQueueLength := len(testBroker.topics["Test_Topic_2"])
	if !reflect.DeepEqual(testBroker.topics["Test_Topic_2"][topicQueueLength-1], newSubscriber) {
		t.Fatalf("Subscriber is different than expected.\nExpected: %v\nFound: %v",
			testBroker.topics["Test_Topic_2"][topicQueueLength-1], newSubscriber)
	}

	if len(newSubscriber.queue) != 0 {
		t.Fatalf("Expected queue length for new subscriber to be %v, found %v", 0, len(newSubscriber.queue))
	}
}

func TestSubscribeMissingTopic(t *testing.T) {
	testBroker := Broker {
		topics: map[string][]*Subscriber {},
	}

	expectedError := ErrInvalidTopic

	newSubscriber, err := testBroker.Subscribe("")

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none.", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}

	if newSubscriber != nil {
		t.Fatalf("Expected newSubscriber to be nil, received: %v", newSubscriber)
	}

}

func TestUnsubscribeMultipleSubscribers(t *testing.T) {

	testBroker,_ := beforeTest()

	newSubscriber, err := testBroker.Subscribe("Test_Topic_1")
	_, err = testBroker.Subscribe("Test_Topic_1")

	if err != nil {
		t.Fatalf("Got error subscribing topic: %v", err.Error())
	}

	err = testBroker.Unsubscribe("Test_Topic_1", newSubscriber.Id)

	if err != nil {
		t.Fatalf("Got error unsubscribing topic: %v", err.Error())
	}

	existingSubscribers := testBroker.topics["Test_Topic_1"]

	if len(existingSubscribers) != 2 {
		t.Fatalf("Expected %v remaining subscribers after unsubscribe, found %v", 2, len(existingSubscribers))
	}

	for _, subscriber := range existingSubscribers {
		if subscriber.Id == newSubscriber.Id {
			t.Fatal("Failed to unsubscribe")
		}
	}
}

 func TestUnsubscribeSingleSubscriber(t *testing.T) {
	testBroker, testSubscriber := beforeTest()

	err := testBroker.Unsubscribe("Test_Topic_1", testSubscriber.Id)

	if err != nil {
		t.Fatalf("Got error unsubscribing topic: %v", err.Error())
	}

	existingSubscribers := testBroker.topics["Test_Topic_1"]

	if existingSubscribers != nil {
		t.Fatalf("Expected topic to be deleted from list, found %v subscribers", len(existingSubscribers))
	}
 }

func TestUnsubscribeMissingTopic(t *testing.T) {
	testBroker, testSubscriber := beforeTest()

	err := testBroker.Unsubscribe("", testSubscriber.Id)

	expectedError := ErrInvalidTopic

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestUnsubscribeMissingSubscriberId(t *testing.T) {
	testBroker, _ := beforeTest()

	err := testBroker.Unsubscribe("Test_Topic_1", "")

	expectedError := ErrInvalidSubscriberId

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestUnsubscribeTopicNotFound(t *testing.T) {
	testBroker, testSubscriber := beforeTest()

	err := testBroker.Unsubscribe("Test_Topic_2", testSubscriber.Id)

	expectedError := ErrTopicNotFound

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestUnsubscribeSubscriberNotFound(t *testing.T) {
	testBroker, _ := beforeTest()

	err := testBroker.Unsubscribe("Test_Topic_1", GenerateRandomId())

	expectedError := ErrSubscriberIdNotFound

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestPublishSingleSubscriber(t *testing.T) {
	testBroker, testSubscriber := beforeTest()
	testMessage := "test_message"

	err := testBroker.Publish("Test_Topic_1", testMessage)

	if err != nil {
		t.Fatalf("Got error publishing message: %v", err.Error())
	}

	if len(testSubscriber.queue) != 1 {
		t.Fatalf("Expected subscriber queue to be of length %v, found %v", 1, len(testSubscriber.queue))
	}

	if testSubscriber.queue[0] != testMessage {
		t.Fatalf("Error publishing message.\nExpected: %v\nReceived: %v", testMessage, testSubscriber.queue[0])
	}
}

func TestPublishMultipleSubscribers(t *testing.T) {
	testBroker, testSubscriber1 := beforeTest()
	testSubscriber2, err := testBroker.Subscribe("Test_Topic_1")
	testSubscriber3, err := testBroker.Subscribe("Test_Topic_1")

	if err != nil {
		t.Fatalf("Got error subscribing to topic: %v", err)
	}

	testMessage := "test_message"

	err = testBroker.Publish("Test_Topic_1", testMessage)

	if err != nil {
		t.Fatalf("Got error publishing message: %v", err.Error())
	}

	if len(testSubscriber1.queue) != 1 {
		t.Fatalf("Expected subscriber queue to be of length %v, found %v", 1, len(testSubscriber1.queue))
	}

	if testSubscriber1.queue[0] != testMessage {
		t.Fatalf("Error publishing message.\nExpected: %v\nReceived: %v", testMessage, testSubscriber1.queue[0])
	}

	if len(testSubscriber2.queue) != 1 {
		t.Fatalf("Expected subscriber queue to be of length %v, found %v", 1, len(testSubscriber2.queue))
	}

	if testSubscriber2.queue[0] != testMessage {
		t.Fatalf("Error publishing message.\nExpected: %v\nReceived: %v", testMessage, testSubscriber2.queue[0])
	}

	if len(testSubscriber3.queue) != 1 {
		t.Fatalf("Expected subscriber queue to be of length %v, found %v", 1, len(testSubscriber3.queue))
	}

	if testSubscriber3.queue[0] != testMessage {
		t.Fatalf("Error publishing message.\nExpected: %v\nReceived: %v", testMessage, testSubscriber3.queue[0])
	}
}

func TestPublishMissingTopic(t *testing.T) {
	testBroker, _ := beforeTest()
	testMessage := "test_message"

	err := testBroker.Publish("", testMessage)

	expectedError := ErrInvalidTopic

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestPublishMissingMessage(t *testing.T) {
	testBroker, _ := beforeTest()

	err := testBroker.Publish("Test_Topic_1", "")

	expectedError := ErrInvalidMessage

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestPublishTopicNotFound(t *testing.T) {
	testBroker, _ := beforeTest()
	testMessage := "test_message"

	err := testBroker.Publish("Test_Topic_2", testMessage)

	expectedError := ErrTopicNotFound

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}
}

func TestPublishSubscribersNotFound(t *testing.T) {
	testBroker := Broker {
		topics: map[string][]*Subscriber {
			"Test_Topic_1": {},
		},
	}

	testMessage := "test_message"

	err := testBroker.Publish("Test_Topic_1", testMessage)

	expectedError := ErrNoSubscribers

	if err == nil {
		t.Fatalf("Expected error \"%v\", received none", expectedError)
	}

	if err.Error() != expectedError.Error() {
		t.Fatalf("Expected error \"%v\", received \"%v\"", expectedError, err.Error())
	}

}