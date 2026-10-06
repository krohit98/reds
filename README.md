# REDS — Real-time Event Delivery Service
### `Go` | `net/http` | `Server-Sent Events` | `Go concurrency primitives` | `In-memory queues` | `CLI`
REDS (Real-time Event Delivery Service) is a systems-oriented backend project built in Go. It is a publish/subscribe system built to explore backend and systems engineering concepts from the ground up.

The project is intentionally developed incrementally. It starts with a small in-memory event broker exposed through HTTP and a terminal client, and will progressively evolve toward real-time event streaming, concurrency, connection management, networking internals, and custom TCP communication.

The goal is not to recreate Kafka or another production message broker. Instead, REDS is a hands-on project for understanding the building blocks behind networked backend and event-delivery systems.

## Project Goals
REDS focuses on:
- Go backend development
- Publish/subscribe architecture
- HTTP and SSE-based event delivery
- Go concurrency and synchronization
- Connection lifecycle and cancellation
- Backpressure and slow consumers
- Networking and TCP internals
## Current Status
### Real-time Event Delivery

The current implementation provides an in-memory pub/sub service with real-time event delivery over Server-Sent Events (SSE).
The system currently supports:
- Creating subscriptions to topics
- Publishing messages to topics
- Per-subscriber message queues
- Per-subscriber delivery channels for active real-time consumers
- Unsubscribing from topics
- Real-time event streaming via SSE
- HTTP API communication
- A terminal-based client
- Multiple independent client processes communicating with the same REDS server process

Messages are currently simple strings and the broker stores all state in memory. There is no persistence, replication, replay, or distributed operations yet.
The next focus is making the delivery path more robust through concurrency handling, reliability, backpressure, testing, and performance measurement.

### Current Architecture
```text
                    ┌──────────────────────┐
                    │     REDS Server      │
                    │                      │
                    │     HTTP Server      │
                    │          │           │
                    │          ▼           │
                    │       Broker         │
                    │          │           │
                    │     ┌────┴────┐      │
                    │     ▼         ▼      │
                    │   news      events   │
                    │     │         │      │
                    │     ▼         ▼      │
                    │ Subscribers / Queues │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┼─────────────┐
                 │             │             │
                 ▼             ▼             ▼
              Client 1      Client 2      Client 3
```
The current broker uses a simple in-memory representation:
```text
Broker
  |
  ├── topic A
  │     ├── subscriber A
  │     │      └── id, queue, channel
  │     └── subscriber B
  │            └── id, queue, channel
  │
  └── topic B
        └── subscriber C
                └── id, queue, channel
```
At this stage, a subscriber is associated with a topic-specific subscription. A future version can separate subscriber identity from subscriptions so that one subscriber can subscribe to multiple topics.
## HTTP API
The current server exposes the following endpoints:
| Operation | Endpoint |
|---|---|
| Subscribe | `GET /subscribe/{topic}` |
| Unsubscribe | `GET /unsubscribe/{topic}/{subscriberId}` |
| Publish | `GET /publish/{topic}/{message}` |
| Retrieve messages | `GET /messages/{topic}/{subscriberId}` |

## Running REDS
You need Go installed on your system.

### 1. Start the REDS server
From the repository root:
```bash
cd server
go run main.go
```
The server listens on:
```text
http://localhost:8080
```
Keep this terminal running.
### 2. Start a client
Open another terminal:
```bash
cd client
go run main.go
```
You should see:
```text
Welcome to REDS
Type a command to get started
>>
```
Multiple client processes can be started in separate terminals. They all communicate with the same REDS server process.
### Subscribe to a topic
```text
>> subscribe/news
```
Example:
```text
Subscribed successfully! Subscriber ID: 9dzhhLQc
```
The returned subscriber ID is used for subsequent operations.
### Publish a message
```text
>> publish/news/gd stock price increased
```
Example:
```text
Successfully published to topic: news
```
Messages can contain spaces. `/` is currently used as the command delimiter.
### Retrieve queued messages
```text
>> messages/news/9dzhhLQc
```
Example:
```text
gd stock price increased, roberry incident
```
The messages endpoint now supports real-time streaming via SSE. The subscriber's delivery channel is used to stream newly published events to an active client connection.
### Unsubscribe
```text
>> unsubscribe/news/9dzhhLQc
```
Example:
```text
Successfully unsubscribed subscriber with id: 9dzhhLQc from topic: news
```
If the subscriber was the final subscriber for that topic, the current broker removes the topic from its in-memory topic registry.
### Help
```text
>> help
```
Displays the available commands.
## Multi-Terminal Example
A useful way to demonstrate the current system is to run the server and multiple clients simultaneously.
### T1 — REDS server
```bash
cd server
go run main.go
```
### T2 — Controller / publisher
```bash
cd client
go run main.go
```
Create subscriptions:
```text
>> subscribe/news
Subscribed successfully! Subscriber ID: 9dzhhLQc
>> subscribe/events
Subscribed successfully! Subscriber ID: FydyQtig
```
Publish messages:
```text
>> publish/news/gd stock price increased
Successfully published to topic: news
>> publish/news/roberry incident
Successfully published to topic: news
>> publish/events/hackathon
Successfully published to topic: events
>> publish/events/trade fair
Successfully published to topic: events
```
The broker now contains separate queues for the two topic subscriptions.
### T3 — News consumer
Start another client:
```bash
cd client
go run main.go
```
Retrieve the news messages:
```text
>> messages/news/9dzhhLQc
gd stock price increased, roberry incident
```
### T4 — Events consumer
Start another client:
```bash
cd client
go run main.go
```
Retrieve the event messages:
```text
>> messages/events/FydyQtig
hackathon, trade fair
```
This demonstrates that messages are routed to the appropriate topic/subscriber queue.
### Unsubscribe
From T2:
```text
>> unsubscribe/news/9dzhhLQc
Successfully unsubscribed subscriber with id: 9dzhhLQc from topic: news
```
After the final subscriber is removed, the topic is deleted from the current in-memory broker.
A subsequent request from T3:
```text
>> messages/news/9dzhhLQc
Error 404: Cannot fetch response: no match found for topic
```
## Real-time Event Delivery with SSE
REDS uses Server-Sent Events (SSE) to maintain long-lived connections with active subscribers.
```text
Publisher - publishes a message in a topic
    │
    ▼
 Broker - finds all subscribers for that topic
    │
    ├── Subscriber A ──► message added to queue/channel ──► SSE ──► Client A
    ├── Subscriber B ──► message added to queue/channel ──► SSE ──► Client B
    └── Subscriber C ──► message added to queue/channel ──► SSE ──► Client C
```
When a message is published, the broker delivers it to the channels associated with subscribers of that topic. An active SSE connection waits on its subscriber channel and streams incoming events to the client.

## Next Steps
The next focus is strengthening the current real-time delivery system:
- Concurrency-safe broker operations
- Reliability and connection cleanup
- Backpressure and slow-consumer handling
- Unit and integration testing
- Load testing and performance benchmarking
- Networking internals and raw TCP communication
- Persistence, delivery semantics, and fault tolerance