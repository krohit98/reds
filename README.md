# REDS — Real-time Event Delivery Service

REDS (Real-time Event Delivery Service) is a Go-based publish/subscribe system built to explore backend and systems engineering concepts from the ground up.

The project is intentionally developed incrementally. It starts with a small in-memory event broker exposed through HTTP and a terminal client, and will progressively evolve toward real-time event streaming, concurrency, connection management, networking internals, and custom TCP communication.

The goal is not to recreate Kafka or another production message broker. Instead, REDS is a hands-on project for understanding the building blocks behind networked backend and event-delivery systems.

## Project Goals

REDS is being developed to explore:

- Go backend development
- HTTP server and client communication
- Publish/subscribe architecture
- Topic and subscriber management
- In-memory message queues
- Real-time event delivery with Server-Sent Events (SSE)
- Go concurrency and synchronization
- Connection lifecycle and cancellation
- Backpressure and slow consumers
- Graceful shutdown
- HTTP/1.1 and networking internals
- Raw TCP communication
- Custom application protocols
- Backend and systems-oriented design

## Current Status

### Phase 1 — In-memory HTTP Pub/Sub

**Current phase**

The current implementation provides a simple single-node, in-memory pub/sub service.

The system currently supports:

- Creating subscriptions to topics
- Publishing messages to topics
- Per-subscriber message queues
- Unsubscribing from topics
- Retrieving queued messages
- HTTP API communication
- A terminal-based client
- Multiple independent client processes communicating with the same REDS server

Messages are currently simple strings and the broker stores all state in memory. There is no persistence, replication, replay, or distributed operation yet.

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
  └── topics
        ├── news
        │     ├── subscriber
        │     │      └── queue
        │     └── subscriber
        │            └── queue
        │
        └── events
              └── subscriber
                     └── queue
```

At this stage, a subscriber is associated with a topic-specific subscription. A future version can separate subscriber identity from subscriptions so that one subscriber can subscribe to multiple topics.

## Repository Structure

The project is currently split into the server, broker, and terminal client:

```text
REDS/
├── broker/
│   ├── broker.go
│   ├── errors.go
│   └── broker_test.go
│
├── server/
│   └── main.go
│
└── client/
    └── main.go
```

The exact files may evolve as the project progresses.

### Broker

Contains the core pub/sub domain logic:

- Topic management
- Subscriber management
- Message publishing
- Per-subscriber queues
- Broker-level errors

The broker does not depend on HTTP. This keeps the core messaging logic independent of the transport layer.

### Server

Provides the HTTP interface to the broker.

### Client

Provides a simple interactive terminal interface for communicating with the REDS server over HTTP.

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

## Current CLI Commands

The current client uses `/` as the command separator.

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

The current messages endpoint reads the subscriber's queued messages without implementing real-time streaming yet.

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

## HTTP API

The current server exposes the following endpoints:

| Operation | Endpoint |
|---|---|
| Subscribe | `GET /subscribe/{topic}` |
| Unsubscribe | `GET /unsubscribe/{topic}/{subscriberId}` |
| Publish | `GET /publish/{topic}/{message}` |
| Retrieve messages | `GET /messages/{topic}/{subscriberId}` |

These endpoints are intentionally simple in the current phase. The API will evolve as the project moves toward a more complete event-delivery model.

## Error Handling

The broker defines domain-level errors independently of HTTP.

The HTTP server maps those errors to appropriate HTTP responses.

For example:

```text
Invalid input       → 400 Bad Request
Missing topic       → 404 Not Found
Missing subscriber  → 404 Not Found
Unexpected failure  → 500 Internal Server Error
```

This separation allows the broker to remain independent of HTTP and makes it possible to introduce other transports later.

## Phase 2 — Real-time Event Delivery

The next major step is to introduce Server-Sent Events (SSE).

The current message retrieval model is request/response based:

```text
Client
  │
  │ GET /messages/...
  ▼
Server
  │
  │ current queued messages
  ▼
Client
  │
  └── connection closes
```

SSE will change this into a long-lived connection:

```text
Client
  │
  │ GET /events/...
  ▼
Server
  │
  │ connection remains open
  │
  ├── event ───────► Client
  ├── event ───────► Client
  ├── event ───────► Client
  │
  └── ...
```

This will allow a subscriber to receive events as they are published instead of repeatedly polling the messages endpoint.

The SSE phase will also introduce practical Go concurrency problems, including:

- Handling long-lived connections
- Concurrent event delivery
- Goroutines
- Channels
- Request cancellation
- Client disconnects
- Connection cleanup
- Slow consumers and backpressure

## Future Development

The project will evolve through several stages.

### Phase 2 — SSE and Concurrent Delivery

- Add an SSE event endpoint
- Stream messages to connected subscribers
- Introduce concurrent event handling
- Handle client disconnects
- Add cancellation and connection cleanup
- Explore backpressure and slow consumers

### Phase 3 — API and Domain Model Improvements

- Introduce JSON request/response schemas
- Improve API semantics and HTTP methods
- Separate subscriber identity from topic subscriptions
- Introduce a dedicated subscription model
- Improve message/event representation
- Add stronger validation and tests

### Phase 4 — Networking Internals

- Explore `net/http` internals
- Build a raw TCP server using `net.Conn`
- Understand HTTP/1.1 at the byte-stream level
- Parse HTTP requests manually
- Construct HTTP responses manually
- Explore framing and connection management

### Phase 5 — Custom Transport

- Introduce a custom TCP interface for REDS
- Explore application-level framing and protocols
- Compare the custom transport with HTTP
- Investigate the implications of persistent connections and concurrent clients

### Longer-term Systems Exploration

Potential future areas include:

- Durable message storage
- Delivery semantics
- Message acknowledgements
- Persistence and recovery
- Metrics and observability
- Load testing and benchmarking
- Consumer backpressure
- Concurrency-safe data structures
- Horizontal scaling
- Replication and fault tolerance

These are future exploration areas rather than capabilities of the current implementation.

## Design Philosophy

REDS is intentionally built from simple primitives instead of immediately introducing external messaging frameworks.

The project follows a progression:

```text
Simple working system
        ↓
Understand the implementation
        ↓
Identify the next limitation
        ↓
Introduce the concept that solves it
        ↓
Measure / test the behavior
        ↓
Increase system complexity
```

This keeps each stage understandable while gradually moving the project from a simple Go backend toward a systems-oriented networking project.

## Non-Goals

REDS is not intended to be a production replacement for established messaging systems such as Kafka, RabbitMQ, or NATS.

The purpose is to understand the engineering concepts behind these kinds of systems by implementing a smaller system incrementally.

## Technology

- Go
- `net/http`
- Go concurrency primitives
- Server-Sent Events
- TCP / networking fundamentals
- In-memory data structures
- CLI / terminal applications
