# Consumer Groups

Publishing messages is often straightforward: There's a message and a topic to which it's published.
Deciding who should receive the message is more complex.

So far, we've dealt with a single subscriber to a topic.
Redis Streams make this very easy because you can pass the topic name and receive any future messages.
However, this is far from a production-ready setup.

We'll look at more advanced concepts in the following modules. For now, consider two limitations:

1. We may want to run a second replica of the same application. If we do, both instances will receive the same messages, so they will be processed twice.
2. If our service goes down, it will lose all messages sent before it comes back up.
   This doesn't need to be a serious outage, either: A simple restart after deploying a new version is enough to cause this.

A *consumer group* is a concept intended to deal with these issues and decide which subscribers should receive which messages.

Even though most Pub/Subs have some way to achieve this, not all call it a consumer group.
Some brokers use a different model, like creating a *subscription* or *queue*, but the core idea is the same.

In the case of Redis Streams, a consumer group is a string assigned to the subscriber.
All subscribers using the same value are part of the same group.

**Each message is delivered to a single subscriber within the group.**
In other words, the message is delivered to the group, not to each subscriber individually.
Most often, subscribers within the same group receive messages in a round-robin fashion, which distributes the load evenly.


```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant T as Topic
    participant S1 as Subscriber 1
    participant S2 as Subscriber 2

    Pub->>T: Publish message 1
    T->>S1: Deliver message 1
    S1-->>T: Ack
    Pub->>T: Publish message 2
    T->>S2: Deliver message 2
    S2-->>T: Ack
```

Additionally, Redis remembers the last message delivered to each group.
If a subscriber restarts, it will receive the messages sent during the time it was down
(unless there were other subscribers in the same group and it already processed them).

To set a consumer group for the Redis Streams subscriber, pass the chosen name as the `ConsumerGroup` config option.

```go
subscriber, err := redisstream.NewSubscriber(
	redisstream.SubscriberConfig{
		Client: redisClient, 
		ConsumerGroup: "my_consumer_group",
	},
	logger,
)
```


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

What do consumer groups look like in other Pub/Subs?

- Kafka uses *consumer groups*, like Redis.
- RabbitMQ uses *queues*.
- Google Cloud Pub/Sub and NATS use *subscriptions*.

</span>
	</div>
	</div>

## Exercise

File: `03-message-broker/05-consumer-groups/main.go`

The `Subscribe` function sets up two subscribers for the same topic: `orders-placed`.
With each new order, the code appends a row to a spreadsheet and sends a notification to the user.

Update the exercise code so that each subscriber is assigned to a different consumer group.
Call the groups `notifications` and `spreadsheets`.
