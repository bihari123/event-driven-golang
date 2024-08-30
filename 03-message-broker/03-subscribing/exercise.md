# Subscribing for messages

To receive messages, you need to *subscribe* to the same topic they're being published on.

```go
type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	Close() error
}
```

The idea is very similar to the publishing side: You must specify a topic, so the Pub/Sub knows which messages to deliver to you.
Most of the time, this will be the same string that was used on the publishing side.

```go
messages, err := subscriber.Subscribe(context.Background(), "orders")
if err != nil {
	panic(err)
}

for msg := range messages {
	orderID := string(msg.Payload)
	fmt.Println("New order placed with ID:", orderID)
}
```

`Subscribe` returns a channel of messages.
You can use it like any Go channel.
Any new message published on the topic will be delivered to it.

Most Pub/Subs deliver a single message at a time. You need to let the broker know that a message has been correctly processed with a *message acknowledgement*, usually abbreviated as *ack*.

Watermill's messages expose the `Ack()` method, which does this.
The correct iteration looks like this:

```go
for msg := range messages {
	orderID := string(msg.Payload)
	fmt.Println("New order placed with ID:", orderID)
	msg.Ack()
}
```

It's easy to miss this step, but it's crucial.
If you notice that your subscriber receives a single message and then stops,
it's probably because you forgot to `Ack()` the message.

Creating a subscriber is very similar to creating a publisher:

```go
subscriber, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
	Client: rdb,
}, logger)
```

### Closing the Subscriber

While each `Publish` is a one-time operation, `Subscribe` starts an asynchronous worker process.

To close it you can either call the `Close()` method on the subscriber, or cancel the context passed to `Subscribe`.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

### The Context Primer

If you're not familiar with `context.Context`, here's a short introduction.

The Context is used mainly for two purposes:

* Canceling long-running operations, either via timeouts or explicit cancellation.
* Passing arbitrary values between functions.

The base context is created with `context.Background()`.
It's an empty context, that has no behavior.

```go
ctx := context.Background()
```

You can create a new context from an existing one with `context.WithCancel()`.
Calling the `cancel` function will cancel the context and all contexts created from it.

```go
ctx, cancel := context.WithCancel(context.Background())
```

You can also create a context with a timeout, which will cancel itself after the specified duration.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
```

You will see `context.Context` as the first argument of many functions.
It usually indicates that the function does an external request or a long-running operation.

</span>
	</div>
	</div>

## Exercise

File: `03-message-broker/03-subscribing/main.go`

Create a Redis Stream subscriber and subscribe to the `progress` topic.

Print the incoming messages in the following format:

```text
Message ID: a16b6ab0-8c29-48f5-9d26-b508906af976 - 50%
Message ID: d33c5cac-1cce-4783-b931-7ecba25fa7dc - 100%
```

Don't forget to *ack* the messages.

For now, use `context.Background()` where a context is needed.
