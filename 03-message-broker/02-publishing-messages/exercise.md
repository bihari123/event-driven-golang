# Publishing Messages

When you want to spin up an HTTP server, you don't write the HTTP protocol manually. Instead, you choose a library
that does the heavy lifting for you and gives you a nice API to define all the endpoints.

Back in 2018, we thought the same should be true when working with messages. Since there was no library in Go that worked like this,
we decided to create it. The library is called [Watermill](https://github.com/ThreeDotsLabs/watermill), and we've been using
it in production for many different projects for years now.

We're going to use Watermill for the rest of the training exercises because we believe it will provide you with a great experience for exploring
event-driven patterns. We designed Watermill as a library, not a framework, so there's no vendor lock-in; if you decide to use anything else, it should be straightforward to translate the examples.
Finally, note that none of the ideas we share is Watermill-specific.

### The Publisher

Watermill hides all the complexity of Pub/Subs behind just two interfaces: the `Publisher` and the `Subscriber`.

For now, let's consider the first one.

```go
type Publisher interface {
	Publish(topic string, messages ...*Message) error
	Close() error
}
```

To publish a message, you need to pass a `topic` and a slice of messages.

To *publish* a message means to append it to the given topic.
Anyone who subscribes to the same topic will receive the messages on it in a first-in, first-out (FIFO) fashion.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>
While FIFO is a common way to deliver messages, it can vary depending on the Pub/Sub and how it's configured.

This is true for many behaviors we'll describe in this training.
It's best to check the documentation of the Pub/Sub you're using to confirm it works as you expect.
</span>
	</div>
	</div>

To create a message, use the `NewMessage` constructor, like this, which takes just two arguments. 

```go
msg := message.NewMessage(watermill.NewUUID(), []byte(orderID))
```

The first argument is the message's UUID, which is used mainly for debugging purposes.
Most of the time, any kind of UUID is fine.

The second argument is the payload. It's a slice of bytes, so it can be anything you want, as long as you can marshal it.

To publish this message, call the `Publish` method:

```go
err := publisher.Publish("orders", msg)
if err != nil {
	panic(err)
}
```

First, however, you need a publisher. The exact publisher will depend on the Pub/Sub you choose.

Here's how to create one for Redis Streams:

```go
logger := watermill.NewStdLogger(false, false)

rdb := redis.NewClient(&redis.Options{
	Addr: os.Getenv("REDIS_ADDR"),
})

publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
	Client: rdb,
}, logger)
if err != nil {
	panic(err)
}
```

The `NewStdLogger`'s arguments are for `debug` and `trace`, respectively.
You don't need to use this logger: You can adapt any other logger you use to the [`watermill.LoggerAdapter`](https://github.com/ThreeDotsLabs/watermill/blob/559222086a70e83f930fd904c2a53991749f3877/log.go#L43) interface.

You need the following imports to make the code above work:

```go
import (
	"os"
	
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)
```

## Exercise

File: `03-message-broker/02-publishing-messages/main.go`

Create a publisher for Redis Streams, and publish two messages on the `progress` topic.
The first one's payload should be `50`, and the second one's should be `100`.

To get the necessary dependencies, either run `go get` for them individually, or run `go mod tidy` after adding your code.
