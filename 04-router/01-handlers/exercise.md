# Watermill Router: Handlers

The `Publisher` and `Subscriber` abstract away many details of the underlying Pub/Sub, but they are still quite low-level interfaces. 
Watermill's high-level API is the `Router`.

The `Router` is similar to an HTTP router and gives you a convenient way to define message handlers.

Creating a router is as simple as:

```go
router, err := message.NewRouter(message.RouterConfig{}, logger)
```

You can define a new handler with the `AddHandler` method. A message handler is a function that
receives a message from a given topic and publishes another message (or messages) to another topic.

You need to pass a publisher and subscriber along with some other parameters:

```go
router.AddHandler(
	"handler_name", 
	"subscriber_topic", 
	subscriber, 
	"publisher_topic", 
	publisher, 
	func(msg *message.Message) ([]*message.Message, error) {
		newMsg := message.NewMessage(watermill.NewUUID(), []byte("response"))
		return []*message.Message{newMsg}, nil
	},
)
```

The handler name is used mostly for debugging.
It can be any string you want, but it needs to be unique across handlers within the same Router.

The Router handles the publisher and subscriber orchestration for you.
You just define the input and output topics and the handler function with the message processing logic.

All messages that the handler function returns will be published to the given topic.

The returned `error` has a key role.
If the handler function returns `nil`, the message is acknowledged.
If the handler returns an error, a negative acknowledgement is sent.
Thanks to this, you don't need to worry about calling `Ack()` or `Nack()` anymore.

After adding all the handlers, you need to start the Router:

```go
err = router.Run(context.Background())
```

`Run` is blocking. If you don't run it in a separate goroutine, make sure you add all your handlers first.

## Exercise

File: `04-router/01-handlers/main.go`

Add a new handler to the Router.
It should subscribe to values from the `temperature-celsius` topic and publish the converted values to the `temperature-fahrenheit` topic.
You can use the included `celsiusToFahrenheit` function to convert the values.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

You don't need to call `Ack()` or `Nack()` in the Router's handler function.
The message is acknowledged if the returned error is `nil`.

</span>
	</div>
	</div>
