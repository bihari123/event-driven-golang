# Negative Ack

As with handling HTTP requests, processing a message can fail for many reasons, such as the message being invalid or the database being down.
In contrast to synchronous calls, we can't just return the error to the client
because the client is not waiting for a response at this point.

The most important aspect of handling errors is not to lose the message.
Usually, you don't want to call `Ack()` for a message that failed to process.
Instead, there's a `Nack()` method that sends a "negative acknowledgement" that tells the broker
to return the message back to the queue. 
What happens next depends on the Pub/Sub implementation, but most often, the message will be redelivered, either immediately or after a delay.

```go
for msg := range messages {
	orderID := string(msg.Payload)
	fmt.Println("New order placed with ID:", orderID)
	
	err := SaveToDatabase(orderID)
	if err != nil {
		fmt.Println("Error saving to database:", err)
		msg.Nack()
		continue
	}
	
	msg.Ack()
}
```

We'll look into different strategies for error handling in future modules. For now, returning the message
back to the queue is good enough: It will be redelivered and processed again, which sometimes is all we need.

## Exercise

File: `03-message-broker/04-nack/main.go`

We're using a smoke sensor that publishes messages on a Pub/Sub topic. The message's payload can be one of two values:

* `0` - no smoke detected
* `1` - smoke detected

Based on this, we want to turn the alarm on or off using the provided `AlarmClient`. 

Sometimes, changing the alarm state can fail (a non-nil error is returned). In this case, we want to retry the processing.

Fill in the missing logic in `ConsumeMessages`. Turn the alarm on or off, depending on the value in the payload.
Then check the error and call `Ack()` or `Nack()` on the message.

The `0` and `1` values are strings - there's no need to convert them to integers.

Note: you can see the tests for this exercise.
They use the [`GoChannel` Pub/Sub](https://watermill.io/pubsubs/gochannel/).
It's an in-memory Pub/Sub using Go channels, ideal for testing.
