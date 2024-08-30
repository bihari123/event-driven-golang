# Marshalling

So far, our messages have contained just a single string, like the ID.
In real applications, you often need to send more complex data.

Most events can be represented as structs, for example:

```go
type OrderPlaced struct {
	ID         string
	CustomerID string
	Total      Money
	PlacedAt   time.Time
}
```

To send this over the Pub/Sub, you need to marshal (serialize) it to a slice of bytes.
For example, using JSON:

```go
type OrderPlaced struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Total      string `json:"total"`
	PlacedAt   string `json:"placed_at"`
}
```

Notice how we've changed complex types (`Money` and `time.Time`) to primitive types.

You can follow two paths here: Either keep complex types and use their default marshallers
(or define custom marshallers for them), or keep only primitive types in the events.

The first approach is more convenient, but gives you less control over the output.
For example, `time.Time` marshals to a string in the `RFC3339Nano` format by default.
If you need to change it at some point, you will need to rework all the fields that use it. 

Using only primitive types makes the result more explicit, but requires more boilerplate.
You would usually keep two types for each event: one on the application layer, and one on the Pub/Sub layer.
You would need to map the fields manually between them.

Consider which approach is better for your use case.
As a rule of thumb, keeping separate structs can be a good idea if you have a proper domain layer with isolated domain logic.
On the other hand, if your events have lots of fields and aren't close to the domain, the default marshallers may be good enough.

Finally, if you're not sure, start with one approach and make sure it's easy to change later.
Sometimes it's better to have a working solution than to spend too much time on finding the perfect one.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>
For more details on using separate models, see our blog posts:

- [Business Applications in Go: Things to know about DRY](https://threedots.tech/post/things-to-know-about-dry/)
- [Introducing Clean Architecture by refactoring a Go project](https://threedots.tech/post/introducing-clean-architecture/) 
</span>
	</div>
	</div>


Publishing a marshalled event can look like this:

```go
func PublishOrderPlaced(orderPlaced app.OrderPlaced) error {
	event := OrderPlaced {
		ID:         orderPlaced.ID, 
		CustomerID: orderPlaced.CustomerID, 
		Total:      fmt.Sprintf("%v %v", orderPlaced.Total.Amount, orderPlaced.Total.Currency), 
		PlacedAt:   orderPlaced.PlacedAt.Format(time.RFC3339),
	}
	
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	
	msg := message.NewMessage(watermill.NewUUID(), payload)
	
	return publisher.Publish("orders-placed", msg)
}

```

The exact format doesn't matter that much as long as the publisher and the subscriber use the same one.
While JSON is very popular, there are more specialized formats like [Protocol Buffers](https://protobuf.dev) or [Avro](https://avro.apache.org).

We often use Protocol Buffers for the benefits it provides over JSON:

- It's a binary format (smaller messages and faster processing).
- It has a typed schema.
- It's easy to generate ready-to-use structs.

There are some trade-offs, though:

- The binary messages are not human-readable, so they're harder to debug.
- You need to set up the tools to generate the code.

Some brokers provide a *schema registry* that allows you to store the message schemas in a central place.
They are beyond the scope of this training.
For Protocol Buffers, a good start is keeping the schema files in a repository.

## Exercise

File: `05-events/02-marshalling/main.go`

Add a new handler that subscribes to events from the `payment-completed` topic.

The handler should unmarshal the message payload to the `PaymentCompleted` struct and
then send a new payload to the `order-confirmed` topic in the JSON format:

```json
{
  "order_id": "...",
  "confirmed_at": "..."
}
```

The `confirmed_at` field should be taken from the `CompletedAt` field of the `PaymentCompleted` event.
