# Synchronous vs. Asynchronous

Event-driven patterns arise from the asynchronous approach to building systems, but synchronous patterns are much more common in most systems.

The most widely used type of synchronous communication is HTTP, using REST or another style.
A similar idea underlies RPCs as well, such as gRPC: There's a client sending a request and a server processing it
and replying with a response.

This simplicity is a big advantage of the request-reply pattern, but there's also a drawback: Whatever your application does,
it needs to wait for the other server to complete the request. This can take a long time, and the request or response can be interrupted or may fail for
unknown reasons. 
You can decide not to wait for an HTTP response, but then you have no idea if the request was successful.

Synchronous APIs are almost always designed so that waiting for the request is the only way to know the result.
Problems start when you make multiple synchronous calls within one action.

```go
func SignUp(u User) error {
	if err := CreateUserAccount(u); err != nil {
		return err
	}
	
	if err := AddToNewsletter(u); err != nil {
		return err
	}
	
	if err := SendNotification(u); err != nil {
		return err
	}
	
	return nil
}
```

In this example, `AddToNewsletter` and `SendNotification` are HTTP requests over the network.
What happens when one of the calls fails because the other service is down?

You need to choose one of these options:

* Return an error to the user, roll back the database changes, and prevent them from signing up. Business-wise, this is probably not what you want.
* Return a success result to the user, but you will then end up with an inconsistency across your systems that creates manual work for engineers to fix the problem later.

When users sign up, we want to create their accounts, so they can log in and use the website.
We also add them to the newsletter and send them a welcome email.
If one of these actions fails, we don't want to block the user from signing up and placing an order, but we do want the action to happen eventually.
This is where asynchronous patterns can help.

## Exercise

File: `02-async/01-goroutines/main.go`

Let's start with something naive that still gets the job done.

In the exercise code, you will find the `SignUp` method, which is similar to the one above.

**We made the newsletter and notification APIs to be not stable and sometimes go down for unknown reasons.**
We don't want this to block users from signing up.
We also want users to be added to the newsletter and have a notification sent as soon as the APIs are back online.


<div class="alert alert-dismissible bg-light-primary d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-dark">
			<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-lightbulb text-primary" viewBox="0 0 16 16">
			  <path d="M2 6a6 6 0 1 1 10.174 4.31c-.203.196-.359.4-.453.619l-.762 1.769A.5.5 0 0 1 10.5 13a.5.5 0 0 1 0 1 .5.5 0 0 1 0 1l-.224.447a1 1 0 0 1-.894.553H6.618a1 1 0 0 1-.894-.553L5.5 15a.5.5 0 0 1 0-1 .5.5 0 0 1 0-1 .5.5 0 0 1-.46-.302l-.761-1.77a1.964 1.964 0 0 0-.453-.618A5.984 5.984 0 0 1 2 6zm6-5a5 5 0 0 0-3.479 8.592c.263.254.514.564.676.941L5.83 12h4.342l.632-1.467c.162-.377.413-.687.676-.941A5 5 0 0 0 8 1z"/>
			</svg>
			Tip
		</h3>
        <span>

If you see `network error` it's not a problem with our platform - it's a part of the exercise!
Your task is to make it more resilient to such errors.

</span>
	</div>
	</div>

A trivial way to make a request asynchronous is running it in a goroutine.

```go
go func() {
	if err := AddToNewsletter(u); err != nil {
		log.Printf("failed to add user to the newsletter: %v", err)
	}
}()
```

However, any errors are lost this way. We can add simple retries to make sure it eventually succeeds.

```go
go func() {
	for {
		if err := AddToNewsletter(u); err != nil {
			log.Printf("failed to add user to the newsletter: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
}()
```

As mentioned, this is a simplified approach: A simple restart of the service is enough to lose all the retries in progress.
However, it's a good start to illustrate the idea. 

Apply a similar solution to the exercise code.
Don't forget about short sleeps between the retries.
