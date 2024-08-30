# Introducing the Project

It's time to kick off your long-running project. 


<div class="alert alert-dismissible bg-info text-white d-flex flex-column flex-sm-row p-7 mb-10">
    <div class="d-flex flex-column">
        <h3 class="mb-5 text-white">
			Background	
		</h3>
        <span>

You join a new company as a senior engineer. The business is a ticket aggregator for live shows that  integrates with a few ticket vendors, so users can buy all their tickets in one place.

The good news is that the company has been doing very well over the last couple of months. The product-market fit is there,
and many clients are signing up every day.

One caveat is that the codebase is not in the best shape. The MVP was successful at getting VCs to invest in the startup,
but the architecture can't handle the load. You're the new hire, and hopefully, you'll be able to sort everything out!

*(Surprised? We promised only real-life scenarios!)*

</span>
	</div>
	</div>

### The Common Package

We want you to focus on the event-driven part of the project.
There's a `common` package (in a [public repository](https://github.com/ThreeDotsLabs/go-event-driven))
with ready to use code for things like the HTTP server, HTTP clients, and logging.
You don't need to use it, but it's there if you want to.

### The Gateway

All project exercises (and some non-project as well) use external services via HTTP.
They're all available through a *gateway service* (serving as a reverse proxy).
You can access it by the `GATEWAY_ADDR` environment variable.
