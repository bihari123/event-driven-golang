package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

/*
```json
{
  "header": {
    "id": "...",
    "published_at": "..."
  },
  "ticket_id": "...",
  "customer_email": "...",
  "price": {
    "amount": "100",
    "currency": "EUR"
  }
}
```

*/

type EventHeader struct {
	Id          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

func NewEventHeader() EventHeader {
	return EventHeader{
		Id: uuid.NewString(),
		// PublishedAt: time.Now().Format(time.RFC3339),
		PublishedAt: time.Now().UTC(),
	}
}

type Ticket struct {
	TicketId      string `json:"ticket_id"`
	Status        string `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"price"`
}

type TicketBookingConfirmed struct {
	Header        EventHeader `json:"header"`
	TicketId      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"price"`
}

type TicketIssue struct {
	TicketId string `json:"ticket_id"`
	Price    struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"price"`
}
type TicketBookingCanceled struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type TicketSpreadSheet struct {
	TicketId      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         Money  `json:"price"`
}

type TicketsConfirmationRequest struct {
	Tickets []Ticket `json:"tickets"`
}

func main() {
	log.Init(logrus.InfoLevel)

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

	issueSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "issueGroup",
	}, logger)
	if err != nil {
		panic(err)
	}

	refundSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "refundGroup",
	}, logger)
	if err != nil {
		panic(err)
	}

	trackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "trackerGroup",
	}, logger)
	if err != nil {
		panic(err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"my_handler",
		"TicketBookingConfirmed",
		issueSub,
		func(msg *message.Message) error {
			clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
			if err != nil {
				// panic(err)
				return err
			}
			receiptsClient := NewReceiptsClient(clients)

			var ticket TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &ticket); err == nil {
				err = receiptsClient.IssueReceipt(msg.Context(), ticket)
			}
			return err
		},
	)
	router.AddNoPublisherHandler(
		"my_handler2",
		"TicketBookingConfirmed",
		trackerSub,
		func(msg *message.Message) error {
			clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
			if err != nil {
				// panic(err)
				return err
			}
			spreadsheetClient := NewSpreadsheetsClient(clients)

			// for msg := range messages {

			var ticket TicketBookingConfirmed
			if err := json.Unmarshal(msg.Payload, &ticket); err == nil {
				err = spreadsheetClient.AppendRow(
					msg.Context(),
					"tickets-to-print",
					[]string{
						ticket.TicketId,
						ticket.CustomerEmail,
						ticket.Price.Amount,
						ticket.Price.Currency,
					},
				)
			}
			return err
		},
	)

	router.AddNoPublisherHandler(
		"my_handler3",
		"TicketBookingCanceled",
		refundSub,
		func(msg *message.Message) error {
			clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
			if err != nil {
				// panic(err)
				return err
			}
			spreadsheetClient := NewSpreadsheetsClient(clients)

			var ticket TicketBookingCanceled
			if err := json.Unmarshal(msg.Payload, &ticket); err == nil {
				err = spreadsheetClient.AppendRow(
					msg.Context(),
					"tickets-to-refund",
					[]string{
						ticket.TicketID,
						ticket.CustomerEmail,
						ticket.Price.Amount,
						ticket.Price.Currency,
					},
				)
			}
			return err
		},
	)

	e := commonHTTP.NewEcho()

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			if ticket.Status == "confirmed" {
				event := TicketBookingConfirmed{
					TicketId:      ticket.TicketId,
					CustomerEmail: ticket.CustomerEmail,
					Price:         ticket.Price,
					Header:        NewEventHeader(),
				}
				if jsonObj, err := json.Marshal(event); err == nil {

					go func(ticket []byte) {
						for {

							msg := message.NewMessage(watermill.NewUUID(), ticket)
							if err := publisher.Publish("TicketBookingConfirmed", msg); err != nil {
								continue
							}
							break
						}
					}(jsonObj)

					go func(ticket []byte) {
						for {
							msg := message.NewMessage(watermill.NewUUID(), ticket)
							if err := publisher.Publish("TicketBookingConfirmed", msg); err != nil {
								continue
							}
							break
						}
					}(jsonObj)
				}

			} else {
				event := TicketBookingCanceled{
					TicketID:      ticket.TicketId,
					CustomerEmail: ticket.CustomerEmail,
					Price:         ticket.Price,
					Header:        NewEventHeader(),
				}
				if jsonObj, err := json.Marshal(event); err == nil {
					go func(ticket []byte) {
						for {
							msg := message.NewMessage(watermill.NewUUID(), ticket)
							if err := publisher.Publish("TicketBookingCanceled", msg); err != nil {
								continue
							}
							break
						}
					}(jsonObj)
				}
			}
		}
		return c.NoContent(http.StatusOK)
	})

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return router.Run(ctx)
	})

	g.Go(func() error {
		<-router.Running()
		logrus.Info("Server starting...")
		err = e.Start(":8080")
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()
		return e.Shutdown(ctx)
	})

	if err := g.Wait(); err != nil {
		panic(err)
	}
}

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticket TicketBookingConfirmed) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticket.TicketId,
		Price: receipts.Money{
			MoneyAmount:   ticket.Price.Amount,
			MoneyCurrency: ticket.Price.Currency,
		},
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
}

func (c SpreadsheetsClient) AppendRow(
	ctx context.Context,
	spreadsheetName string,
	row []string,
) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(
		ctx,
		spreadsheetName,
		request,
	)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}
