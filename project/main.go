package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
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
		"issue-receipt",
		issueSub,
		func(msg *message.Message) error {
			clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
			if err != nil {
				// panic(err)
				return err
			}
			receiptsClient := NewReceiptsClient(clients)

			// for msg := range messages {
			ticketId := string(msg.Payload)
			err = receiptsClient.IssueReceipt(msg.Context(), ticketId)
			// }
			return err
		},
	)
	router.AddNoPublisherHandler(
		"my_handler2",
		"append-to-tracker",
		trackerSub,
		func(msg *message.Message) error {
			clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
			if err != nil {
				// panic(err)
				return err
			}
			spreadsheetClient := NewSpreadsheetsClient(clients)

			// for msg := range messages {
			ticketId := string(msg.Payload)
			err = spreadsheetClient.AppendRow(
				msg.Context(),
				"tickets-to-print",
				[]string{ticketId},
			)
			return err
		},
	)

	e := commonHTTP.NewEcho()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			// w.Send(Message{Task: TaskIssueReceipt, TicketId: ticket},
			// 	Message{Task: TaskAppendToTracker, TicketId: ticket},
			// )

			go func() {
				for {
					msg := message.NewMessage(watermill.NewUUID(), []byte(string(ticket)))
					if err := publisher.Publish("issue-receipt", msg); err != nil {
						continue
					}
					break
				}
			}()

			go func(ticketId []byte) {
				for {
					msg := message.NewMessage(watermill.NewUUID(), ticketId)
					if err := publisher.Publish("append-to-tracker", msg); err != nil {
						continue
					}
					break
				}
			}([]byte(string(ticket)))
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

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
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
