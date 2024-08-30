package main

import (
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

type ProductOutOfStock struct {
	ProductID string `json:"product_id"`
	Header    Header `json:"header"`
}

type Header struct {
	ID        string `json:"id"`
	EventName string `json:"event_name"`
	OccuredAt string `json:"occurred_at"`
}

func NewHeader(eventName string) Header {
	return Header{
		ID:        uuid.NewString(),
		EventName: eventName,
		OccuredAt: time.Now().Format(time.RFC3339),
	}
}

type ProductBackInStock struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Header    Header `json:""header"`
}

type Publisher struct {
	pub message.Publisher
}

func NewPublisher(pub message.Publisher) Publisher {
	return Publisher{
		pub: pub,
	}
}

func (p Publisher) PublishProductOutOfStock(productID string) error {
	event := ProductOutOfStock{
		ProductID: productID,
		Header:    NewHeader("ProductOutOfStock"),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)

	return p.pub.Publish("product-updates", msg)
}

func (p Publisher) PublishProductBackInStock(productID string, quantity int) error {
	event := ProductBackInStock{
		ProductID: productID,
		Quantity:  quantity,
		Header:    NewHeader("ProductBackInStock"),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)

	return p.pub.Publish("product-updates", msg)
}
