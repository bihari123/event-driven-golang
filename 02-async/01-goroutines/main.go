package main

import (
	"log"
	"time"
)

type User struct {
	Email string
}

type UserRepository interface {
	CreateUserAccount(u User) error
}

type NotificationsClient interface {
	SendNotification(u User) error
}

type NewsletterClient interface {
	AddToNewsletter(u User) error
}

type Handler struct {
	repository          UserRepository
	newsletterClient    NewsletterClient
	notificationsClient NotificationsClient
}

func NewHandler(
	repository UserRepository,
	newsletterClient NewsletterClient,
	notificationsClient NotificationsClient,
) Handler {
	return Handler{
		repository:          repository,
		newsletterClient:    newsletterClient,
		notificationsClient: notificationsClient,
	}
}

func (h Handler) SignUp(u User) error {
	if err := h.repository.CreateUserAccount(u); err != nil {
		log.Println("Error creating the user ", err)
		return err
	}

	go func() {
		for {
			if err := h.newsletterClient.AddToNewsletter(u); err != nil {
				log.Println("error adding to the news letter ", err)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	}()

	go func() {
		for {

			if err := h.notificationsClient.SendNotification(u); err != nil {
				log.Println("error sending the notification")
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	}()

	return nil
}
