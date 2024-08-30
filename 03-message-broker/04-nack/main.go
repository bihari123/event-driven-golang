package main

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type AlarmClient interface {
	StartAlarm() error
	StopAlarm() error
}

func ConsumeMessages(sub message.Subscriber, alarmClient AlarmClient) {
	messages, err := sub.Subscribe(context.Background(), "smoke_sensor")
	if err != nil {
		panic(err)
	}

	for msg := range messages {

		/*var payload string
		err := json.Unmarshal(msg.Payload, &payload)
		if err != nil {
			msg.Nack()
			continue
		}
		*/
		switch string(msg.Payload) {
		case "0":
			// stop the alarm
			if err := alarmClient.StopAlarm(); err != nil {
				msg.Nack()
				continue
			}
		case "1":
			if err := alarmClient.StartAlarm(); err != nil {
				msg.Nack()
				continue
			}
		}
		msg.Ack()
	}
}
