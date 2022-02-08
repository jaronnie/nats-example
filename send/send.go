package main

import (
	"context"
	"encoding/json"
	"fmt"
	"mynats/sdk"
	"time"
)

const QueueLen = 1000

// message of resource's update
const resourcesUpdateTopic = "rdsutbbp-resources-update"

// message que
var queue = make(chan msg, QueueLen)

type Message struct {
	DataJson      string // json message
	ResourcesType string // resources type
	ResourcesID   int64  // resources ID int database
}

type msg struct {
	retry   int
	ctx     context.Context
	message *Message
}

func main() {
	SendMQ(context.Background(), &Message{
		DataJson:      "",
		ResourcesType: "",
		ResourcesID:   1,
	}, 1)
	time.Sleep(time.Second*10)
}

// SendMQ 发送消息队列通知上层更新表 异步并重试
func SendMQ(ctx context.Context, message *Message, retry int) {
	queue <- msg{
		retry:   retry,
		ctx:     ctx,
		message: message,
	}
}

func sendResourcesUpdateMessage(ctx context.Context, message *Message) error {
	messageJson, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return sdk.NewServiceMQ("127.0.0.1:4222").Send(resourcesUpdateTopic, string(messageJson))
}

func init() {
	go func() {
		for {
			select {
			case msg := <-queue:
				{
					err := sendResourcesUpdateMessage(msg.ctx, msg.message)
					if err != nil {
						fmt.Println(err)
						msg.retry++
						if msg.retry < 10 {
							queue <- msg
						}
					}
				}
			}
		}
	}()
}
