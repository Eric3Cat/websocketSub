package websocketSub

import (
	"bytes"
	"context"
	"errors"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"io"
	"log"
	"sync"
	"time"
)

const (
	// writeWait 是向对等方写入消息的时间限制。
	writeWait = 10 * time.Second

	// pongWait 是读取对等方下一个pong消息的时间限制。
	// pongWait = 60 * time.Second
	pongWait = 60 * time.Second

	// pingPeriod 是向对等方发送ping消息的周期。必须小于pongWait。
	pingPeriod = (pongWait * 9) / 10

	// maxMessageSize 是对等方允许的最大消息大小。
	maxMessageSize = 2048000000

	// bufSize 是发送缓冲区大小
	//bufSize = 256
	bufSize = 204800

	// ackEvent 是用于确认消息的事件名称。
	ackEvent = "ack"
)

// Client表示一个客户端连接。
type Client struct {
	// The client subscribe channels
	// 客户端订阅的频道
	Channels []string
	// The websocket connection.
	// websocket连接
	conn *websocket.Conn
	// The channel subId map
	// 渠道子ID映射表
	SubIds map[string]int64
	// Buffered channel of outbound messages.
	// 输出消息的缓冲通道
	Send chan []byte
	// ctx
	Ctx context.Context

	// uuid
	Id string

	Solid *Solid

	mu sync.Mutex
}

// HandlerSubId 是用于生成子ID的函数。
type HandlerSubId func(subId int64) error

// GetSubId 是用于获取子ID的函数。
type GetSubId func(channel string) int64

// MustNewClient创建一个新的Client实例。
func MustNewClient(ctx context.Context, conn *websocket.Conn, id string, solidOption *SolidOption) *Client {
	client := &Client{
		Channels: []string{},
		conn:     conn,
		SubIds:   map[string]int64{},
		Send:     make(chan []byte, bufSize),
		Ctx:      ctx,
		Id:       id,
	}
	client.Solid = MustNewSolid(solidOption, client)

	return client
}

// Subscribe将频道添加到Client的订阅列表中。
func (c *Client) Subscribe(channel string) error {
	if Contains(c.Channels, channel) {
		return errors.New("duplicate subscribe")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Channels = append(c.Channels, channel)
	return nil
}

// BindChannelWithSubId将频道与子ID关联起来。
func (c *Client) BindChannelWithSubId(channel string, subId int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SubIds[channel] = subId
}

// UnSubscribe从Client的订阅列表中解除频道的订阅。
//func (c *Client) UnSubscribe(channel string) int64 {
//	c.mu.Lock()
//	for idx, v := range c.Channels {
//		// 从channel subId切片中删除channel
//		if v == channel {
//			c.Channels = append(c.Channels[:idx], c.Channels[idx+1:]...)
//		}
//	}
//	c.mu.Unlock()
//
//	// 从channel subId中删除channel
//	subId := c.SubIds[channel]
//	c.mu.Lock()
//	delete(c.SubIds, channel)
//	c.mu.Unlock()
//	return subId
//}

// close关闭Client的连接和资源。
func (c *Client) defaultClose(pubSubClient *PubSubClient) {
	log.Printf("[ 开始执行关闭ws ]   [by- %v]", "client.go-close(pubSubClient *PubSubClient)")
	for _, subId := range c.SubIds {
		pubSubClient.UnSubscribe(subId)
	}
	c.conn.Close()
	log.Printf("[ 成功关闭ws ]   [by- %v]", "client.go-close(pubSubClient *PubSubClient)")
}

// close关闭Client的连接和资源。
func (c *Client) close(pubSubClient *PubSubClient) {
	for _, subId := range c.SubIds {
		pubSubClient.UnSubscribe(subId)
	}
	defer func() {
		c.conn.Close()
		log.Printf("[ 关闭ws ]   [by- %v]", "client.go-close(pubSubClient *PubSubClient)")
		c = nil
	}()
}

// ReadPump在pubSubClient上读取消息并处理。
func (c *Client) ReadPump(pubSubClient *PubSubClient) {
	defer func() {
		c.close(pubSubClient) // 异常退出时关闭连接
	}()

	c.conn.SetReadLimit(maxMessageSize)              // 设置连接读取限制大小
	c.conn.SetReadDeadline(time.Now().Add(pongWait)) // 设置读取超时时间
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait)) // 重新设置读取超时时间
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage() // 读取消息

		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err) // 打印错误日志
			}
			break
		}

		message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1)) // 去除消息两侧空格并替换换行符为空格
		var event Event
		_ = jsoniter.Unmarshal(message, &event) // 解析消息为event结构体

		if event.EventName == "ping" { // 如果消息类型为ping
			var pong Event
			pong.EventName = "pong"
			pongByte, _ := jsoniter.Marshal(&pong) // 将pong结构体转为json字节流
			c.Send <- pongByte                     // 发送pong消息
		} else {
			if onMessageWrapper, ok := subScribeFuncs[event.EventName]; ok { // 如果订阅函数存在
				onMessage := onMessageWrapper.OnMessage      // 获取消息处理函数
				channelKeyFun := onMessageWrapper.ChannelFun // 获取频道函数
				var channel = event.EventName                // 频道名称
				if channelKeyFun != nil {
					channel = channelKeyFun(context.Background(), []byte(event.Data)) // 根据频道函数获取具体频道
				}

				GoSafe(func() { // 协程安全执行
					err = c.Subscribe(channel) // 订阅频道
					if err != nil {
						return
					}
					subId := pubSubClient.Subscribe(c, channel, onMessage) // 在pubsubclient中订阅频道
					c.BindChannelWithSubId(channel, subId)                 // 将频道与订阅id关联
					GoSafe(func() {                                        // 协程安全执行
						c.Solid.PullOfflineMessage() // 拉取离线消息以等待重新发送
					})
				})
			}

			if event.EventName == ackEvent { // 如果消息类型为ack
				var ackEvent Event
				_ = jsoniter.Unmarshal([]byte(event.Data), &ackEvent) // 解析ack消息数据为event结构体
				c.Solid.Ack(context.Background(), &ackEvent)          // 执行ack操作
			}
		}
	}
}

// writePump将消息从hub写入websocket连接。
//
// 每个连接都会启动一个writePump goroutine来执行所有写入操作。
func (c *Client) writePump(pubSubClient *PubSubClient) {
	ticker := time.NewTicker(pingPeriod)

	defer func() {
		ticker.Stop()
		c.close(pubSubClient)
	}()

	for {
		log.Printf("[ ws writePump  Current user %v, Go Channel length is %v]", c.Id, len(c.Send))
		select {
		case message, ok := <-c.Send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// hub关闭了该通道。
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				log.Printf("[ ws writePump SetWriteDeadline !ok]")
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				log.Printf("[ ws writePump c.conn.NextWriter 出现错误 error: %v ]", err)
				return
			}

			w.Write(message)
			log.Printf("[ ws writePump Write message Success %v]", string(message))
			// 将队列中的聊天消息添加到当前websocket消息中。
			c.WriteData(w)

			if err := w.Close(); err != nil {
				log.Printf("[ ws writePump w.Close() err %v]", err)
				return
			}
			log.Printf("[ ws writePump 响应成功 ")
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// WriteData将发送数据写入w。
func (c *Client) WriteData(w io.WriteCloser) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.Send)
	for i := 0; i < n; i++ {
		w.Write(newline)
		w.Write(<-c.Send)
	}
	return
}
