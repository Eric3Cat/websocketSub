package websocketSub

import (
	"context"
	red "github.com/go-redis/redis/v8"
	"log"
	"math"
	"sync"
	"sync/atomic"
)

// Reviver 是一个函数类型，用于将Redis中的key和value转换为接口类型的数据
type Reviver func(key string, value string) interface{}

// OnMessage 是一个函数类型，用于处理接收到的消息
type OnMessage func(client *Client, data []byte)

// Listener 是一个结构体，用于保存订阅者的信息
type Listener struct {
	Client    *Client
	Channel   string
	OnMessage OnMessage
}

// PubSubRedisOptions 是一个结构体，用于保存PubSubClient的Redis连接选项
type PubSubRedisOptions struct {
	Publisher   *red.Client
	Subscriber  *red.Client
	SolidOption *SolidOption
}

// PubSubClient 是一个结构体，用于实现基于Redis的PubSub功能的客户端
type PubSubClient struct {
	Publisher   *red.Client
	Subscriber  *red.Client
	subMap      map[int64]*Listener
	subsRefsMap map[string][]int64
	subId       int64
	PubSub      *red.PubSub
	mu          sync.Mutex
	DropRun     chan struct{}
	SolidOption *SolidOption
}

// NewPubSubClient 创建并返回一个新的PubSubClient实例
func NewPubSubClient(pubSubRedisOptions PubSubRedisOptions) *PubSubClient {
	pubSubClient := &PubSubClient{
		Publisher:   pubSubRedisOptions.Publisher,
		Subscriber:  pubSubRedisOptions.Subscriber,
		subMap:      map[int64]*Listener{},
		subsRefsMap: map[string][]int64{},
		subId:       int64(0),
		PubSub:      pubSubRedisOptions.Subscriber.Subscribe(context.Background()),
		DropRun:     make(chan struct{}, 0),
		SolidOption: pubSubRedisOptions.SolidOption, // Key ttl
	}

	GoSafe(func() {
		pubSubClient.Run()
	})
	return pubSubClient
}

// Subscribe 订阅指定的频道，并返回订阅的ID
func (p *PubSubClient) Subscribe(client *Client, channel string, onMessage OnMessage) int64 {
	if p.subId >= math.MaxInt64 {
		p.subId = 0
	}
	atomic.AddInt64(&p.subId, 1)
	p.mu.Lock()
	defer func() {
		p.reSubscribe()
		p.mu.Unlock()
	}()

	p.subMap[p.subId] = &Listener{client, channel, onMessage}
	if _, ok := p.subsRefsMap[channel]; ok {
		p.subsRefsMap[channel] = append(p.subsRefsMap[channel], p.subId)
		return p.subId
	}

	p.subsRefsMap[channel] = []int64{p.subId}
	return p.subId
}

// UnSubscribe 取消订阅指定的ID
func (p *PubSubClient) UnSubscribe(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if listener, ok := p.subMap[id]; ok {
		channel := listener.Channel
		if subIds, ok := p.subsRefsMap[channel]; ok {
			for idx, idv := range subIds {
				// update subId slice
				if idv == id {
					p.subsRefsMap[channel] = append(p.subsRefsMap[channel][:idx], p.subsRefsMap[channel][idx+1:]...)
				}
			}

			// empty
			if len(p.subsRefsMap[channel]) == 0 {
				delete(p.subsRefsMap, channel)
				GoSafe(func() {
					p.reSubscribe()
				})
			}
		}
	}
	delete(p.subMap, id)
}

// Publish 发布消息到指定的频道
func (p *PubSubClient) Publish(ctx context.Context, channel string, message []byte) {
	p.Publisher.Publish(ctx, channel, message)
	//offline := &OffLine{
	//	ExpireTime: p.SolidOption.ExpireTime,
	//	Rdb:        p.Publisher,
	//	Key:        GenOfflineKey(channel),
	//}
	//offline.AddToOffline(ctx, message)
}

// reSubscribe 重新订阅所有相关频道
func (p *PubSubClient) reSubscribe() {
	channels := getKeys(p.subsRefsMap)
	p.PubSub.Close()
	p.PubSub = p.Subscriber.Subscribe(context.Background(), channels...)
	p.DropRun <- struct{}{}
}

// Run 启动一个goroutine来处理接收的PubSub消息
func (p *PubSubClient) Run() {
	for {
		select {
		case msg, ok := <-p.PubSub.Channel():
			if ok {
				channel := msg.Channel
				payLoad := msg.Payload

				if ids, ok := p.subsRefsMap[channel]; ok {
					for _, id := range ids {
						if listener := p.subMap[id]; ok {
							log.Printf("[ websocketSub - ] Current Push: [channel %v] [Event %v]", channel, payLoad) // 输入推入日志
							//listener.Client.Solid.Push(context.Background(), channel, []byte(payLoad))
							listener.OnMessage(listener.Client, []byte(payLoad))
						}
					}
				}
			}
		case <-p.DropRun:
			break
		}
	}
}

// getKeys 获取给定map中所有的key，并返回为一个字符串切片
func getKeys(m map[string][]int64) []string {
	var ret = []string{}
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}
