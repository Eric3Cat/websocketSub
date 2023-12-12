package websocketSub

import (
	"context"
	"fmt"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"log"
	"strconv"
	"time"
)

const (
	waiterPrefix   = "websocketSub:online:waiter:hash:%v:%v"   // 等待者键名模板
	receiverPrefix = "websocketSub:online:receiver:hash:%v:%v" // 接收者键名模板
	offsetPrefix   = "websocketSub:online:offset:%v:%v"        // 偏移量键名模板
)

type (
	// 在线状态结构体，包含等待者、接收者和偏移量信息
	Online struct {
		Offset   *Offset
		Waiter   *Waiter
		Receiver *Receiver
	}

	// 偏移量结构体，包含键名、Redis客户端和过期时间
	Offset struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // 键的过期时间
	}

	// 等待者结构体，包含键名、Redis客户端和过期时间
	Waiter struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // 键的过期时间
	}

	// 接收者结构体，包含键名、Redis客户端和过期时间
	Receiver struct {
		Key        string
		Rdb        *red.Client
		ExpireTime time.Duration // 键的过期时间
	}
)

// 确认消息，更新偏移量、删除等待者和接收者中的消息
func (r *Online) Ack(ctx context.Context, data *Event) {
	r.Waiter.Del(ctx, data)          // 从Waiter中删除数据
	r.Receiver.Received(ctx, data)   // 将数据接收
	r.Offset.UpdateOffset(ctx, data) // 更新偏移量

}

// 推送消息到等待者列表中的Redis
func (w *Waiter) Push(ctx context.Context, data []byte) {
	var event Event
	err := jsoniter.Unmarshal(data, &event)
	if err != nil {
		return
	}
	key := w.Key
	field := event.Id
	value := string(data)
	w.Rdb.HSet(ctx, key, field, value).Err()
	log.Printf("[ websocketSub - ] Current pull: [channel %v] [Event %v]", key, value) // 输入推出日志
	w.Rdb.Expire(ctx, w.Key, w.ExpireTime)
}

// 获取等待者列表中的所有消息
func (w *Waiter) All(ctx context.Context) []interface{} {
	strings := []interface{}{}
	result, err := w.Rdb.HGetAll(ctx, w.Key).Result()
	if err != nil {
		return strings
	}
	for _, item := range result {
		strings = append(strings, item)
	}

	Sort(strings, func(a, b interface{}) int {
		var s1 Event
		var s2 Event
		jsoniter.Unmarshal([]byte(a.(string)), &s1)
		jsoniter.Unmarshal([]byte(b.(string)), &s2)
		return int(s1.Time - s2.Time)
	})

	return strings
}

// 删除等待者列表中的消息
func (w *Waiter) Del(ctx context.Context, data *Event) {
	w.Rdb.HDel(ctx, w.Key, data.Id)
}

// 接收到消息时更新接收者列表中的消息
func (r *Receiver) Received(ctx context.Context, data *Event) {
	byteData, err := jsoniter.Marshal(data)
	if err != nil {
		return
	}
	r.Rdb.HSet(ctx, r.Key, data.Id, string(byteData))
	r.Rdb.Expire(ctx, r.Key, r.ExpireTime)
}

// 判断是否已接收到消息
func (r *Receiver) IsReceived(ctx context.Context, data []byte) bool {
	var event Event
	err := jsoniter.Unmarshal(data, &event)
	if err != nil {
		return false
	}
	result, err := r.Rdb.HGet(ctx, r.Key, event.Id).Result()
	if err != nil {
		return false
	}

	if result != "" {
		return true
	}

	return false
}

// 更新偏移量
func (o *Offset) UpdateOffset(ctx context.Context, data *Event) {
	old := o.Rdb.Get(ctx, o.Key).Val()
	oldTime, err := strconv.Atoi(old)
	if err != nil {
		o.Rdb.Set(ctx, o.Key, strconv.Itoa(int(data.Time)), o.ExpireTime)
		return
	}
	if int64(oldTime) > data.Time {
		return
	}
	o.Rdb.Set(ctx, o.Key, strconv.Itoa(int(data.Time)), o.ExpireTime)
}

// 获取偏移量
func (o *Offset) Offset(ctx context.Context) int64 {
	result, err := o.Rdb.Get(ctx, o.Key).Result()
	if err != nil {
		return 0
	}
	convResult, err := strconv.Atoi(result)
	if err != nil {
		return 0
	}
	return int64(convResult)
}

// 生成等待者键名
func GenWaiterKey(channel, id string) string {
	return fmt.Sprintf(waiterPrefix, channel, id)
}

// 生成接收者键名
func GenReceiverKey(channel, id string) string {
	return fmt.Sprintf(receiverPrefix, channel, id)
}

// 生成偏移量键名
func GenOffsetKey(channel, id string) string {
	return fmt.Sprintf(offsetPrefix, channel, id)
}
