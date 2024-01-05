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
	offlinePrefix = "websocketSub:offline:zset:%v" // 离线用户数据的键名模板
)

type (
	OffLine struct {
		ExpireTime time.Duration // 键的过期时间
		Rdb        *red.Client
		Key        string
	}
)

func (o *OffLine) AddToOffline(ctx context.Context, data []byte) {
	var event Event
	err := jsoniter.Unmarshal(data, &event)

	if err == nil {
		o.Rdb.ZAdd(ctx, o.Key, &red.Z{
			Score:  float64(event.Time), // 设置离线事件的时间为分数
			Member: string(data),        // 将离线事件的数据作为成员添加到离线用户集合中
		})
		o.Rdb.Expire(ctx, o.Key, o.ExpireTime) // 设置离线用户集合的过期时间
		log.Printf("[ ws AddToOffline key:%v,value:%v ]", o.Key, string(data))
	} else {
		log.Printf("[ ws AddToOffline 无法成功添加 出现错误 error: %v ]", err)
	}
}

func (o *OffLine) MessageByOffset(ctx context.Context, offset int64) ([]string, error) {
	max := time.Now().UnixMilli()
	result, err := o.Rdb.ZRangeByScore(ctx, o.Key, &red.ZRangeBy{
		Min:    strconv.Itoa(int(offset)), // 设置最小分数为偏移量
		Max:    strconv.Itoa(int(max)),    // 设置最大分数为当前时间
		Offset: 0,                         // 返回结果的起始位置
		Count:  1<<63 - 1,                 // 返回结果的数量
	}).Result()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (o *OffLine) PullOffLine(ctx context.Context, online *Online) {
	offset := online.Offset.Offset(ctx)
	datas, err := o.MessageByOffset(ctx, offset)
	if err != nil {
		return
	}
	if datas != nil && len(datas) > 0 {
		for _, item := range datas {
			event := []byte(item)
			if !online.Receiver.IsReceived(ctx, event) { // 如果接收方未收到离线事件，则将其推入等待者的消息队列中
				online.Waiter.Push(ctx, event)
			}
		}
	}
}

func GenOfflineKey(channel string) string {
	return fmt.Sprintf(offlinePrefix, channel) // 根据频道生成离线用户数据的键名
}
