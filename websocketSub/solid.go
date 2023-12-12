package websocketSub

import (
	"context"
	red "github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"time"
)

type (
	// SolidOption 定义Solid的选项类型
	SolidOption struct {
		ExpireTime time.Duration // Key的过期时间
		Duration   time.Duration // 重新发送消息的间隔时间
		Rdb        *red.Client
	}

	// Solid 定义Solid类型，用于处理WebSocket消息的订阅和发布
	Solid struct {
		Client     *Client
		ExpireTime time.Duration // Key的过期时间
		Duration   time.Duration // 重新发送消息的间隔时间
		Rdb        *red.Client
	}
)

// MustNewSolid 创建并返回一个Solid对象，参数为SolidOption对象和Client对象
func MustNewSolid(solidOption *SolidOption, client *Client) *Solid {
	return &Solid{
		Client:     client,
		ExpireTime: solidOption.ExpireTime,
		Duration:   solidOption.Duration,
		Rdb:        solidOption.Rdb,
	}
}

// PullOfflineMessage 从离线消息中拉取未处理的消息
func (s *Solid) PullOfflineMessage() {
	ctx := context.Background() // 创建一个空的上下文对象

	// 遍历s.Client.Channels中的每个channel
	for _, channel := range s.Client.Channels {
		rdb := s.Rdb               // 获取s的rdb值
		expireTime := s.ExpireTime // 获取s的expireTime值
		id := s.Client.Id          // 获取s.Client的id值

		// 创建一个Online对象
		online := &Online{
			Waiter: &Waiter{
				Key:        GenWaiterKey(channel, id), // 生成waiter的key值
				Rdb:        rdb,                       // 设置waiter的rdb值
				ExpireTime: expireTime,                // 设置waiter的expireTime值
			},
			Receiver: &Receiver{
				Key:        GenReceiverKey(channel, id), // 生成receiver的key值
				Rdb:        rdb,                         // 设置receiver的rdb值
				ExpireTime: expireTime,                  // 设置receiver的expireTime值
			},
			Offset: &Offset{
				Key:        GenOffsetKey(channel, id), // 生成offset的key值
				Rdb:        rdb,                       // 设置offset的rdb值
				ExpireTime: expireTime,                // 设置offset的expireTime值
			},
		}

		// 创建一个OffLine对象
		offline := &OffLine{
			ExpireTime: expireTime,             // 设置expireTime值
			Rdb:        rdb,                    // 设置rdb值
			Key:        GenOfflineKey(channel), // 生成offline的key值
		}

		// 调用OffLine的PullOffLine方法，传入Online对象作为参数
		offline.PullOffLine(ctx, online)
	}

}

// Push 将消息推送到指定的WebSocket通道
func (s *Solid) Push(ctx context.Context, channel string, event []byte) {
	rdb := s.Rdb
	expireTime := s.ExpireTime
	id := s.Client.Id
	online := &Online{
		Waiter: &Waiter{
			Key:        GenWaiterKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
		Receiver: &Receiver{
			Key:        GenReceiverKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
		Offset: &Offset{
			Key:        GenOffsetKey(channel, id),
			Rdb:        rdb,
			ExpireTime: expireTime,
		},
	}
	online.Waiter.Push(ctx, event)
}

// Ack 标记指定消息为已读
func (s *Solid) Ack(ctx context.Context, event *Event) {
	for _, channel := range s.Client.Channels {
		rdb := s.Rdb               // s.Rdb 表示 s 中的 Rdb 字段
		expireTime := s.ExpireTime // s.ExpireTime 表示 s 中的 ExpireTime 字段
		id := s.Client.Id          // s.Client.Id 表示 s 中的 Client 结构体的 Id 字段

		online := &Online{
			Waiter: &Waiter{
				Key:        GenWaiterKey(channel, id), // GenWaiterKey(channel, id) 函数表示生成 Waiter 结构体的 Key 字段的值
				Rdb:        rdb,                       // rdb 表示上述的 rdb 变量
				ExpireTime: expireTime,                // expireTime 表示上述的 expireTime 变量
			},
			Receiver: &Receiver{
				Key:        GenReceiverKey(channel, id), // GenReceiverKey(channel, id) 函数表示生成 Receiver 结构体的 Key 字段的值
				Rdb:        rdb,                         // rdb 表示上述的 rdb 变量
				ExpireTime: expireTime,                  // expireTime 表示上述的 expireTime 变量
			},
			Offset: &Offset{
				Key:        GenOffsetKey(channel, id), // GenOffsetKey(channel, id) 函数表示生成 Offset 结构体的 Key 字段的值
				Rdb:        rdb,                       // rdb 表示上述的 rdb 变量
				ExpireTime: expireTime,                // expireTime 表示上述的 expireTime 变量
			},
		}

		online.Ack(ctx, event)
	}
}

// MonitorReSend 监听指定频道并重新发送指定时间后未处理的消息
func (s *Solid) MonitorReSend() {
	if s.Duration == 0 {
		return
	}
	ticker := time.NewTicker(s.Duration)

	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			for _, channel := range s.Client.Channels {
				c := channel
				GoSafe(func() {
					rdb := s.Rdb
					expireTime := s.ExpireTime
					id := s.Client.Id

					online := &Online{
						Waiter: &Waiter{
							Key:        GenWaiterKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
						Receiver: &Receiver{
							Key:        GenReceiverKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
						Offset: &Offset{
							Key:        GenOffsetKey(c, id),
							Rdb:        rdb,
							ExpireTime: expireTime,
						},
					}
					ctx := context.Background()
					strings := online.Waiter.All(ctx)
					for _, item := range strings {
						str := item.(string)
						if s.IsFresh(str) {
							continue
						}
						s.Client.Send <- []byte(str)
					}
				})
			}
		}
	}
}

// IsFresh 检查消息是否为新鲜消息，即距离发送时间小于指定的间隔时间
func (s *Solid) IsFresh(data string) bool {
	var event Event
	now := time.Now().UnixMilli()
	err := jsoniter.Unmarshal([]byte(data), &event)
	if err != nil {
		return false
	}
	if now-s.Duration.Milliseconds() < event.Time {
		return true
	}
	return false
}
