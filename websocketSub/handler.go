package websocketSub

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
)

type (
	// OnMessageWrapper 包装了OnMessage和ChannelFun
	OnMessageWrapper struct {
		OnMessage  OnMessage
		ChannelFun ChannelFun
	}

	// SubScribeFuncs 用于存储订阅函数的映射
	SubScribeFuncs map[string]OnMessageWrapper

	// ChannelFun 是一个函数类型，用于处理Channel消息
	ChannelFun func(ctx context.Context, data []byte) string

	// GenUUIDFun 是一个函数类型，用于生成UUID
	GenUUIDFun func(r *http.Request) string

	// Event 是一个用于存储事件数据的结构体
	Event struct {
		Id        string `json:"Id"`        // 事件ID
		EventName string `json:"EventName"` // 事件名称
		Data      string `json:"Data"`      // 事件数据
		Time      int64  `json:"Time"`      // 事件时间
	}
)

var (
	newline        = []byte{'\n'}     // 换行符
	space          = []byte{' '}      // 空格
	subScribeFuncs = SubScribeFuncs{} // 存储订阅函数的映射
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024, // 缓冲区大小
	WriteBufferSize: 1024, // 缓冲区大小
	CheckOrigin: func(r *http.Request) bool { // 检查请求来源是否合法
		return true
	},
}

// AddWsEvent 用于添加WebSocket事件和处理函数
func AddWsEvent(eventName string, channelFun ChannelFun, onMessage OnMessage) {
	if _, ok := subScribeFuncs[eventName]; !ok { // 如果事件不存在于subScribeFuncs中
		subScribeFuncs[eventName] = OnMessageWrapper{ // 添加事件和处理函数到subScribeFuncs中
			OnMessage:  onMessage,
			ChannelFun: channelFun,
		}
	}
}

// ServeWs 用于处理WebSocket请求
func ServeWs(pubSubClient *PubSubClient, w http.ResponseWriter, r *http.Request, genUUIDFun GenUUIDFun) {
	fmt.Println("----------开始连接ws-----------") // 输出连接信息
	conn, err := upgrader.Upgrade(w, r, nil)   // 升级HTTP连接为WebSocket连接
	if err != nil {
		log.Printf("[ ws ServeWs upgrader.Upgrade 出现错误 error: %v ]", err)
		return
	}
	id := genUUIDFun(r)                                              // 生成UUID
	ctx := r.Context()                                               // 获取请求上下文
	client := MustNewClient(ctx, conn, id, pubSubClient.SolidOption) // 创建WebSocket客户端

	GoSafe(func() {
		client.ReadPump(pubSubClient) // 启动读取Pump
	})
	GoSafe(func() {
		client.writePump(pubSubClient) // 启动写入Pump
	})
	//GoSafe(func() {
	//	client.Solid.MonitorReSend() // 监听并重新发送消息
	//})

}
