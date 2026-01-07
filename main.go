package main

import (
	"log"
	"fmt"
	"net/http"
	"strings"
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/gorilla/websocket"
)

// WebSocketの設定
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {return true}, // 全てのドメインからの接続を許可
}

// 型定義: クライアントに送るメッセージ
type client struct {
	send chan string
	username string
}

var (
	// 新しく入ったクライアントを知らせるチャネル
	entering = make(chan client)
	// 出て行ったクライアントを知らせるチャネル
	leaving = make(chan client)
	// 全員に配るメッセージ用のチャネル
	messages = make(chan string)
	
	ctx = context.Background()
	rdb *redis.Client
)


func main() {

	// Redisに接続
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379", // docker-composeで指定したサービス名
	})

	// 管理人(ブロードキャスター)を1つの独立した並行処理として起動
	go broadcaster()

	// HTTPハンドラの設定
	http.HandleFunc("/ws", handleConnections)
	// 静的ファイル(index.html)をルートで表示
	http.Handle("/", http.FileServer(http.Dir(".")))

	fmt.Println("WebSocket Chat Server started on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func broadcaster() {
	// 接続されている全クライアントを管理
	clients := make(map[client]bool)

	for {
		select {
		case msg := <- messages:
			// Redisにログを保存(10件を維持)
			rdb.LPush(ctx, "chat_history", msg)
			rdb.LTrim(ctx, "chat_history", 0, 19)

			// 全員にメッセージを配信
			for cli := range clients {
				cli.send <- msg
			}
		case cli := <-entering:
			clients[cli] = true

			// 履歴を送信
			lastMsgs, _ := rdb.LRange(ctx, "chat_history", 0, 19).Result()
			for i := len(lastMsgs) - 1; i >= 0; i-- {
				cli.send <- "HISTORY: " + lastMsgs[i]
			}
			
		case cli := <- leaving:
			delete(clients, cli)
			close(cli.send)
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// HTTPをWebSocketにアップグレード
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ws.Close()

	ch := make(chan string)
	go clientWriter(ws, ch)

	// 最初は名無し
	username := "Anonymouse"
	cli := client{ch, username}
	entering <- cli

	for{
		_, msg, err := ws.ReadMessage()
		if err != nil {
			leaving <- cli
			break
		}
		text := string(msg)

		if strings.HasPrefix(text, "/nick ") {
			username = strings.TrimPrefix(text, "/nick ")
			messages <- "SYSTEM: User changed name to " + username
			continue
		}
		messages <- username + ": " + text
	}
}

func clientWriter(ws *websocket.Conn, ch <- chan string) {
	for msg := range ch {
		ws.WriteMessage(websocket.TextMessage, []byte(msg))
	}
}