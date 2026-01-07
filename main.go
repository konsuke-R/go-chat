package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// 型定義: クライアントに送るメッセージ
type client struct {
	chanName chan<- string // 送信専用チャネル
	username string
}

var (
	// 新しく入ったクライアントを知らせるチャネル
	entering = make(chan client)
	// 出て行ったクライアントを知らせるチャネル
	leaving = make(chan client)
	// 全員に配るメッセージ用のチャネル
	messages = make(chan string)
)


func main() {
	// 8080ポートで待ち受け
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	
	fmt.Println("Server is running on :8080")

	// 管理人(ブロードキャスター)を1つの独立した並行処理として起動
	go broadcaster()

	for {
		// 接続を受け入れる
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		
		// クライアントごとの並行処理を開始
		go handleConn(conn)
	}
}

func broadcaster() {
	// 接続されている全クライアントを管理
	clients := make(map[client]bool)

	for {
		select {
		case msg := <- messages:
			// 全員にメッセージを配信
			for cli := range clients {
				cli.chanName <- msg
			}
		case cli := <-entering:
			clients[cli] = true
		case cli := <- leaving:
			delete(clients, cli)
			close(cli.chanName)
		}
	}
}

func handleConn(conn net.Conn) {
	ch := make(chan string) // 個別のクライアント用のチャネル
	// クライアントへの送信専用Goroutineを起動
	go clientWriter(conn, ch)

	// 最初に入力された文字列を「名前」にする
	fmt.Fprint(conn, "Enter your name: ")
	input := bufio.NewScanner(conn)
	var username string
	if input.Scan() {
		username = input.Text()
	}

	cli := client{ch, username}
	ch <- "Welcome, " + username + "!"
	messages <- username + " has joined the room"
	entering <- cli

	for input.Scan() {
		text := input.Text()

		// コマンド処理の例
		if strings.HasPrefix(text, "/nick ") {
			newNick := strings.TrimPrefix(text, "/nick ")
			messages <- fmt.Sprintf("SYSTEM: %s changed name to %s", username, newNick)
			username = newNick
			// 本来はboradcaster側のmapも更新する必要がある
			continue
		}

		messages <- username + ": " + text
	}

	leaving <- cli
	messages <- username + " has left"
	conn.Close()
}

func clientWriter(conn net.Conn, ch <- chan string) {
	for msg := range ch {
		fmt.Fprintln(conn, msg)
	}
}