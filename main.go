package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	// 1. 8080ポートで待ち受け
	ln, _ := net.Listen("tcp", ":8080")
	fmt.Println("Server is running on :8080")

	for {
		// 2. 接続を受け入れる
		conn, _ := ln.Accept()
		
		// 3. 並行処理でハンドリング（ここで go を使うのがGo流！）
		go func(c net.Conn) {
			fmt.Fprintln(c, "Hi! What is your name?")
			name, _ := bufio.NewReader(c).ReadString('\n')
			fmt.Fprintf(c, "Nice to meet you, %s", name)
			c.Close()
		}(conn)
	}
}