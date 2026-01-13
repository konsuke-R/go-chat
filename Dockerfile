# ===ビルド用イメージ===
FROM golang:alpine AS builder

# ホットリロード用ツールのインストール
RUN go install github.com/air-verse/air@latest

# 作業ディレクトリの設定
WORKDIR /app

# 依存関係のコピーとダウンロード
COPY go.mod go.sum ./
RUN go mod download

# アプリケーションのコピー
COPY . . 

RUN go build -o main main.go

# ===実行用ステージ===
FROM alpine:latest AS runner

WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/index.html .

# 実行
CMD ["./main"]