# 開発用イメージ
FROM golang:alpine

# ホットリロード用ツールのインストール
RUN go install github.com/air-verse/air@latest

# 作業ディレクトリの設定
WORKDIR /app

# 依存関係のコピーとダウンロード
COPY go.mod ./

RUN go mod download

# アプリケーションのコピー
COPY . . 

# airコマンドで起動
CMD ["air", "-c", ".air.toml"]