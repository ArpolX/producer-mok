FROM golang:1.25
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev

WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o producer ./cmd/producer
CMD ["./producer"]