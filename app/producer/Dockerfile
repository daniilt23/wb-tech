FROM golang:1.25.1

WORKDIR /app/producer

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /my-app

EXPOSE 8081

CMD ["/my-app"]