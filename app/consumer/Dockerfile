FROM golang:1.25.1

WORKDIR /app/consumer

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /my-app

EXPOSE 8081

CMD ["/my-app"]