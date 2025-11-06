FROM golang:1.22-alpine
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN apk add --no-cache git
CMD ["/bin/sh", "-c", "echo 'container built' && sleep 3600"]
