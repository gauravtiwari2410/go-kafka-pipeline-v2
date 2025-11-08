# Use Go 1.23 (matches your go.mod requirement)
FROM golang:1.23-alpine

# Set working directory
WORKDIR /app

# Copy dependency files first (for better caching)
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the code
COPY . .

# Install git (needed for some Go modules)
RUN apk add --no-cache git

# Optional: set automatic Go toolchain if needed
ENV GOTOOLCHAIN=auto

# Simple test command (you can change later)
CMD ["/bin/sh", "-c", "echo 'container built' && sleep 3600"]
