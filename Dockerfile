# Use an official Go runtime as the parent image
FROM golang:1.21.4-bullseye

# Set the working directory in the Docker container
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go app
RUN go build -o gateio_crypto_mining

# Run the Go executable when the container launches
CMD ["./gateio_crypto_mining"]
