# Use an official Go runtime as the parent image
FROM golang:1.21.4-bullseye

# Set the working directory in the Docker container
WORKDIR /app

# Copy the Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Copy the run script
COPY run.sh .
RUN chmod +x run.sh


# Run the script when the container launches
CMD ["./run.sh"]
