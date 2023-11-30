#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status.

echo "Running downloader..."
go run downloader/downloader.go

echo "Running parser..."
go run parser/parser.go
