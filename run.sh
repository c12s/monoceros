#!/bin/bash

# Check for required arguments
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 NODE_ID LISTEN_ADDR CONTACT_NODE_ID CONTACT_NODE_ADDR"
  exit 1
fi

NODE_ID=$1
LISTEN_ADDR=$2
CONTACT_NODE_ID=$3
CONTACT_NODE_ADDR=$4

# Export environment variables from .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo ".env file not found in cmd/"
  exit 1
fi

# Export required variables from arguments
export NODE_ID="$NODE_ID"
export LISTEN_ADDR="$LISTEN_ADDR"
export CONTACT_NODE_ID="$CONTACT_NODE_ID"
export CONTACT_NODE_ADDR="$CONTACT_NODE_ADDR"

# Run the program
go run ./cmd/main.go
