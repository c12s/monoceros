#!/bin/bash

# Check for required arguments
if [ "$#" -ne 8 ]; then
  echo "Usage: $0 NODE_ID RN_LISTEN_ADDR GN_LISTEN_ADDR RRN_LISTEN_ADDR RN_CONTACT_NODE_ID RN_CONTACT_NODE_ADDR GN_CONTACT_NODE_ID GN_CONTACT_NODE_ADDR"
  exit 1
fi

NODE_ID=$1

RN_LISTEN_ADDR=$2
GN_LISTEN_ADDR=$3
RRN_LISTEN_ADDR=$4

RN_CONTACT_NODE_ID=$5
RN_CONTACT_NODE_ADDR=$6

GN_CONTACT_NODE_ID=$7
GN_CONTACT_NODE_ADDR=$8

# Export environment variables from .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo ".env file not found in cmd/"
  exit 1
fi

# Export required variables from arguments
export NODE_ID="$NODE_ID"

export RN_LISTEN_ADDR="$RN_LISTEN_ADDR"
export GN_LISTEN_ADDR="$GN_LISTEN_ADDR"
export RRN_LISTEN_ADDR="$RRN_LISTEN_ADDR"

export RN_CONTACT_NODE_ID="$RN_CONTACT_NODE_ID"
export RN_CONTACT_NODE_ADDR="$RN_CONTACT_NODE_ADDR"

export GN_CONTACT_NODE_ID="$GN_CONTACT_NODE_ID"
export GN_CONTACT_NODE_ADDR="$GN_CONTACT_NODE_ADDR"

# Run the program
go run ./cmd/main.go
