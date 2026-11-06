#!/usr/bin/env bash

COMPOSE_FILE="./metrics/docker-compose.yml"

if [[ "$1" = "start" ]]; then
  docker compose -f $COMPOSE_FILE  build
  docker compose -f $COMPOSE_FILE up -d
elif [[ "$1" = "restart" ]]; then
  docker compose -f $COMPOSE_FILE down
  sleep 2
  docker compose -f $COMPOSE_FILE build
  docker compose -f $COMPOSE_FILE up -d
else
  docker compose -f $COMPOSE_FILE down
fi