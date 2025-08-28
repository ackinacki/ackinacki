#!/bin/bash

set -eu

container_id=$(sudo docker ps -q -a --filter "label=com.docker.compose.service=block_manager")

if [[ ! -z "$container_id" ]]; then
  pid=$(sudo docker inspect --format '{{.State.Pid}}' "$container_id")
  if ps -e | grep -q -e $pid; then
    sudo kill -SIGHUP $pid
    echo "[$(date -Is)]" "SIGHUP sent to $pid"
  else
    echo "[$(date -Is)]" "Couldn't find Block Manager container process"
  fi
else
  echo "[$(date -Is)]" "Couldn't find Block Manager container"
fi
