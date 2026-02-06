#!/bin/bash

set -eu

container_id=$(sudo docker ps -q -a --filter "label=com.docker.compose.service=block_manager")

if [[ ! -z "$container_id" ]]; then
  pid=$(sudo docker inspect --format '{{.State.Pid}}' "$container_id")
  if ps -e | grep -q -e $pid; then
    sudo kill -SIGHUP $pid
    echo "[$(date -Is)]" "SIGHUP sent to $pid (BM)"
  else
    echo "[$(date -Is)]" "Couldn't find Block Manager container process"
    exit
  fi
else
  echo "[$(date -Is)]" "Couldn't find Block Manager container"
  exit
fi

container_id=$(sudo docker ps -q -a --filter "label=com.docker.compose.service=q_server_bm")

if [[ ! -z "$container_id" ]]; then
  pid=$(sudo docker inspect --format '{{.State.Pid}}' "$container_id")
  if ps -e | grep -q -e $pid; then
    sleep 1 # fast reopen
    sudo kill -SIGHUP $pid
    echo "[$(date -Is)]" "SIGHUP sent to $pid (QS)"
    sleep 5  # to be extra safe file rename + checkpointing + database recreation is done
    sudo kill -SIGHUP $pid
    echo "[$(date -Is)]" "SIGHUP sent again to $pid (QS)"
  else
    echo "[$(date -Is)]" "Couldn't find BM Q-Server container process"
    exit
  fi
else
  echo "[$(date -Is)]" "Couldn't find BM Q-Server container"
  exit
fi
