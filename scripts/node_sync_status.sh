#!/usr/bin/env bash 

LOG_FILE=$1

  if [ ! -e $LOG_FILE ]; then
    echo Log file $LOG_FILE not found.
    exit 1
  fi

NODE_BLOCK_TIME=$(grep -Po "Last finalized block data: seq_no: ([0-9]*).*time: \K[0-9]{10}" $1 | tail -n 1)
CURRENT_TIME=$(date +%s)
TIME_DIFF_SECOND=$(( CURRENT_TIME-NODE_BLOCK_TIME ))
TIME_DIFF=$(echo "$TIME_DIFF_SECOND / 3600" | bc)

echo "Synced up to $(echo $NODE_BLOCK_TIME | xargs -I {} date +"%Y-%m-%d %H:%M:%S" -d @{}) ($TIME_DIFF hours behind real time)"
