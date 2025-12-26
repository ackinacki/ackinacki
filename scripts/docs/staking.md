## Alert conditions

There are some specific cases that should be monitored because they indicate a potential problem with the staking process.

### Continuous script restarting

If the script restarts continuously, it indicates that the script is crashing, and staking log must be checked.

To catch such a condition, monitor the `bk_staking_startup`, if it is increasing for a considerable amount of time, then the script is likely crashing.

### Staking process stuck

If the staking process is stuck, it could be due to various reasons such as network issues, connectivity problems, or internal script errors.

In such cases, it is important to investigate the staking logs for any error messages or warnings that could provide clues about the root cause.

This can be observed by monitoring the `bk_staking_heartbeat` metric, which is reported at each iteration of the staking process main loop.

If the heartbeat is not increasing for a considerable amount of time, staking logs must be checked.

### Failed function calls

There are two approaches to monitoring failed function calls.

You should keep an eye on all metrics containing the word `_failed_` to detect any failures in function calls.

Alternatively, you can look at function start and done metrics and alert if the function was started but not finished successfully.

For example, if `bk_staking_place_stake` was updated, but `bk_staking_place_stake_done` was not updated for considerable time, then the function call either failed or hanged.

## Staking metrics

The `staking.sh` script reports various metrics to an OpenTelemetry (otel) collector using the `report_metric` function. All metric names are prefixed with `bk_staking_`.

Unless otherwise specified (these metrics are described in _italics_), the value reported for each metric is the current Unix timestamp.

### General Metrics

| Metric Name            | Description                                                                                        |
|------------------------|----------------------------------------------------------------------------------------------------|
| `bk_staking_startup`   | Reported when the script starts.                                                                   |
| `bk_staking_started`   | Reported after initial stake check, just before entering the main loop or executing a single pass. |
| `bk_staking_heartbeat` | Reported at the end of each `process_epoch` execution (in daemon mode or single pass).             |
| `bk_staking_exited`    | Reported when the script exits normally.                                                           |

### Staking Process Metrics (`process_epoch`)

| Metric Name                                 | Description                                                                |
|---------------------------------------------|----------------------------------------------------------------------------|
| `bk_staking_process_epoch`                  | Reported when the `process_epoch` function starts.                         |
| `bk_staking_stakes_count`                   | _Reports the current number of active stakes._                             |
| `bk_staking_process_epoch_touched_preepoch` | Reported when the preepoch is touched during processing.                   |
| `bk_staking_process_epoch_touched_epoch`    | Reported when the epoch is touched during processing.                      |
| `bk_staking_process_epoch_done`             | Reported when the `process_epoch` function finishes processing all stakes. |


### Stake Placement Metrics (`place_stake`)

| Metric Name                                   | Description                                                                                         |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `bk_staking_place_stake`                      | Reported when starting to place a new stake.                                                        |
| `bk_staking_place_stake_failed_licenses`      | Reported if no active licenses are found for staking.                                               |
| `bk_staking_place_stake_failed_balance`       | Reported if the wallet balance is insufficient for the minimum stake (for non-privileged licenses). |
| `bk_staking_place_stake_signer_index`         | _Reports the selected signer index for the stake request._                                          |
| `bk_staking_place_stake_failed_stake_request` | Reported if the stake request was sent but no new active stakes were detected afterwards.           |
| `bk_staking_place_stake_done`                 | Reported when a stake request is successfully accepted.                                             |

### Continue Stake Placement Metrics (`place_continue_stake`)

| Metric Name                                      | Description                                                                                       |
|--------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `bk_staking_place_continue_stake`                | Reported when starting to place a continue stake.                                                 |
| `bk_staking_place_continue_stake_failed_no_lic`  | Reported if no active licenses are found for continue staking.                                    |
| `bk_staking_place_continue_stake_failed_balance` | Reported if available tokens are insufficient for continue staking (for non-privileged licenses). |
| `bk_staking_place_continue_stake_signer_index`   | Reports the selected signer index for the continue stake request.                                 |
| `bk_staking_place_continue_stake_done`           | Reported when a continue stake request is successfully sent.                                      |

### Cooler Epoch Metrics (`process_cooler_epoch`)

| Metric Name                                  | Description                                                                                               |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `bk_staking_process_cooler_epoch`            | Reported when starting to process a cooler epoch.                                                         |
| `bk_staking_cooler_seqno_finish`             | _Reports the sequence number when the cooler epoch finishes._                                             |
| `bk_staking_cooler_seqno_current`            | _Reports the current block sequence number during cooler processing._                                     |
| `bk_staking_process_cooler_epoch_skip_seqno` | Reported if the cooler is skipped because the current sequence number hasn't reached the finish sequence. |
| `bk_staking_process_cooler_epoch_done`       | Reported when cooler epoch processing is complete.                                                        |

### BLS Key Update Metrics (`update_bls_keys`)

| Metric Name                                    | Description                                                                                     |
|------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `bk_staking_update_bls_keys`                   | Reported when starting to update BLS keys.                                                      |
| `bk_staking_update_bls_keys_failed_helper`     | Reported if the `node-helper` command is not found.                                             |
| `bk_staking_update_bls_keys_failed_key_update` | Reported if the BLS key update fails (e.g., key length didn't increase or public key is empty). |
| `bk_staking_update_bls_keys_done`              | Reported when BLS keys are successfully updated and the node is signaled.                       |

### Signal Handling Metrics (Graceful Shutdown)

| Metric Name                                                      | Description                                                                                                  |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `bk_staking_trigger_stopping_staking`                            | Reported when a SIGINT or SIGTERM signal is received.                                                        |
| `bk_staking_trigger_stopping_staking_seqnostart`                 | Reported if the script fails to find the sequence number to touch the epoch during shutdown.                 |
| `bk_staking_trigger_stopping_staking_done`                       | Reported when the epoch has been touched and the script is exiting.                                          |
| `bk_staking_trigger_stopping_continue_staking`                   | Reported when a SIGHUP signal is received to disable continue staking.                                       |
| `bk_staking_trigger_stopping_continue_staking_failed_seqnostart` | Reported if the script fails to find the sequence number for the current epoch during continue cancellation. |
| `bk_staking_trigger_stopping_continue_staking_not_continued`     | Reported if the current epoch is already not being continued.                                                |
| `bk_staking_trigger_stopping_continue_staking_done`              | Reported when the continue staking has been successfully cancelled.                                          |
