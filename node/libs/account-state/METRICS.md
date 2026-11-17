# Durable account batch metrics

These histograms measure whole batches processed by the durable account writer.
They have no additional labels and use seconds (`s`). All three receive one sample
together after a nonempty batch has been successfully processed. Failed batches
and batches still accumulating, queued or being written do not emit samples.

| OpenTelemetry metric | Interval |
| --- | --- |
| `node_state_accounts_batch_accumulation_duration` | Accumulator creation/reset to flush. The next accumulator starts at the same instant as the previous batch's flush. Includes time with no new changes. |
| `node_state_accounts_batch_queue_duration` | Flush to the start of writer processing. Includes channel backpressure, waiting for earlier batches and snapshot pin restrictions. |
| `node_state_accounts_batch_write_duration` | Start of writer processing to completion of the whole batch, including Merkle map persistence, archive writes and cleanup. Excludes queue and pin waits. |

The same measurements apply to early flushes before a snapshot pin, batches deferred
until a pin is released and final batches flushed during shutdown. Accumulation
and queue histogram boundaries extend to 3,600 seconds; write boundaries extend
to 30 seconds. Longer observations remain in the `+Inf` bucket and the sum.

The existing `node_state_accounts_aerospike_batch_write_duration` measures individual
Aerospike write calls in microseconds. Its final finite bucket is now 30,000,000
microseconds (30 seconds). A whole writer batch may make multiple such calls.

## Grafana queries

With standard Prometheus unit suffix translation, the new histogram sums are
exported as `<OpenTelemetry metric>_seconds_sum`. If your exporter preserves the
original names without unit suffixes, replace `_seconds_sum` with `_sum` below.
Apply the same node filters to both sides of the ratio.

Writer time divided by accumulation time over the last five minutes:

```promql
rate(node_state_accounts_batch_write_duration_seconds_sum[5m])
/
rate(node_state_accounts_batch_accumulation_duration_seconds_sum[5m])
```

A sustained value above 1 means processing completed batches takes longer than
forming them. This is a throughput comparison, not a wall-clock utilization
percentage: accumulation overlaps writing. Pin pauses can grow the queue even
when this ratio is below 1. Early partial batches are included. With no completed
batches in the window, the ratio is undefined; do not interpret it as zero load.

Average queue wait in seconds for batches completed in the last five minutes:

```promql
rate(node_state_accounts_batch_queue_duration_seconds_sum[5m])
/
rate(node_state_accounts_batch_queue_duration_seconds_count[5m])
```

During a pin, newly deferred batches are absent from these duration graphs until
they finish writing. Display `node_state_accounts_accumulator_deferred_batches`
alongside them to see the current number of deferred batches.
`node_state_accounts_accumulator_write_queue_blocks` shows blocks currently
accumulating in memory, rather than the number of batches waiting for the writer.
