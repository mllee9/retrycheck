# retrycheck

Retry policies drift from what's actually running. Someone tunes the base
delay in code, forgets to update the docs, or a config value gets rolled back
in one service but not another, and now you have a client hammering a
downstream dependency on a schedule nobody signed off on. The usual way to
notice is an incident.

retrycheck answers one question: given a log of retry attempts and the policy
they're supposed to follow, did they follow it? It reads one attempt per
line, checks the gap before each attempt against what the policy prescribes,
and prints every place they disagree.

## Input format

One attempt per line: `timestamp_ms,outcome`, where `outcome` is `ok` or
`err`. Blank lines and lines starting with `#` are ignored.

```
# checkout-service, order 88213
1717000000000,err
1717000000101,err
1717000000305,err
1717000000709,ok
```

This is meant to be produced by grepping a request ID out of your logs and
reshaping it, not written by hand. A shell one-liner or a small script that
turns structured logs into this format is enough; retrycheck deliberately
doesn't parse your log format for you.

## Grouped mode

A single request's retry history is the common case, but a log aggregator
usually hands you an interleaved stream covering many requests at once. Pass
`--grouped` and add a request id as the middle field:

```
$ cat many-requests.log
1717000000000,order-88213,err
1717000000000,order-91004,err
1717000000101,order-88213,err
1717000000230,order-91004,err
1717000000305,order-88213,ok

$ retrycheck --grouped --base-delay-ms 100 --multiplier 2 many-requests.log
line 4: [order-91004] attempt 2 waited 230ms, policy expected ~100ms
order-88213: 3 attempts, 305ms elapsed, 0 violation(s)
order-91004: 2 attempts, 230ms elapsed, 1 violation(s)
5 attempts across 2 request(s), 1 violation(s)
```

Each request id gets its own attempt count, first-seen timestamp, and success
state, so one id's retries never affect another's, no matter how the lines
are interleaved. State is kept per request id rather than per line, so memory
use tracks the number of requests in flight, not the length of the stream.

## JSON output

Pass `--format json` to get line-delimited JSON instead of the human-readable
report, one object per line: a `violation` object as each one is found,
followed by a closing `summary` (or `group_summary` per request id, in
grouped mode). This is meant for feeding a dashboard or another script, not
for reading in a terminal.

```
$ retrycheck --format json --base-delay-ms 100 --multiplier 2 bad.log
{"type":"violation","kind":"delay_mismatch","line":2,"attempt":2,"observed_ms":50,"expected_min_ms":100,"expected_max_ms":100}
{"type":"violation","kind":"delay_mismatch","line":3,"attempt":3,"observed_ms":850,"expected_min_ms":200,"expected_max_ms":200}
{"type":"summary","attempts":3,"elapsed_ms":900,"violations":2}
```

`violation` objects always carry `kind`, `line`, and the fields relevant to
that kind (`already_succeeded`, `max_attempts_exceeded`,
`timestamp_out_of_order`, or `delay_mismatch`). Grouped mode adds a
`request_id` field to every object. Like text mode, this is emitted as each
line is processed, not buffered and dumped at the end.

## Usage

```
$ retrycheck --base-delay-ms 100 --multiplier 2 --max-attempts 5 attempts.log
4 attempts, 709ms elapsed, 0 violation(s)
```

A policy that isn't being followed:

```
$ cat bad.log
1717000000000,err
1717000000050,err
1717000000900,ok

$ retrycheck --base-delay-ms 100 --multiplier 2 bad.log
line 2: attempt 2 waited 50ms, policy expected ~100ms
line 3: attempt 3 waited 850ms, policy expected ~200ms
3 attempts, 900ms elapsed, 2 violation(s)
```

With no file given, retrycheck reads stdin, so it fits into a pipeline:

```
$ ./extract-attempts.sh order-88213 | retrycheck --jitter full --base-delay-ms 200
```

Exit code is 0 if the log matches the policy, 1 if it found violations, 2 on
a usage or parse error.

### Options

| Flag               | Default | Meaning                                      |
|---------------------|---------|-----------------------------------------------|
| `--max-attempts N`  | 5       | attempts allowed before the policy gives up   |
| `--base-delay-ms N` | 100     | delay before the second attempt               |
| `--multiplier X`    | 2.0     | growth factor applied per attempt             |
| `--max-delay-ms N`  | 30000   | cap on the computed delay                     |
| `--jitter MODE`     | none    | `none`, `full`, `equal`, or `decorrelated` (see below) |
| `--grouped`         | off     | expect `timestamp_ms,request_id,outcome` and check each request id on its own |
| `--format MODE`     | text    | `text` for the human-readable report, `json` for line-delimited JSON |

## Jitter modes

`--jitter` controls how a single deterministic delay per attempt (`none`)
turns into a range, and what counts as compliant within that range:

* `none` - the delay must match the computed value, within a 5ms tolerance
  for clock and scheduler skew.
* `full` - delay is uniform(0, cap), so any gap up to the cap is accepted.
* `equal` - delay is uniform(cap/2, cap): half the cap is fixed, the other
  half is jittered. Accepted range is `[cap/2, cap]`.
* `decorrelated` - each delay is uniform(base_delay_ms, previous_delay * 3),
  capped at max_delay_ms. Unlike the other modes, the accepted range for one
  attempt depends on the delay actually observed before the previous one, not
  on the attempt number by itself, so retrycheck carries the last observed
  gap forward as it walks the stream.

For `full`, `equal`, and `decorrelated`, a `delay_mismatch` violation reports
`expected_min_ms` and `expected_max_ms` instead of a single value, since
there's no single correct delay to compare against.

## Why streaming matters here

A retry storm on a busy endpoint can produce a log with millions of lines for
a single request ID once you include everything retried against it over a
bad day. retrycheck reads its input with a line-buffered reader and only
ever keeps the current and previous attempt in memory - it never loads the
whole file. Piping a multi-gigabyte log through it costs the same memory as
piping a ten-line one.

## Status

Early. The policy model covers exponential backoff with an optional cap and
four jitter modes (none, full, equal, decorrelated), which covers most
libraries' backoff schemes. See the roadmap for what's next.

## License

MIT, see LICENSE.
