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
| `--jitter MODE`     | none    | `none` for a fixed delay, `full` for uniform(0, cap) |

## Why streaming matters here

A retry storm on a busy endpoint can produce a log with millions of lines for
a single request ID once you include everything retried against it over a
bad day. retrycheck reads its input with a line-buffered reader and only
ever keeps the current and previous attempt in memory - it never loads the
whole file. Piping a multi-gigabyte log through it costs the same memory as
piping a ten-line one.

## Status

Early. The policy model covers exponential backoff with an optional cap and
either no jitter or full jitter, which covers the common cases but not every
library's exact scheme. See the roadmap for what's next.

## License

MIT, see LICENSE.
