//! retrycheck answers one question: does this log of retry attempts follow the
//! backoff policy it claims to follow?
//!
//! It reads one attempt per line (`timestamp_ms,outcome`) from a file or stdin and
//! walks it line by line, never holding more than the current and previous attempt
//! in memory. That matters because the intended input is a request's full retry
//! history pulled from a log aggregator, which can run to millions of lines for a
//! busy endpoint having a bad day.

mod policy;

use policy::{Jitter, RetryPolicy};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::process::ExitCode;

struct Args {
    policy: RetryPolicy,
    input_path: Option<String>,
    grouped: bool,
}

fn print_usage() {
    eprintln!(
        "usage: retrycheck [options] [file]\n\
\n\
Reads one retry attempt per line as `timestamp_ms,outcome` (outcome is \"ok\" or\n\
\"err\") and checks the gaps between attempts against the given backoff policy.\n\
Reads from stdin if no file is given. Lines that are blank or start with '#' are\n\
skipped.\n\
\n\
With --grouped, lines carry a request id as the middle field:\n\
`timestamp_ms,request_id,outcome`. Each request id is checked against the\n\
policy independently, so one file can hold an interleaved stream of many\n\
retry sequences.\n\
\n\
options:\n\
  --max-attempts N     stop attempts allowed before giving up (default 5)\n\
  --base-delay-ms N    delay before the second attempt (default 100)\n\
  --multiplier X       growth factor applied per attempt (default 2.0)\n\
  --max-delay-ms N     cap on the computed delay (default 30000)\n\
  --jitter MODE        \"none\" or \"full\" (default none)\n\
  --grouped            expect a request id column and check ids independently\n\
  -h, --help            print this message"
    );
}

fn parse_args() -> Result<Args, String> {
    let mut max_attempts: u32 = 5;
    let mut base_delay_ms: u64 = 100;
    let mut multiplier: f64 = 2.0;
    let mut max_delay_ms: u64 = 30_000;
    let mut jitter = Jitter::None;
    let mut input_path: Option<String> = None;
    let mut grouped = false;

    let mut argv = env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--max-attempts" => max_attempts = next_value(&mut argv, &arg)?.parse().map_err(|_| format!("{arg} expects an integer"))?,
            "--base-delay-ms" => base_delay_ms = next_value(&mut argv, &arg)?.parse().map_err(|_| format!("{arg} expects an integer"))?,
            "--multiplier" => multiplier = next_value(&mut argv, &arg)?.parse().map_err(|_| format!("{arg} expects a number"))?,
            "--max-delay-ms" => max_delay_ms = next_value(&mut argv, &arg)?.parse().map_err(|_| format!("{arg} expects an integer"))?,
            "--jitter" => {
                jitter = match next_value(&mut argv, &arg)?.as_str() {
                    "none" => Jitter::None,
                    "full" => Jitter::Full,
                    other => return Err(format!("unknown jitter mode '{other}', expected 'none' or 'full'")),
                }
            }
            "--grouped" => grouped = true,
            other if other.starts_with('-') => return Err(format!("unknown option '{other}'")),
            other => {
                if input_path.is_some() {
                    return Err("only one input file may be given".to_string());
                }
                input_path = Some(other.to_string());
            }
        }
    }

    Ok(Args {
        policy: RetryPolicy {
            max_attempts,
            base_delay_ms,
            multiplier,
            max_delay_ms,
            jitter,
        },
        input_path,
        grouped,
    })
}

fn next_value(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    argv.next().ok_or_else(|| format!("{flag} expects a value"))
}

/// One parsed line of input: a timestamp and whether the attempt succeeded.
struct Attempt {
    timestamp_ms: u64,
    ok: bool,
}

fn parse_line(line: &str) -> Result<Option<Attempt>, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }
    let (ts_str, outcome_str) = trimmed
        .split_once(',')
        .ok_or_else(|| format!("expected 'timestamp_ms,outcome', got '{trimmed}'"))?;
    let timestamp_ms: u64 = ts_str
        .trim()
        .parse()
        .map_err(|_| format!("bad timestamp '{}'", ts_str.trim()))?;
    let ok = match outcome_str.trim() {
        "ok" => true,
        "err" => false,
        other => return Err(format!("bad outcome '{other}', expected 'ok' or 'err'")),
    };
    Ok(Some(Attempt { timestamp_ms, ok }))
}

/// Walks the attempt stream and checks each gap against the policy, printing
/// violations as they're found. Returns the number of violations seen.
fn check<R: BufRead>(reader: R, policy: &RetryPolicy) -> Result<u32, String> {
    let mut attempt_num: u32 = 0;
    let mut prev_timestamp_ms: Option<u64> = None;
    let mut first_timestamp_ms: Option<u64> = None;
    let mut succeeded_at: Option<u32> = None;
    let mut violations: u32 = 0;

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("read error: {e}"))?;
        let attempt = match parse_line(&line) {
            Ok(Some(a)) => a,
            Ok(None) => continue,
            Err(e) => return Err(format!("line {}: {e}", line_no + 1)),
        };

        attempt_num += 1;
        first_timestamp_ms.get_or_insert(attempt.timestamp_ms);

        if let Some(finished_at) = succeeded_at {
            println!(
                "line {}: attempt {attempt_num} fired after attempt {finished_at} already succeeded",
                line_no + 1
            );
            violations += 1;
        }

        if attempt_num > policy.max_attempts {
            println!(
                "line {}: attempt {attempt_num} exceeds the policy's max of {}",
                line_no + 1,
                policy.max_attempts
            );
            violations += 1;
        }

        if let Some(prev) = prev_timestamp_ms {
            if attempt.timestamp_ms < prev {
                println!(
                    "line {}: timestamp {} is before the previous attempt's {}",
                    line_no + 1,
                    attempt.timestamp_ms,
                    prev
                );
                violations += 1;
            } else {
                let gap = attempt.timestamp_ms - prev;
                if !policy.accepts_gap(attempt_num, gap) {
                    println!(
                        "line {}: attempt {attempt_num} waited {}ms, policy expected ~{}ms",
                        line_no + 1,
                        gap,
                        policy.expected_delay_ms(attempt_num)
                    );
                    violations += 1;
                }
            }
        }

        if attempt.ok {
            succeeded_at = Some(attempt_num);
        }
        prev_timestamp_ms = Some(attempt.timestamp_ms);
    }

    if let (Some(first), Some(last)) = (first_timestamp_ms, prev_timestamp_ms) {
        println!(
            "{attempt_num} attempts, {}ms elapsed, {violations} violation(s)",
            last - first
        );
    } else {
        println!("no attempts found in input");
    }

    Ok(violations)
}

/// One parsed line of grouped input: a timestamp, the request id it belongs
/// to, and whether the attempt succeeded.
struct GroupedAttempt {
    timestamp_ms: u64,
    request_id: String,
    ok: bool,
}

fn parse_grouped_line(line: &str) -> Result<Option<GroupedAttempt>, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }
    let expected = || format!("expected 'timestamp_ms,request_id,outcome', got '{trimmed}'");
    let (ts_str, rest) = trimmed.split_once(',').ok_or_else(expected)?;
    let (request_id, outcome_str) = rest.split_once(',').ok_or_else(expected)?;
    let timestamp_ms: u64 = ts_str
        .trim()
        .parse()
        .map_err(|_| format!("bad timestamp '{}'", ts_str.trim()))?;
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err("request id must not be empty".to_string());
    }
    let ok = match outcome_str.trim() {
        "ok" => true,
        "err" => false,
        other => return Err(format!("bad outcome '{other}', expected 'ok' or 'err'")),
    };
    Ok(Some(GroupedAttempt {
        timestamp_ms,
        request_id: request_id.to_string(),
        ok,
    }))
}

/// Per-request-id state carried across an interleaved stream, so one request
/// id's attempt count and timing never leaks into another's.
struct GroupState {
    attempt_num: u32,
    prev_timestamp_ms: Option<u64>,
    first_timestamp_ms: u64,
    succeeded_at: Option<u32>,
    violations: u32,
}

/// Same walk as `check`, but keyed by request id so a single interleaved
/// stream covering many concurrent retry sequences can be checked in one
/// pass. Memory use grows with the number of distinct request ids in flight,
/// not with the length of the stream.
fn check_grouped<R: BufRead>(reader: R, policy: &RetryPolicy) -> Result<u32, String> {
    let mut groups: HashMap<String, GroupState> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut total_attempts: u32 = 0;

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("read error: {e}"))?;
        let attempt = match parse_grouped_line(&line) {
            Ok(Some(a)) => a,
            Ok(None) => continue,
            Err(e) => return Err(format!("line {}: {e}", line_no + 1)),
        };

        total_attempts += 1;
        let req = attempt.request_id.clone();
        let state = groups.entry(req.clone()).or_insert_with(|| {
            order.push(req.clone());
            GroupState {
                attempt_num: 0,
                prev_timestamp_ms: None,
                first_timestamp_ms: attempt.timestamp_ms,
                succeeded_at: None,
                violations: 0,
            }
        });

        state.attempt_num += 1;

        if let Some(finished_at) = state.succeeded_at {
            println!(
                "line {}: [{req}] attempt {} fired after attempt {finished_at} already succeeded",
                line_no + 1,
                state.attempt_num
            );
            state.violations += 1;
        }

        if state.attempt_num > policy.max_attempts {
            println!(
                "line {}: [{req}] attempt {} exceeds the policy's max of {}",
                line_no + 1,
                state.attempt_num,
                policy.max_attempts
            );
            state.violations += 1;
        }

        if let Some(prev) = state.prev_timestamp_ms {
            if attempt.timestamp_ms < prev {
                println!(
                    "line {}: [{req}] timestamp {} is before the previous attempt's {}",
                    line_no + 1,
                    attempt.timestamp_ms,
                    prev
                );
                state.violations += 1;
            } else {
                let gap = attempt.timestamp_ms - prev;
                if !policy.accepts_gap(state.attempt_num, gap) {
                    println!(
                        "line {}: [{req}] attempt {} waited {}ms, policy expected ~{}ms",
                        line_no + 1,
                        state.attempt_num,
                        gap,
                        policy.expected_delay_ms(state.attempt_num)
                    );
                    state.violations += 1;
                }
            }
        }

        if attempt.ok {
            state.succeeded_at = Some(state.attempt_num);
        }
        state.prev_timestamp_ms = Some(attempt.timestamp_ms);
    }

    let mut total_violations: u32 = 0;
    for req in &order {
        let state = &groups[req];
        let elapsed = state.prev_timestamp_ms.unwrap_or(state.first_timestamp_ms) - state.first_timestamp_ms;
        println!(
            "{req}: {} attempts, {elapsed}ms elapsed, {} violation(s)",
            state.attempt_num, state.violations
        );
        total_violations += state.violations;
    }

    if order.is_empty() {
        println!("no attempts found in input");
    } else {
        println!(
            "{total_attempts} attempts across {} request(s), {total_violations} violation(s)",
            order.len()
        );
    }

    Ok(total_violations)
}

fn run() -> Result<u32, String> {
    let args = parse_args()?;
    match args.input_path {
        Some(path) => {
            let file = File::open(&path).map_err(|e| format!("can't open '{path}': {e}"))?;
            let reader = BufReader::new(file);
            if args.grouped {
                check_grouped(reader, &args.policy)
            } else {
                check(reader, &args.policy)
            }
        }
        None => {
            let stdin = io::stdin();
            let reader = stdin.lock();
            if args.grouped {
                check_grouped(reader, &args.policy)
            } else {
                check(reader, &args.policy)
            }
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(0) => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(1),
        Err(e) => {
            eprintln!("retrycheck: {e}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            base_delay_ms: 100,
            multiplier: 2.0,
            max_delay_ms: 30_000,
            jitter: Jitter::None,
        }
    }

    fn check_str(input: &str, policy: &RetryPolicy) -> u32 {
        check(Cursor::new(input.as_bytes()), policy).expect("check should not error on valid input")
    }

    #[test]
    fn parse_line_skips_blank_and_comment_lines() {
        assert!(parse_line("").unwrap().is_none());
        assert!(parse_line("   ").unwrap().is_none());
        assert!(parse_line("# checkout-service, order 88213").unwrap().is_none());
    }

    #[test]
    fn parse_line_reads_a_valid_attempt() {
        let attempt = parse_line("1717000000101,err").unwrap().unwrap();
        assert_eq!(attempt.timestamp_ms, 1717000000101);
        assert!(!attempt.ok);

        let attempt = parse_line("1717000000709,ok").unwrap().unwrap();
        assert!(attempt.ok);
    }

    #[test]
    fn parse_line_rejects_malformed_input() {
        assert!(parse_line("not-a-line").is_err());
        assert!(parse_line("abc,err").is_err());
        assert!(parse_line("1717000000101,maybe").is_err());
    }

    #[test]
    fn check_reports_no_violations_for_a_compliant_log() {
        let log = "1717000000000,err\n1717000000101,err\n1717000000305,err\n1717000000709,ok\n";
        assert_eq!(check_str(log, &policy()), 0);
    }

    #[test]
    fn check_flags_a_gap_that_is_too_short_or_too_long() {
        let log = "1717000000000,err\n1717000000050,err\n1717000000900,ok\n";
        assert_eq!(check_str(log, &policy()), 2);
    }

    #[test]
    fn check_flags_attempts_past_max_attempts() {
        let mut p = policy();
        p.max_attempts = 2;
        let log = "1717000000000,err\n1717000000100,err\n1717000000300,ok\n";
        assert_eq!(check_str(log, &p), 1);
    }

    #[test]
    fn check_flags_an_attempt_after_success() {
        let log = "1717000000000,err\n1717000000100,ok\n1717000000300,err\n";
        assert_eq!(check_str(log, &policy()), 1);
    }

    #[test]
    fn check_flags_out_of_order_timestamps() {
        let log = "1717000000100,err\n1717000000000,err\n";
        assert_eq!(check_str(log, &policy()), 1);
    }

    #[test]
    fn check_ignores_blank_lines_and_comments() {
        let log = "# order 88213\n1717000000000,err\n\n1717000000101,err\n";
        assert_eq!(check_str(log, &policy()), 0);
    }

    #[test]
    fn check_returns_zero_violations_for_empty_input() {
        assert_eq!(check_str("", &policy()), 0);
    }

    fn check_grouped_str(input: &str, policy: &RetryPolicy) -> u32 {
        check_grouped(Cursor::new(input.as_bytes()), policy)
            .expect("check_grouped should not error on valid input")
    }

    #[test]
    fn parse_grouped_line_reads_a_valid_attempt() {
        let attempt = parse_grouped_line("1717000000101,order-88213,err").unwrap().unwrap();
        assert_eq!(attempt.timestamp_ms, 1717000000101);
        assert_eq!(attempt.request_id, "order-88213");
        assert!(!attempt.ok);
    }

    #[test]
    fn parse_grouped_line_rejects_a_missing_request_id() {
        assert!(parse_grouped_line("1717000000101,,err").is_err());
    }

    #[test]
    fn parse_grouped_line_rejects_a_missing_column() {
        assert!(parse_grouped_line("1717000000101,err").is_err());
    }

    #[test]
    fn check_grouped_tracks_interleaved_requests_independently() {
        let log = "\
1717000000000,a,err\n\
1717000000000,b,err\n\
1717000000101,a,err\n\
1717000000101,b,err\n\
1717000000305,a,ok\n\
1717000000305,b,ok\n";
        assert_eq!(check_grouped_str(log, &policy()), 0);
    }

    #[test]
    fn check_grouped_flags_a_violation_in_only_the_request_that_has_it() {
        let log = "\
1717000000000,a,err\n\
1717000000050,a,err\n\
1717000000000,b,err\n\
1717000000101,b,err\n";
        assert_eq!(check_grouped_str(log, &policy()), 1);
    }

    #[test]
    fn check_grouped_counts_attempts_per_request_not_globally() {
        let mut p = policy();
        p.max_attempts = 1;
        let log = "1717000000000,a,err\n1717000000000,b,err\n1717000000101,a,err\n";
        assert_eq!(check_grouped_str(log, &p), 1);
    }

    #[test]
    fn check_grouped_returns_zero_violations_for_empty_input() {
        assert_eq!(check_grouped_str("", &policy()), 0);
    }
}
