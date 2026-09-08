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
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::process::ExitCode;

struct Args {
    policy: RetryPolicy,
    input_path: Option<String>,
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
options:\n\
  --max-attempts N     stop attempts allowed before giving up (default 5)\n\
  --base-delay-ms N    delay before the second attempt (default 100)\n\
  --multiplier X       growth factor applied per attempt (default 2.0)\n\
  --max-delay-ms N     cap on the computed delay (default 30000)\n\
  --jitter MODE        \"none\" or \"full\" (default none)\n\
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

fn run() -> Result<u32, String> {
    let args = parse_args()?;
    match args.input_path {
        Some(path) => {
            let file = File::open(&path).map_err(|e| format!("can't open '{path}': {e}"))?;
            check(BufReader::new(file), &args.policy)
        }
        None => {
            let stdin = io::stdin();
            check(stdin.lock(), &args.policy)
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
