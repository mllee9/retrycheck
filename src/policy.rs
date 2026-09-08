//! A retry policy is the thing a client is *supposed* to follow. Everything in this
//! module is pure math: given a policy and an attempt number, what delay does the
//! policy prescribe before that attempt fires.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Jitter {
    /// No jitter: the delay before an attempt is a single deterministic value.
    None,
    /// Full jitter (as in the AWS backoff writeup): delay is uniform(0, computed_cap),
    /// so an observed gap is only wrong if it exceeds the cap.
    Full,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub multiplier: f64,
    pub max_delay_ms: u64,
    pub jitter: Jitter,
}

impl RetryPolicy {
    /// Delay the policy prescribes before running `attempt` (attempts are 1-indexed,
    /// so there is no delay before attempt 1). Grows exponentially from base_delay_ms,
    /// capped at max_delay_ms.
    pub fn expected_delay_ms(&self, attempt: u32) -> u64 {
        if attempt <= 1 {
            return 0;
        }
        let exponent = (attempt - 2) as i32;
        let raw = self.base_delay_ms as f64 * self.multiplier.powi(exponent);
        if !raw.is_finite() {
            return self.max_delay_ms;
        }
        raw.min(self.max_delay_ms as f64).max(0.0) as u64
    }

    /// Whether an observed gap, in milliseconds, before `attempt` is consistent with
    /// this policy. Deterministic policies get a small fixed tolerance to absorb
    /// clock and scheduler skew; jittered policies only bound the delay from above.
    pub fn accepts_gap(&self, attempt: u32, observed_ms: u64) -> bool {
        let expected = self.expected_delay_ms(attempt);
        match self.jitter {
            Jitter::None => {
                const TOLERANCE_MS: u64 = 5;
                observed_ms + TOLERANCE_MS >= expected && observed_ms <= expected + TOLERANCE_MS
            }
            Jitter::Full => observed_ms <= expected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            base_delay_ms: 100,
            multiplier: 2.0,
            max_delay_ms: 10_000,
            jitter: Jitter::None,
        }
    }

    #[test]
    fn first_attempt_has_no_delay() {
        assert_eq!(policy().expected_delay_ms(1), 0);
    }

    #[test]
    fn delay_doubles_per_attempt() {
        let p = policy();
        assert_eq!(p.expected_delay_ms(2), 100);
        assert_eq!(p.expected_delay_ms(3), 200);
        assert_eq!(p.expected_delay_ms(4), 400);
    }

    #[test]
    fn delay_is_capped() {
        let p = policy();
        assert_eq!(p.expected_delay_ms(20), 10_000);
    }

    #[test]
    fn full_jitter_only_bounds_from_above() {
        let mut p = policy();
        p.jitter = Jitter::Full;
        assert!(p.accepts_gap(3, 0));
        assert!(p.accepts_gap(3, 200));
        assert!(!p.accepts_gap(3, 201));
    }
}
