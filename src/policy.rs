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
    /// Equal jitter: half the computed cap is fixed, the other half is uniform,
    /// so delay is uniform(cap/2, cap). Keeps a retry storm from ever going
    /// fully silent the way full jitter occasionally does.
    Equal,
    /// Decorrelated jitter (also from the AWS writeup): each delay is
    /// uniform(base_delay_ms, previous_delay * 3), capped at max_delay_ms.
    /// Unlike the other modes the bound depends on the delay actually observed
    /// before the previous attempt, not on the attempt number alone.
    Decorrelated,
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

    /// The range of delays this policy allows before `attempt`. `prev_observed_ms`
    /// is the delay actually observed before the previous attempt; every mode but
    /// decorrelated jitter ignores it, since only decorrelated jitter chooses each
    /// delay from the last one rather than from the attempt number.
    pub fn delay_bounds(&self, attempt: u32, prev_observed_ms: u64) -> (u64, u64) {
        if attempt <= 1 {
            return (0, 0);
        }
        match self.jitter {
            Jitter::None => {
                let expected = self.expected_delay_ms(attempt);
                (expected, expected)
            }
            Jitter::Full => (0, self.expected_delay_ms(attempt)),
            Jitter::Equal => {
                let cap = self.expected_delay_ms(attempt);
                (cap / 2, cap)
            }
            Jitter::Decorrelated => {
                // prev_observed_ms is 0 before the second attempt, when there is no
                // prior observed delay yet; the algorithm starts from base_delay_ms.
                let prev = if prev_observed_ms == 0 { self.base_delay_ms } else { prev_observed_ms };
                let max = prev.saturating_mul(3).min(self.max_delay_ms).max(self.base_delay_ms);
                (self.base_delay_ms, max)
            }
        }
    }

    /// Whether an observed gap, in milliseconds, before `attempt` is consistent with
    /// this policy. Deterministic policies get a small fixed tolerance to absorb
    /// clock and scheduler skew; jittered policies only need the gap inside their
    /// bounds. See `delay_bounds` for what `prev_observed_ms` means.
    pub fn accepts_gap(&self, attempt: u32, observed_ms: u64, prev_observed_ms: u64) -> bool {
        let (min, max) = self.delay_bounds(attempt, prev_observed_ms);
        match self.jitter {
            Jitter::None => {
                const TOLERANCE_MS: u64 = 5;
                observed_ms + TOLERANCE_MS >= min && observed_ms <= max + TOLERANCE_MS
            }
            _ => observed_ms >= min && observed_ms <= max,
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
        assert!(p.accepts_gap(3, 0, 0));
        assert!(p.accepts_gap(3, 200, 0));
        assert!(!p.accepts_gap(3, 201, 0));
    }

    #[test]
    fn equal_jitter_bounds_between_half_cap_and_cap() {
        let mut p = policy();
        p.jitter = Jitter::Equal;
        // attempt 3's cap is 200, so the allowed range is [100, 200].
        assert!(!p.accepts_gap(3, 99, 0));
        assert!(p.accepts_gap(3, 100, 0));
        assert!(p.accepts_gap(3, 200, 0));
        assert!(!p.accepts_gap(3, 201, 0));
    }

    #[test]
    fn decorrelated_jitter_starts_from_base_delay() {
        let mut p = policy();
        p.jitter = Jitter::Decorrelated;
        // no prior observed delay yet: range is [base_delay_ms, base_delay_ms * 3].
        assert!(!p.accepts_gap(2, 99, 0));
        assert!(p.accepts_gap(2, 100, 0));
        assert!(p.accepts_gap(2, 300, 0));
        assert!(!p.accepts_gap(2, 301, 0));
    }

    #[test]
    fn decorrelated_jitter_bounds_on_the_previous_observed_delay() {
        let mut p = policy();
        p.jitter = Jitter::Decorrelated;
        // previous attempt actually waited 250ms, so this one's range is [100, 750].
        assert!(!p.accepts_gap(3, 99, 250));
        assert!(p.accepts_gap(3, 100, 250));
        assert!(p.accepts_gap(3, 750, 250));
        assert!(!p.accepts_gap(3, 751, 250));
    }

    #[test]
    fn decorrelated_jitter_never_exceeds_the_cap() {
        let mut p = policy();
        p.jitter = Jitter::Decorrelated;
        // previous delay of 9000ms would push 3x past max_delay_ms (10_000).
        let (_, max) = p.delay_bounds(4, 9_000);
        assert_eq!(max, 10_000);
    }
}
