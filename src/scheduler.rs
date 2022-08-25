//! Structs for scheduling how work is dispatched amongst workers.

use std::num::NonZeroUsize;

use crate::Workload;

/// A trait to help the dispatcher dispatch requests in a load-balancing manner.
pub trait Scheduler: Send + 'static {
    // Inits the scheduler based on the total number of workers.
    fn init(&mut self, num_workers: NonZeroUsize);

    /// Selects a worker to dispatch work to based on the distribution of workloads.
    fn select(&mut self, workloads: &[Workload]) -> usize;
}

/// The default `Scheduler` implementation that employs a pseudo-P2C algorithm.
pub struct P2cScheduler {
    next: usize,
    jump: usize,
    mask: usize,
}

impl P2cScheduler {
    pub fn new(num_workers: NonZeroUsize) -> Self {
        let mask = num_workers.get();
        let jump = if mask <= 6 {
            1
        } else if mask <= 8 {
            3
        } else {
            prime::previous_prime(mask - 3)
        };
        Self {
            next: 0,
            jump,
            mask,
        }
    }
}

impl Scheduler for P2cScheduler {
    fn init(&mut self, num_workers: NonZeroUsize) {
        let new = Self::new(num_workers);
        *self = Self { ..new };
    }

    fn select(&mut self, workloads: &[Workload]) -> usize {
        // Instead of trying to find the smallest workload,
        // use a pseudo-p2c algo to find the worker to assign the req to.
        let new_next = (self.next + self.jump) % self.mask;
        let i = if workloads[new_next] < workloads[self.next] {
            new_next
        } else {
            self.next
        };
        self.next = (self.next + 1) % self.mask;
        i
    }
}

mod prime {
    fn power(a: u64, n: u64, m: u64) -> u64 {
        let mut power = a;
        let mut result = 1;
        let mut n = n;
        while n > 0 {
            if n & 1 > 0 {
                result = (result * power) % m;
            }
            power = (power * power) % m;
            n >>= 1;
        }
        result
    }

    /// Miller–Rabin primality test
    /// https://en.wikipedia.org/wiki/Miller%E2%80%93Rabin_primality_test#Deterministic_variants
    fn is_prime_mr(n: u64) -> bool {
        if n <= 3 {
            return n >= 2;
        }
        if n % 2 == 0 {
            return false;
        }

        let (d, s) = {
            let mut d = n / 2;
            let mut s = 1;
            while d % 2 == 0 {
                d /= 2;
                s += 1;
            }
            (d, s)
        };

        if n < 2_047 {
            [2].iter()
        } else if n < 1_373_653 {
            [2, 3].iter()
        } else if n < 9_080_191 {
            [31, 73].iter()
        } else if n < 25_326_001 {
            [2, 3, 5].iter()
        } else if n < 3_215_031_751 {
            [2, 3, 5, 7].iter()
        } else if n < 4_759_123_141 {
            [2, 7, 61].iter()
        } else if n < 1_122_004_669_633 {
            [2, 13, 23, 1_662_803].iter()
        } else if n < 2_152_302_898_747 {
            [2, 3, 5, 7, 11].iter()
        } else if n < 3_474_749_660_383 {
            [2, 3, 5, 7, 11, 13].iter()
        } else if n < 341_550_071_728_321 {
            [2, 3, 5, 7, 11, 13, 17].iter()
        } else if n < 3_825_123_056_546_413_051 {
            [2, 3, 5, 7, 11, 13, 17, 19, 23].iter()
        } else {
            [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37].iter()
        }
        .cloned()
        .all(|a: u64| -> bool {
            let mut x = power(a, d, n);
            let mut y: u64 = 0;
            let mut s = s;

            while s > 0 {
                y = (x * x) % n;
                if y == 1 && x != 1 && x != n - 1 {
                    return false;
                }
                x = y;
                s -= 1;
            }
            y == 1
        })
    }

    pub fn previous_prime(mut x: usize) -> usize {
        match x {
            0 | 1 => return 0,
            2 => return 2,
            3 | 4 => return 3,
            _ => {}
        }
        if x % 2 == 0 {
            x -= 1;
        }

        // x is odd
        let (o, mut i) = if x % 6 == 5 { (5, 4) } else { (1, 2) };

        x = (x / 6) * 6 + o;
        while !is_prime_mr(x as u64) {
            x -= i;
            i ^= 6;
        }
        x
    }
}
