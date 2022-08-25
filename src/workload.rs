use std::{
    fmt::{self, Display, Formatter},
    mem,
    ops::{AddAssign, SubAssign},
};

/// Workload of a `Request`.
///
/// Workloads are denominated in non-negative integers.
#[derive(Debug, PartialEq, PartialOrd)]
pub enum Workload {
    Int(usize),
    Float(f32),
}

impl Display for Workload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(v) => write!(f, "{}", v),
            Self::Float(v) => write!(f, "{}", v),
        }
    }
}

impl From<u8> for Workload {
    fn from(v: u8) -> Self {
        Self::Int(v as usize)
    }
}

impl From<i32> for Workload {
    fn from(v: i32) -> Self {
        assert!(v >= 0, "Workload must be non-negative");
        if mem::size_of::<i32>() > mem::size_of::<usize>() {
            assert!(v < usize::MAX as i32, "Workload overflow");
        }

        Self::Int(v as usize)
    }
}

impl From<usize> for Workload {
    fn from(v: usize) -> Self {
        Self::Int(v)
    }
}

impl From<f32> for Workload {
    fn from(v: f32) -> Self {
        assert!(v >= 0f32, "Workload must be non-negative");
        Self::Float(v)
    }
}

impl AddAssign for Workload {
    fn add_assign(&mut self, rhs: Self) {
        use Workload::*;
        *self = match (&*self, &rhs) {
            (&Int(a), &Int(b)) => {
                if a < usize::MAX - b {
                    Int(a + b)
                } else {
                    Float(a as f32 + b as f32)
                }
            }
            (&Int(a), &Float(b)) => Float(a as f32 + b),
            (&Float(a), &Int(b)) => Float(a + b as f32),
            (&Float(a), &Float(b)) => Float(a + b),
        }
    }
}

impl SubAssign for Workload {
    fn sub_assign(&mut self, rhs: Self) {
        use Workload::*;
        *self = match (&*self, &rhs) {
            (&Int(a), &Int(b)) => {
                assert!(a >= b, "Workload underflow");
                Int(a - b)
            }
            (&Int(a), &Float(b)) => {
                assert!(a as f32 >= b, "Workload underflow");
                Float(a as f32 - b)
            }
            (&Float(a), &Int(b)) => {
                assert!(a >= b as f32, "Workload underflow");
                Float(a - b as f32)
            }
            (&Float(a), &Float(b)) => {
                assert!(a >= b, "Workload underflow");
                Float(a - b)
            }
        }
    }
}
