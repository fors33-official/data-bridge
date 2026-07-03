// Copyright (c) 2026 FORS33. All rights reserved.
// Batch execution caps shared across connectors (transport-only).

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct ExecutionLimits {
    pub max_records: Option<u64>,
    pub max_duration_sec: Option<u64>,
    pub max_pages: Option<usize>,
}

impl ExecutionLimits {
    pub fn check_records(&self, accepted: u64) -> Option<&'static str> {
        if let Some(max) = self.max_records {
            if accepted >= max {
                return Some("max_records");
            }
        }
        None
    }

    pub fn check_duration(&self, start: Instant) -> Option<&'static str> {
        if let Some(sec) = self.max_duration_sec {
            if start.elapsed() >= Duration::from_secs(sec) {
                return Some("max_duration_sec");
            }
        }
        None
    }

    pub fn check_pages(&self, pages_done: usize) -> Option<&'static str> {
        if let Some(max) = self.max_pages {
            if pages_done >= max {
                return Some("max_pages");
            }
        }
        None
    }

    pub fn check_writer(&self, accepted: u64, start: Instant) -> Option<&'static str> {
        self.check_records(accepted)
            .or_else(|| self.check_duration(start))
    }
}

pub fn emit_batch_complete(reason: &str) {
    eprintln!("[FORS33] batch complete reason={}", reason);
}

/// Writer stopped draining the channel after a batch cap (`max_duration`, `max_records`, etc.).
/// Batch connectors must exit cleanly; stream connectors remain fail-fast (exit 1).
pub fn writer_send_disconnected(is_batch: bool, connector: &str) -> bool {
    if is_batch {
        eprintln!(
            "[FORS33] {} batch writer closed; stopping connector.",
            connector
        );
        return true;
    }
    eprintln!(
        "[FORS33] FATAL: Writer channel closed. Stopping {} connector.",
        connector
    );
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_send_disconnected_batch_returns_true() {
        assert!(writer_send_disconnected(true, "websocket"));
    }

    #[test]
    fn check_pages_stops_at_cap() {
        let limits = ExecutionLimits {
            max_pages: Some(3),
            ..Default::default()
        };
        assert!(limits.check_pages(2).is_none());
        assert_eq!(limits.check_pages(3), Some("max_pages"));
    }
}
