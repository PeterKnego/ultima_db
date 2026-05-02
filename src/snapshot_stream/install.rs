// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#[derive(Debug, Clone, Copy)]
pub enum OnUnknown {
    Drop,
    Keep,
    Error,
}

#[derive(Debug, Clone)]
pub struct InstallOptions {
    pub on_unknown_tables: OnUnknown,
    pub commit_version: Option<u64>,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            on_unknown_tables: OnUnknown::Drop,
            commit_version: None,
        }
    }
}
