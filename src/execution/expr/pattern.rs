use std::collections::HashSet;

use super::Executor;

impl Executor {
    pub(crate) fn like_match(text: &str, pattern: &str) -> bool {
        if !pattern.contains('_') && !pattern.contains('?') {
            return Self::like_percent_only_match(text, pattern);
        }
        if let Some(prefix) = Self::like_fixed_prefix(pattern) {
            if prefix.len() == pattern.len() {
                return text == pattern;
            }
            if pattern[prefix.len()..].chars().all(|c| c == '%') {
                return text.starts_with(&prefix);
            }
        }

        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let mut t_idx = 0;
        let mut p_idx = 0;
        let mut t_backup = None;
        let mut p_backup = None;

        while t_idx < text_chars.len() {
            if p_idx < pattern_chars.len()
                && (pattern_chars[p_idx] == '?' || pattern_chars[p_idx] == text_chars[t_idx])
            {
                t_idx += 1;
                p_idx += 1;
            } else if p_idx < pattern_chars.len() && pattern_chars[p_idx] == '_' {
                t_idx += 1;
                p_idx += 1;
            } else if p_idx < pattern_chars.len() && pattern_chars[p_idx] == '%' {
                p_backup = Some(p_idx + 1);
                p_idx += 1;
                t_backup = Some(t_idx + 1);
            } else if let Some(p_back) = p_backup {
                p_idx = p_back;
                t_idx = t_backup.unwrap();
                t_backup = Some(t_idx + 1);
            } else {
                return false;
            }
        }

        while p_idx < pattern_chars.len() && pattern_chars[p_idx] == '%' {
            p_idx += 1;
        }

        p_idx == pattern_chars.len()
    }

    fn like_percent_only_match(text: &str, pattern: &str) -> bool {
        if !pattern.contains('%') {
            return text == pattern;
        }

        let mut remainder = text;
        let mut parts = pattern
            .split('%')
            .filter(|part| !part.is_empty())
            .peekable();
        let Some(first) = parts.next() else {
            return true;
        };

        if pattern.starts_with('%') {
            let index = if parts.peek().is_none() && !pattern.ends_with('%') {
                remainder.rfind(first)
            } else {
                remainder.find(first)
            };
            let Some(index) = index else {
                return false;
            };
            remainder = &remainder[index + first.len()..];
        } else {
            if !remainder.starts_with(first) {
                return false;
            }
            remainder = &remainder[first.len()..];
        }

        while let Some(part) = parts.next() {
            let Some(index) = remainder.find(part) else {
                return false;
            };
            remainder = &remainder[index + part.len()..];

            if parts.peek().is_none() && !pattern.ends_with('%') {
                return remainder.is_empty();
            }
        }

        pattern.ends_with('%') || remainder.is_empty()
    }

    pub(crate) fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub(crate) fn tokenize_unique(text: &str) -> HashSet<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub(crate) fn like_fixed_prefix(pattern: &str) -> Option<String> {
        let prefix: String = pattern
            .chars()
            .take_while(|c| *c != '%' && *c != '_' && *c != '?')
            .collect();
        if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        }
    }
}
