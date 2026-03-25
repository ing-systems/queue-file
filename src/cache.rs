//! Element position caching for optimized random access.
//!
//! This module provides the [`OffsetCache`] which stores element positions
//! to accelerate [`Iter::nth`] operations on large queues. Without caching,
//! seeking to element N requires O(N) reads from the beginning. With caching,
//! this is reduced based on the chosen policy.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

use crate::Element;

/// Maximum number of cached element positions.
const MAX_CACHE_SIZE: usize = 4096;

/// Policy controlling how element file-positions are cached to accelerate [`Iter::nth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetCacheKind {
    /// Cache one position every `offset` elements.
    ///
    /// This policy provides a fixed seek performance (O(offset)) at the cost
    /// of O(N/offset) memory usage.
    Linear { offset: usize },
    /// Cache positions at indices that are perfect squares (1, 4, 9, 16, 25, …).
    ///
    /// This policy provides a good trade-off between memory usage (O(√N)) and
    /// seek performance (O(√N)).
    Quadratic,
}

/// Cache for element positions to accelerate random access iteration.
#[derive(Debug, Clone)]
pub struct OffsetCache {
    /// Cached (index, element) pairs.
    pub(crate) offsets: RefCell<VecDeque<(usize, Element)>>,
    /// The caching policy, if any.
    pub(crate) kind: Option<OffsetCacheKind>,
    /// Number of elements removed since cache creation.
    pub(crate) total_removed: Cell<usize>,
}

impl OffsetCache {
    /// Creates a new empty cache.
    pub fn new() -> Self {
        Self { offsets: RefCell::new(VecDeque::new()), kind: None, total_removed: Cell::new(0) }
    }

    /// Sets the caching policy.
    ///
    /// Passing `None` disables caching and clears any existing cache.
    pub fn set_policy(&mut self, kind: impl Into<Option<OffsetCacheKind>>) {
        self.kind = kind.into();

        if self.kind.is_none() {
            self.clear();
        }
    }

    /// Clears all cached offsets.
    pub fn clear(&self) {
        self.offsets.borrow_mut().clear();
        self.total_removed.set(0);
    }

    /// Conditionally caches an element position based on the current policy.
    ///
    /// Called during iteration to cache element positions for future lookups.
    pub fn cache_elem_if_needed(
        &self, index: usize, elem: Element, elem_cnt: usize, affected_items: usize,
    ) {
        debug_assert!(index <= elem_cnt);
        debug_assert!(index + 1 >= affected_items);

        let need_to_cache = match self.kind {
            Some(OffsetCacheKind::Linear { offset }) => {
                let last_cached_abs_index = self.offsets.borrow().back().map_or(0, |(idx, _)| *idx);
                let last_cached_rel_index =
                    last_cached_abs_index.saturating_sub(self.total_removed.get());
                Self::should_cache_linear(index, offset, last_cached_rel_index)
            }
            Some(OffsetCacheKind::Quadratic) => Self::should_cache_quadratic(index, affected_items),
            None => false,
        };

        if !need_to_cache {
            return;
        }

        let abs_index = index + self.total_removed.get();
        let mut offsets = self.offsets.borrow_mut();

        if let Some(&(last_cached_index, last_cached_elem)) = offsets.back() {
            if last_cached_index >= abs_index {
                if last_cached_index == abs_index {
                    debug_assert_eq!(last_cached_elem.pos, elem.pos);
                    debug_assert_eq!(last_cached_elem.len, elem.len);
                }

                return;
            }
        }

        offsets.push_back((abs_index, elem));

        if offsets.len() > MAX_CACHE_SIZE {
            offsets.pop_front();
        }
    }

    #[inline]
    const fn should_cache_linear(index: usize, offset: usize, last_cached_index: usize) -> bool {
        index.saturating_sub(last_cached_index) >= offset
    }

    #[inline]
    fn should_cache_quadratic(index: usize, affected_items: usize) -> bool {
        let x = (index as f64).sqrt() as usize;
        x > 1 && (index + 1 - affected_items..=index).contains(&(x * x))
    }

    /// Returns the cached offset closest to but not exceeding `i`.
    ///
    /// Used by [`Iter::nth`] to jump directly to a cached position.
    #[inline]
    pub fn cached_offset_up_to(&self, i: usize) -> Option<(usize, Element)> {
        let abs_i = i + self.total_removed.get();
        let offsets = self.offsets.borrow();
        let idx = offsets
            .binary_search_by(|(idx, _)| idx.cmp(&abs_i))
            .map_or_else(|i| i.checked_sub(1), Some)?;

        let (abs_idx, elem) = offsets[idx];
        Some((abs_idx.saturating_sub(self.total_removed.get()), elem))
    }

    /// Marks `n` elements as removed, updating internal counters.
    ///
    /// Called after [`QueueFile::remove_n`] to adjust cache state.
    pub fn drop_up_to(&self, n: usize) {
        self.total_removed.set(self.total_removed.get() + n);
        let mut offsets = self.offsets.borrow_mut();
        while matches!(offsets.front(), Some((index, _)) if *index < self.total_removed.get()) {
            offsets.pop_front();
        }
    }
}
