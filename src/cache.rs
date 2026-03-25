use std::collections::VecDeque;

use crate::Element;

/// Policy controlling how element file-positions are cached to accelerate [`Iter::nth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetCacheKind {
    /// Cache one position every `offset` elements.
    Linear { offset: usize },
    /// Cache positions at indices that are perfect squares (1, 4, 9, 16, 25, …).
    Quadratic,
}

#[derive(Debug, Clone)]
pub struct OffsetCache {
    pub(crate) offsets: VecDeque<(usize, Element)>,
    pub(crate) kind: Option<OffsetCacheKind>,
}

impl OffsetCache {
    pub fn new() -> Self {
        Self { offsets: VecDeque::new(), kind: None }
    }

    pub fn set_policy(&mut self, kind: impl Into<Option<OffsetCacheKind>>) {
        self.kind = kind.into();

        if self.kind.is_none() {
            self.offsets.clear();
        }
    }

    pub fn clear(&mut self) {
        self.offsets.clear();
    }

    pub fn cache_elem_if_needed(
        &mut self, index: usize, elem: Element, elem_cnt: usize, affected_items: usize,
    ) {
        debug_assert!(index <= elem_cnt);
        debug_assert!(index + 1 >= affected_items);

        let need_to_cache = match self.kind {
            Some(OffsetCacheKind::Linear { offset }) => {
                let last_cached_index = self.offsets.back().map_or(0, |(idx, _)| *idx);
                Self::should_cache_linear(index, offset, last_cached_index)
            }
            Some(OffsetCacheKind::Quadratic) => Self::should_cache_quadratic(index, affected_items),
            None => false,
        };

        if !need_to_cache {
            return;
        }

        if let Some(&(last_cached_index, last_cached_elem)) = self.offsets.back() {
            if last_cached_index >= index {
                if last_cached_index == index {
                    debug_assert_eq!(last_cached_elem.pos, elem.pos);
                    debug_assert_eq!(last_cached_elem.len, elem.len);
                }

                return;
            }
        }

        self.offsets.push_back((index, elem));
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

    #[inline]
    pub fn cached_index_up_to(&self, i: usize) -> Option<usize> {
        self.offsets
            .binary_search_by(|(idx, _)| idx.cmp(&i))
            .map_or_else(|i| i.checked_sub(1), Some)
    }

    pub fn drop_up_to(&mut self, n: usize) {
        while matches!(self.offsets.front(), Some((index, _)) if *index < n) {
            self.offsets.pop_front();
        }
        self.offsets.iter_mut().for_each(|(i, _)| *i -= n);
    }
}
