// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    while i + 1 < key.key_len()
        && i + 1 < first_key.key_len()
        && key.key_ref()[i] == first_key.key_ref()[i]
    {
        i += 1
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        // length of data(u8)  | length of offsets(u16) | number of key-value pairs in the block
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // key_len(2) | key | value_len(2) | value
        let entry_size = SIZEOF_U16 + SIZEOF_U16 + key.raw_len() + SIZEOF_U16 + value.len();

        if self.estimated_size() + entry_size > self.block_size && !self.first_key.is_empty() {
            return false;
        }

        // offset of data
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // encode key_overlap_len
        self.data.put_u16(overlap as u16);
        // encode rest_key_len
        self.data.put_u16((key.key_len() - overlap) as u16);
        // encode rest_key
        self.data.put(&key.key_ref()[overlap..]);
        // encode key ts
        self.data.put_u64(key.ts());
        // encode value_len
        self.data.put_u16(value.len() as u16);
        // encode value
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty")
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
