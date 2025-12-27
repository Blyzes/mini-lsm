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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);

        buf.put_slice(&self.data);
        self.offsets.iter().for_each(|&n| buf.put_u16(n));
        buf.put_u16(self.offsets.len() as u16);

        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let total_len = data.len();

        let num_of_elements =
            u16::from_be_bytes([data[total_len - 2], data[total_len - 1]]) as usize;
        let offsets_section_start = total_len - 2 - num_of_elements * 2;
        let mut offsets_section = Vec::with_capacity(num_of_elements);
        for i in 0..num_of_elements {
            let curr_pos = offsets_section_start + i * 2;
            let offset = u16::from_be_bytes([data[curr_pos], data[curr_pos + 1]]);
            offsets_section.push(offset);
        }

        let data_section = data[..offsets_section_start].to_vec();

        Self {
            data: data_section,
            offsets: offsets_section,
        }
    }
}
