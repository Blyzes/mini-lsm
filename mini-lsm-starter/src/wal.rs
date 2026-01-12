// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create_new(path).context("failed to create WAL")?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf = buf.as_slice();
        while rbuf.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = rbuf.get_u16() as usize;
            hasher.write_u16(key_len as u16);
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            hasher.write(&key);
            rbuf.advance(key_len);
            let ts = rbuf.get_u64();
            hasher.write_u64(ts);
            let value_len = rbuf.get_u16() as usize;
            hasher.write_u16(value_len as u16);
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            hasher.write(&value);
            rbuf.advance(value_len);
            let checksum = rbuf.get_u32();
            if hasher.finalize() != checksum {
                bail!("checksum mismatched")
            }
            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::with_capacity(
            key.raw_len() + value.len() + 2 * size_of::<u16>() + size_of::<u32>(),
        );
        let mut hasher = crc32fast::Hasher::new();
        hasher.write_u16(key.key_len() as u16);
        buf.put_u16(key.key_len() as u16);
        hasher.write(key.key_ref());
        buf.put_slice(key.key_ref());
        hasher.write_u64(key.ts());
        buf.put_u64(key.ts());
        hasher.write_u16(value.len() as u16);
        buf.put_u16(value.len() as u16);
        hasher.write(value);
        buf.put_slice(value);
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
