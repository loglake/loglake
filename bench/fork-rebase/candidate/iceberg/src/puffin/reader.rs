// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::OnceCell;

use super::validate_puffin_compression;
use crate::Result;
use crate::io::InputFile;
use crate::io::read_observability::{
    ObjectStoreReadPhase, ReadDebouncer, record_object_store_reads,
};
use crate::puffin::blob::Blob;
use crate::puffin::metadata::{BlobMetadata, FileMetadata};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BlobKey {
    path: String,
    offset: u64,
}

fn metadata_debouncer() -> &'static ReadDebouncer<String, Arc<FileMetadata>> {
    static DEBOUNCER: OnceLock<ReadDebouncer<String, Arc<FileMetadata>>> = OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

fn blob_debouncer() -> &'static ReadDebouncer<BlobKey, Arc<[u8]>> {
    static DEBOUNCER: OnceLock<ReadDebouncer<BlobKey, Arc<[u8]>>> = OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

struct MetadataCache {
    order: VecDeque<String>,
    entries: HashMap<String, Arc<FileMetadata>>,
}

fn metadata_cache() -> &'static Mutex<MetadataCache> {
    static CACHE: OnceLock<Mutex<MetadataCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(MetadataCache {
            order: VecDeque::new(),
            entries: HashMap::new(),
        })
    })
}

fn metadata_cache_max_entries() -> usize {
    std::env::var("LOGLAKE_PUFFIN_FOOTER_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(128)
}

fn metadata_cache_get(path: &str) -> Option<Arc<FileMetadata>> {
    (metadata_cache_max_entries() > 0)
        .then(|| metadata_cache().lock().unwrap().entries.get(path).cloned())
        .flatten()
}

fn metadata_cache_put(path: String, metadata: Arc<FileMetadata>) {
    let max_entries = metadata_cache_max_entries();
    if max_entries == 0 {
        return;
    }
    let mut cache = metadata_cache().lock().unwrap();
    if cache.entries.contains_key(&path) {
        return;
    }
    cache.entries.insert(path.clone(), metadata);
    cache.order.push_back(path);
    while cache.order.len() > max_entries {
        if let Some(evicted) = cache.order.pop_front() {
            cache.entries.remove(&evicted);
        }
    }
}

#[derive(Default)]
struct BlobCache {
    bytes: usize,
    order: VecDeque<BlobKey>,
    entries: HashMap<BlobKey, Arc<[u8]>>,
}

impl BlobCache {
    fn put(
        &mut self,
        key: BlobKey,
        bytes: Arc<[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> bool {
        if max_entries == 0 || max_bytes == 0 || bytes.len() > max_bytes {
            return false;
        }
        if self.entries.contains_key(&key) {
            return true;
        }
        self.bytes += bytes.len();
        self.entries.insert(key.clone(), bytes);
        self.order.push_back(key);
        while self.entries.len() > max_entries || self.bytes > max_bytes {
            if let Some(evicted) = self.order.pop_front()
                && let Some(bytes) = self.entries.remove(&evicted)
            {
                self.bytes = self.bytes.saturating_sub(bytes.len());
            }
        }
        true
    }
}

fn blob_cache() -> &'static Mutex<BlobCache> {
    static CACHE: OnceLock<Mutex<BlobCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BlobCache::default()))
}

fn blob_cache_bounds() -> (usize, usize) {
    let entries = std::env::var("LOGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(128);
    let bytes = std::env::var("LOGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(256 * 1024 * 1024);
    (entries, bytes)
}

fn blob_cache_get(key: &BlobKey) -> Option<Arc<[u8]>> {
    let (max_entries, max_bytes) = blob_cache_bounds();
    if max_entries == 0 || max_bytes == 0 {
        return None;
    }
    blob_cache().lock().unwrap().entries.get(key).cloned()
}

fn blob_cache_put(key: BlobKey, bytes: Arc<[u8]>) {
    let (max_entries, max_bytes) = blob_cache_bounds();
    if max_entries == 0 || max_bytes == 0 {
        return;
    }
    if bytes.len() > max_bytes {
        metrics::counter!(
            "loglake_iceberg_puffin_blob_cache_evictions_total",
            "reason" => "oversized"
        )
        .increment(1);
        return;
    }
    blob_cache()
        .lock()
        .unwrap()
        .put(key, bytes, max_entries, max_bytes);
}

/// Puffin reader
pub struct PuffinReader {
    input_file: InputFile,
    file_metadata: OnceCell<Arc<FileMetadata>>,
}

impl PuffinReader {
    /// Returns a new Puffin reader
    pub fn new(input_file: InputFile) -> Self {
        Self {
            input_file,
            file_metadata: OnceCell::new(),
        }
    }

    /// Returns file metadata
    pub async fn file_metadata(&self) -> Result<&FileMetadata> {
        self.file_metadata
            .get_or_try_init(|| async {
                let path = self.input_file.location().to_string();
                if let Some(metadata) = metadata_cache_get(&path) {
                    tokio::task::coop::consume_budget().await;
                    return Ok(metadata);
                }
                let input_file = self.input_file.clone();
                let cache_path = path.clone();
                metadata_debouncer()
                    .run(path, "puffin_metadata", move || async move {
                        if let Some(metadata) = metadata_cache_get(&cache_path) {
                            tokio::task::coop::consume_budget().await;
                            return Ok(metadata);
                        }
                        let metadata = Arc::new(FileMetadata::read(&input_file).await?);
                        metadata_cache_put(cache_path, metadata.clone());
                        Ok(metadata)
                    })
                    .await
            })
            .await
            .map(Arc::as_ref)
    }

    /// Returns blob
    pub async fn blob(&self, blob_metadata: &BlobMetadata) -> Result<Blob> {
        validate_puffin_compression(blob_metadata.compression_codec)?;

        let start = blob_metadata.offset;
        let end = start + blob_metadata.length;
        let key = BlobKey {
            path: self.input_file.location().to_string(),
            offset: start,
        };
        let data = if let Some(hit) = blob_cache_get(&key) {
            metrics::counter!(
                "loglake_iceberg_puffin_blob_cache_lookups_total",
                "outcome" => "hit"
            )
            .increment(1);
            tokio::task::coop::consume_budget().await;
            hit
        } else {
            metrics::counter!(
                "loglake_iceberg_puffin_blob_cache_lookups_total",
                "outcome" => "miss"
            )
            .increment(1);
            let input_file = self.input_file.clone();
            let codec = blob_metadata.compression_codec;
            let cache_key = key.clone();
            blob_debouncer()
                .run(key, "puffin_blob", move || async move {
                    if let Some(hit) = blob_cache_get(&cache_key) {
                        tokio::task::coop::consume_budget().await;
                        return Ok(hit);
                    }
                    let file_read = input_file.reader().await?;
                    let outcome = file_read.read_with_outcome(start..end).await?;
                    if outcome.fetched {
                        record_object_store_reads(
                            ObjectStoreReadPhase::Index,
                            1,
                            outcome.bytes.len() as u64,
                        );
                    }
                    let data = Arc::<[u8]>::from(codec.decompress(outcome.bytes.to_vec())?);
                    blob_cache_put(cache_key, data.clone());
                    Ok(data)
                })
                .await?
        };

        Ok(Blob {
            r#type: blob_metadata.r#type.clone(),
            fields: blob_metadata.fields.clone(),
            snapshot_id: blob_metadata.snapshot_id,
            sequence_number: blob_metadata.sequence_number,
            data: data.as_ref().to_vec(),
            properties: blob_metadata.properties.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::ErrorKind;
    use crate::compression::CompressionCodec;
    use crate::puffin::metadata::BlobMetadata;
    use crate::puffin::reader::PuffinReader;
    use crate::puffin::test_utils::{
        blob_0, blob_1, java_uncompressed_metric_input_file,
        java_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };

    #[test]
    fn blob_cache_holds_both_bounds_and_rejects_oversized_entries() {
        let mut cache = super::BlobCache::default();
        let key = |offset| super::BlobKey {
            path: "memory://candidate/cache-1752.puffin".to_string(),
            offset,
        };
        assert!(cache.put(key(0), std::sync::Arc::from(&b"aaaa"[..]), 2, 6));
        assert!(cache.put(key(4), std::sync::Arc::from(&b"bbbb"[..]), 2, 6));
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.bytes <= 6);
        assert!(!cache.put(key(8), std::sync::Arc::from(&b"1234567"[..]), 2, 6));
        assert!(!cache.entries.contains_key(&key(8)));
    }

    #[tokio::test]
    async fn test_puffin_reader_uncompressed_metric_data() {
        let input_file = java_uncompressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_puffin_reader_zstd_compressed_metric_data() {
        let input_file = java_zstd_compressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_gzip_compression_rejected_on_blob_access() {
        // Use a real puffin file
        let input_file = java_uncompressed_metric_input_file();
        let reader = PuffinReader::new(input_file);

        // Create a BlobMetadata with Gzip compression
        let gzip_blob_metadata = BlobMetadata {
            r#type: "test-type".to_string(),
            fields: vec![1],
            snapshot_id: 1,
            sequence_number: 1,
            offset: 4,
            length: 10,
            compression_codec: CompressionCodec::gzip_default(),
            properties: HashMap::new(),
        };

        // Attempting to access the blob should fail
        let result = reader.blob(&gzip_blob_metadata).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("gzip"));
        assert!(
            err.to_string()
                .contains("is not supported for Puffin files")
        );
    }
}
