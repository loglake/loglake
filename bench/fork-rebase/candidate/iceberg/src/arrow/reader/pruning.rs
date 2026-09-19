// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.

//! LogLake bloom, inverted-index, and promoted-column pruning.

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

use super::{ArrowReader, PromotedPruneSpec, RawPruneSpec};
use crate::Result;
use crate::io::FileIO;
use crate::puffin::PuffinReader;
use crate::scan::FileScanTask;

impl ArrowReader {
    fn prune_source_label(spec: &RawPruneSpec) -> &'static str {
        if spec.fts_udf {
            "fts_udf"
        } else {
            "like_substring"
        }
    }

    fn metadata_value<'a>(metadata: &'a ParquetMetaData, key: &str) -> Option<&'a str> {
        metadata
            .file_metadata()
            .key_value_metadata()?
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.value.as_deref())
    }

    fn file_trigram_bloom(metadata: &ParquetMetaData) -> Option<loglake_bloom::TokenBloom> {
        loglake_bloom::TokenBloom::from_hex(Self::metadata_value(
            metadata,
            loglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY,
        )?)
    }

    fn footer_inverted_index(
        metadata: &ParquetMetaData,
        column: &str,
    ) -> Option<loglake_index::InvertedIndex> {
        let key = loglake_index::inverted_index_kv_key(column);
        loglake_index::InvertedIndex::from_hex(Self::metadata_value(metadata, key.as_ref())?)
    }

    fn stamped_row_group_size_matches(metadata: &ParquetMetaData, stamped: usize) -> bool {
        let row_groups = metadata.row_groups();
        !row_groups.is_empty()
            && row_groups.iter().enumerate().all(|(index, group)| {
                let rows = group.num_rows() as usize;
                if index + 1 == row_groups.len() {
                    rows > 0 && rows <= stamped
                } else {
                    rows == stamped
                }
            })
    }

    async fn puffin_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<loglake_index::InvertedIndex>> {
        for reference in &task.statistics_blobs {
            if reference.blob_type != "loglake-inverted-v1"
                || reference.properties.get("column").map(String::as_str) != Some(column)
            {
                continue;
            }
            let Some(row_group_size) = reference
                .properties
                .get("row_group_size")
                .and_then(|value| value.parse::<usize>().ok())
            else {
                continue;
            };
            if !Self::stamped_row_group_size_matches(metadata, row_group_size) {
                metrics::counter!("loglake_index_stamp_mismatch_total").increment(1);
                return Ok(None);
            }

            let reader = PuffinReader::new(file_io.new_input(&reference.statistics_path)?)
                .with_cache_bypass(cache_bypass);
            let blob_metadata = reader
                .file_metadata()
                .await?
                .blobs()
                .iter()
                .find(|blob| {
                    blob.blob_type() == "loglake-inverted-v1"
                        && blob.properties().get("data_file").map(String::as_str)
                            == Some(task.data_file_path())
                        && blob.properties().get("column").map(String::as_str) == Some(column)
                })
                .cloned();
            let Some(blob_metadata) = blob_metadata else {
                continue;
            };
            let blob = reader.blob(&blob_metadata).await?;
            return Ok(loglake_index::InvertedIndex::from_bytes(blob.data()));
        }
        Ok(None)
    }

    async fn file_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<(loglake_index::InvertedIndex, &'static str)>> {
        if let Some(index) = Self::footer_inverted_index(metadata, column) {
            return Ok(Some((index, "footer_kv")));
        }
        Ok(
            Self::puffin_inverted_index(file_io, task, metadata, column, cache_bypass)
                .await?
                .map(|index| (index, "puffin")),
        )
    }

    fn index_load_semaphore() -> &'static tokio::sync::Semaphore {
        static SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
        SEMAPHORE.get_or_init(|| {
            tokio::sync::Semaphore::new(index_load_concurrency_from(
                std::env::var("LOGLAKE_INDEX_LOAD_CONCURRENCY")
                    .ok()
                    .as_deref(),
            ))
        })
    }

    fn bloom_might_contain_substring(
        bloom: &loglake_bloom::TokenBloom,
        substring: &str,
    ) -> Option<bool> {
        let grams = loglake_bloom::query_trigrams(substring)?;
        Some(grams.iter().all(|gram| bloom.maybe_contains(gram)))
    }

    pub(super) fn file_might_match_prune_spec(
        metadata: &ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> bool {
        let bloom = (spec.column == "raw")
            .then(|| Self::file_trigram_bloom(metadata))
            .flatten();
        let might_match = |value: &str| {
            bloom
                .as_ref()
                .and_then(|bloom| Self::bloom_might_contain_substring(bloom, value))
                .unwrap_or(true)
        };
        let matches = spec.all_terms.iter().all(|term| might_match(term))
            && (spec.any_terms.is_empty() || spec.any_terms.iter().any(|term| might_match(term)))
            && spec
                .substrings
                .iter()
                .all(|substring| might_match(substring));
        metrics::counter!(
            "loglake_iceberg_raw_bloom_skip_total",
            "outcome" => if matches { "read" } else { "skip" },
            "source" => Self::prune_source_label(spec)
        )
        .increment(1);
        matches
    }

    pub(super) fn rowgroup_bloom_survivors_for_spec(
        metadata: &ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> Option<Vec<usize>> {
        if spec.column != "raw" {
            return None;
        }
        let blooms = loglake_bloom::rowgroup_blooms_from_hex(Self::metadata_value(
            metadata,
            loglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY,
        )?)?;
        if blooms.len() != metadata.num_row_groups() {
            return None;
        }
        let source = Self::prune_source_label(spec);
        Some(
            blooms
                .iter()
                .enumerate()
                .filter_map(|(index, bloom)| {
                    let matches = spec.all_terms.iter().chain(&spec.substrings).all(|value| {
                        Self::bloom_might_contain_substring(bloom, value).unwrap_or(true)
                    }) && (spec.any_terms.is_empty()
                        || spec.any_terms.iter().any(|value| {
                            Self::bloom_might_contain_substring(bloom, value).unwrap_or(true)
                        }));
                    metrics::counter!(
                        "loglake_iceberg_raw_rowgroup_bloom_skip_total",
                        "outcome" => if matches { "read" } else { "skip" },
                        "source" => source
                    )
                    .increment(1);
                    matches.then_some(index)
                })
                .collect(),
        )
    }

    pub(super) fn row_group_might_match_promoted(
        group: &RowGroupMetaData,
        spec: &PromotedPruneSpec,
    ) -> bool {
        let Some(column) = group
            .columns()
            .iter()
            .find(|column| column.column_descr().path().string() == spec.column)
        else {
            return true;
        };
        let Some(statistics) = column.statistics() else {
            return true;
        };
        if statistics.null_count_opt() == Some(group.num_rows() as u64) {
            return false;
        }
        let parquet::file::statistics::Statistics::ByteArray(bytes) = statistics else {
            return true;
        };
        let (Some(min), Some(max)) = (bytes.min_bytes_opt(), bytes.max_bytes_opt()) else {
            return true;
        };
        spec.values
            .iter()
            .any(|value| value.as_bytes() >= min && value.as_bytes() <= max)
    }

    pub(super) async fn inverted_index_row_selection(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        selected_row_groups: &Option<Vec<usize>>,
        spec: &RawPruneSpec,
        cache_bypass: bool,
    ) -> Result<Option<RowSelection>> {
        let index = {
            let _permit = Self::index_load_semaphore()
                .acquire()
                .await
                .expect("index-load semaphore is never closed");
            Self::file_inverted_index(file_io, task, metadata, &spec.column, cache_bypass).await?
        };
        let Some((index, storage)) = index else {
            return Ok(None);
        };
        let mut matching: Option<Vec<u32>> = None;
        if !spec.all_terms.is_empty() {
            let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
            matching = Some(index.matching_rows_all(&terms));
        }
        if !spec.any_terms.is_empty() {
            let mut union = Vec::new();
            for term in &spec.any_terms {
                if let Some(rows) = index.postings(term) {
                    union = union_sorted(&union, rows);
                }
            }
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &union),
                None => union,
            });
        }
        for substring in &spec.index_substrings {
            let Some(rows) = index.rows_containing(substring) else {
                return Ok(None);
            };
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &rows),
                None => rows,
            });
        }
        let Some(matching) = matching else {
            return Ok(None);
        };
        metrics::counter!(
            "loglake_iceberg_inverted_index_used_total",
            "source" => Self::prune_source_label(spec),
            "storage" => storage
        )
        .increment(1);
        Ok(Some(index_matches_row_selection(
            metadata.row_groups(),
            selected_row_groups,
            &matching,
        )))
    }
}

fn index_load_concurrency_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.trim().parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

fn union_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    merge_sorted(left, right, false)
}

fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    merge_sorted(left, right, true)
}

fn merge_sorted(left: &[u32], right: &[u32], intersection: bool) -> Vec<u32> {
    let mut output = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                if !intersection {
                    output.push(left[left_index]);
                }
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                if !intersection {
                    output.push(right[right_index]);
                }
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                output.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    if !intersection {
        output.extend_from_slice(&left[left_index..]);
        output.extend_from_slice(&right[right_index..]);
    }
    output
}

fn index_matches_row_selection(
    row_groups: &[RowGroupMetaData],
    selected_row_groups: &Option<Vec<usize>>,
    matching: &[u32],
) -> RowSelection {
    let mut selectors = Vec::new();
    let mut base = 0_u64;
    let mut cursor = 0;
    for (group_index, group) in row_groups.iter().enumerate() {
        let end = base + group.num_rows() as u64;
        let start = cursor;
        while cursor < matching.len() && u64::from(matching[cursor]) < end {
            cursor += 1;
        }
        let selected = selected_row_groups
            .as_ref()
            .is_none_or(|groups| groups.contains(&group_index));
        if selected {
            let rebased: Vec<u32> = matching[start..cursor]
                .iter()
                .map(|ordinal| (u64::from(*ordinal) - base) as u32)
                .collect();
            selectors.extend(
                loglake_index::row_selection_runs(&rebased, group.num_rows() as u32)
                    .into_iter()
                    .map(|(selected, length)| {
                        if selected {
                            RowSelector::select(length as usize)
                        } else {
                            RowSelector::skip(length as usize)
                        }
                    }),
            );
        }
        base = end;
    }
    selectors.into()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::metadata::{KeyValue, ParquetMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::{
        ArrowReader, PromotedPruneSpec, RawPruneSpec, index_load_concurrency_from,
        intersect_sorted, union_sorted,
    };
    use crate::delete_vector::DeleteVector;
    use crate::io::FileIO;
    use crate::puffin::{Blob, CompressionCodec, PuffinWriter};
    use crate::scan::{FileScanTask, StatisticsBlobReference};
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    fn bloom(values: &[&str]) -> loglake_bloom::TokenBloom {
        let grams: Vec<String> = values
            .iter()
            .flat_map(|value| loglake_bloom::trigrams(value))
            .collect();
        loglake_bloom::TokenBloom::build(grams.len(), 0.000_001, grams.iter().map(String::as_str))
    }

    fn pruning_fixture() -> (tempfile::TempDir, Arc<ParquetMetaData>, FileScanTask) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pruning.parquet");
        let raws = [
            "database timeout",
            "healthy startup",
            "worker heartbeat",
            "database migrated",
            "api timeout",
            "healthy shutdown",
            "database retry",
            "worker complete",
        ];
        let hosts = [
            "alpha", "alpha", "beta", "beta", "gamma", "gamma", "omega", "omega",
        ];
        let rowgroup_blooms = [bloom(&raws[..4]), bloom(&raws[4..])];
        let index = loglake_index::InvertedIndex::from_rows(raws);
        let metadata = vec![
            KeyValue::new(
                loglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY.to_string(),
                Some(bloom(&raws).to_hex()),
            ),
            KeyValue::new(
                loglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY.to_string(),
                Some(loglake_bloom::rowgroup_blooms_to_hex(&rowgroup_blooms)),
            ),
            KeyValue::new(
                loglake_index::inverted_index_kv_key("raw").into_owned(),
                Some(index.to_hex()),
            ),
        ];
        let field = |name: &str, id: i32| {
            Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field("raw", 1), field("host", 2)]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(raws.to_vec())),
                Arc::new(StringArray::from(hosts.to_vec())),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(metadata))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            arrow_schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let metadata = Arc::new(reader.metadata().clone());
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "raw",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "host",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.to_string_lossy().into_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(true)
            .build();
        (temp, metadata, task)
    }

    #[test]
    fn sorted_set_composition_is_exact() {
        assert_eq!(union_sorted(&[1, 3, 5], &[2, 3, 6]), [1, 2, 3, 5, 6]);
        assert_eq!(intersect_sorted(&[1, 3, 5], &[2, 3, 6]), [3]);
        assert_eq!(index_load_concurrency_from(None), 4);
        assert_eq!(index_load_concurrency_from(Some(" 7 ")), 7);
        assert_eq!(index_load_concurrency_from(Some("0")), 4);
        assert_eq!(index_load_concurrency_from(Some("invalid")), 4);
    }

    #[tokio::test]
    async fn pruning_matrix_is_conservative_selective_and_composes_with_deletes() {
        let (_temp, metadata, task) = pruning_fixture();
        let database = RawPruneSpec {
            all_terms: vec!["database".to_string()],
            ..RawPruneSpec::default()
        };
        assert!(ArrowReader::file_might_match_prune_spec(
            &metadata, &database
        ));
        assert_eq!(
            ArrowReader::rowgroup_bloom_survivors_for_spec(&metadata, &database),
            Some(vec![0, 1])
        );
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &database,
            false,
        )
        .await
        .unwrap()
        .expect("inline index should answer a token query");
        assert_eq!(selection.row_count(), 3, "selective token pruning");

        let substring = RawPruneSpec {
            substrings: vec!["timeout".to_string()],
            index_substrings: vec!["timeout".to_string()],
            ..RawPruneSpec::default()
        };
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &substring,
            true,
        )
        .await
        .unwrap()
        .expect("inline index should answer a substring query when caches are bypassed");
        assert_eq!(selection.row_count(), 2, "selective substring pruning");

        let absent = RawPruneSpec {
            substrings: vec!["zzz-absent".to_string()],
            ..RawPruneSpec::default()
        };
        assert!(!ArrowReader::file_might_match_prune_spec(
            &metadata, &absent
        ));

        let any = RawPruneSpec {
            any_terms: vec!["database".to_string(), "heartbeat".to_string()],
            ..RawPruneSpec::default()
        };
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &any,
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(selection.row_count(), 4, "OR terms use the postings union");

        let promoted = PromotedPruneSpec {
            column: "host".to_string(),
            values: vec!["alpha".to_string()],
        };
        assert!(ArrowReader::row_group_might_match_promoted(
            metadata.row_group(0),
            &promoted
        ));
        assert!(!ArrowReader::row_group_might_match_promoted(
            metadata.row_group(1),
            &promoted
        ));
        let pre_promotion = PromotedPruneSpec {
            column: "column_not_in_file".to_string(),
            values: vec!["alpha".to_string()],
        };
        assert!(ArrowReader::row_group_might_match_promoted(
            metadata.row_group(0),
            &pre_promotion
        ));
        assert!(!ArrowReader::stamped_row_group_size_matches(&metadata, 3));

        let mut deleted = roaring::RoaringTreemap::new();
        deleted.insert(0);
        let delete_selection = ArrowReader::build_deletes_row_selection(
            metadata.row_groups(),
            &None,
            &DeleteVector::new(deleted),
        )
        .unwrap();
        let indexed = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &database,
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            indexed.intersection(&delete_selection).row_count(),
            2,
            "index selection intersects positional deletes"
        );
    }

    #[tokio::test]
    async fn pipeline_results_match_with_cold_warm_and_bypassed_caches() {
        use std::sync::atomic::Ordering::Relaxed;

        use arrow_array::cast::AsArray;
        use futures::TryStreamExt;

        use crate::Runtime;
        use crate::arrow::{ArrowReaderBuilder, ScanCounters};
        use crate::scan::FileScanTaskStream;

        async fn read(
            task: FileScanTask,
            raw: Option<RawPruneSpec>,
            promoted: Vec<PromotedPruneSpec>,
            cache_bypass: bool,
        ) -> (Vec<(String, String)>, Arc<ScanCounters>) {
            let counters = Arc::new(ScanCounters::default());
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                .with_raw_prune_spec(raw)
                .with_promoted_prune(promoted)
                .with_cache_bypass(cache_bypass)
                .with_scan_counters(Some(counters.clone()))
                .build();
            let tasks = Box::pin(futures::stream::iter([Ok(task)])) as FileScanTaskStream;
            let batches: Vec<RecordBatch> = reader
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect()
                .await
                .unwrap();
            let rows = batches
                .iter()
                .flat_map(|batch| {
                    let raw = batch.column(0).as_string::<i32>();
                    let host = batch.column(1).as_string::<i32>();
                    (0..batch.num_rows())
                        .map(|row| (raw.value(row).to_string(), host.value(row).to_string()))
                        .collect::<Vec<_>>()
                })
                .collect();
            (rows, counters)
        }

        let (_temp, _metadata, task) = pruning_fixture();
        let (baseline, _) = read(task.clone(), None, vec![], true).await;
        assert_eq!(baseline.len(), 8);
        let database = RawPruneSpec {
            all_terms: vec!["database".to_string()],
            ..RawPruneSpec::default()
        };
        for cache_bypass in [false, false, true] {
            let (pruned, counters) =
                read(task.clone(), Some(database.clone()), vec![], cache_bypass).await;
            let exact_baseline: Vec<_> = baseline
                .iter()
                .filter(|(raw, _)| raw.contains("database"))
                .cloned()
                .collect();
            assert_eq!(pruned, exact_baseline);
            assert_eq!(counters.rows_pruned_selection.load(Relaxed), 5);
        }

        let promoted = PromotedPruneSpec {
            column: "host".to_string(),
            values: vec!["alpha".to_string()],
        };
        let (hinted, counters) = read(task, None, vec![promoted], false).await;
        let exact_hinted: Vec<_> = hinted
            .iter()
            .filter(|(_, host)| host == "alpha")
            .cloned()
            .collect();
        let exact_baseline: Vec<_> = baseline
            .iter()
            .filter(|(_, host)| host == "alpha")
            .cloned()
            .collect();
        assert_eq!(exact_hinted, exact_baseline);
        assert_eq!(counters.row_groups_pruned_stats.load(Relaxed), 1);
    }

    #[tokio::test]
    async fn puffin_index_is_identical_cold_warm_and_bypassed_and_refuses_stale_stamp() {
        let (_temp, metadata, mut task) = pruning_fixture();
        let file_io = FileIO::new_with_fs();
        let puffin_path = std::path::Path::new(task.data_file_path())
            .with_extension("puffin")
            .to_string_lossy()
            .into_owned();
        let output = file_io.new_output(&puffin_path).unwrap();
        let properties = HashMap::from([
            ("data_file".to_string(), task.data_file_path().to_string()),
            ("column".to_string(), "raw".to_string()),
        ]);
        let index = loglake_index::InvertedIndex::from_rows([
            "database timeout",
            "healthy startup",
            "worker heartbeat",
            "database migrated",
            "api timeout",
            "healthy shutdown",
            "database retry",
            "worker complete",
        ]);
        let blob = Blob::builder()
            .r#type("loglake-inverted-v1".to_string())
            .fields(vec![1])
            .snapshot_id(1)
            .sequence_number(1)
            .data(index.to_bytes())
            .properties(properties.clone())
            .build();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        writer
            .add(blob, CompressionCodec::zstd_default())
            .await
            .unwrap();
        writer.close().await.unwrap();

        task.statistics_blobs = vec![StatisticsBlobReference {
            statistics_path: puffin_path,
            blob_type: "loglake-inverted-v1".to_string(),
            properties: properties
                .into_iter()
                .chain([("row_group_size".to_string(), "4".to_string())])
                .collect(),
        }];
        for cache_bypass in [false, false, true] {
            let index =
                ArrowReader::puffin_inverted_index(&file_io, &task, &metadata, "raw", cache_bypass)
                    .await
                    .unwrap()
                    .expect("valid Puffin index");
            assert_eq!(index.matching_rows_all(&["database"]), [0, 3, 6]);
        }

        task.statistics_blobs[0]
            .properties
            .insert("row_group_size".to_string(), "3".to_string());
        assert!(
            ArrowReader::puffin_inverted_index(&file_io, &task, &metadata, "raw", false)
                .await
                .unwrap()
                .is_none(),
            "a stale row-group stamp must fall back to the exact scan"
        );
    }
}
