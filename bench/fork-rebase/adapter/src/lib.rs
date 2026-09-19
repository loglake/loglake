//! Caller adapters for the two 0.9 fork actions replaced by upstream 0.10.1.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Mutex;

use async_trait::async_trait;
use iceberg::spec::{TableMetadata, Type};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

/// An expiry policy expressed in LogLake's caller terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryMode {
    /// Expire by retained count alone. `i64::MAX` deliberately disables
    /// upstream's implicit five-day boundary so the count is authoritative.
    CountOnly { retain_last: usize },
    /// Apply both the retained-count floor and an explicit age boundary.
    AgeAndCount {
        retain_last: usize,
        older_than_ms: i64,
    },
}

/// Metadata-only result of asking upstream 0.10.1 to plan an expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryPreview {
    pub snapshot_ids: Vec<i64>,
    pub ref_names: Vec<String>,
}

impl ExpiryPreview {
    pub fn is_empty(&self) -> bool {
        self.snapshot_ids.is_empty() && self.ref_names.is_empty()
    }
}

/// Build LogLake's additive, replay-safe schema transaction with upstream's
/// richer schema action. Existing names are filtered before the action is
/// built; type drift remains an error rather than becoming a silent no-op.
pub fn additive_schema_transaction(
    table: &Table,
    desired: &[(String, Type)],
) -> Result<(usize, Option<Transaction>)> {
    let missing = missing_optional_columns(table, desired)?;
    if missing.is_empty() {
        return Ok((0, None));
    }

    let tx = Transaction::new(table);
    let mut action = tx.update_schema();
    for (name, field_type) in &missing {
        action = action.add_column(AddColumn::optional(name, field_type.clone()));
    }
    let tx = action.apply(tx)?;
    Ok((missing.len(), Some(tx)))
}

/// Preview the schema metadata produced by the same upstream action without a
/// catalog write. This drives normalized old/new fixture comparisons.
pub async fn preview_additive_schema(
    table: &Table,
    desired: &[(String, Type)],
) -> Result<TableMetadata> {
    let missing = missing_optional_columns(table, desired)?;
    if missing.is_empty() {
        return Ok(table.metadata().clone());
    }

    let tx = build_schema_transaction(table, &missing)?;
    Ok(commit_preview(table, tx).await?.metadata().clone())
}

/// Ask upstream for the exact metadata plan. No catalog or object-store write
/// occurs, so this is also the accurate dry-run/count path.
pub async fn preview_expiry(table: &Table, mode: ExpiryMode) -> Result<ExpiryPreview> {
    let after = commit_preview(table, build_expiry_transaction(table, mode)?).await?;
    let after_ids: HashSet<i64> = after
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    let mut snapshot_ids: Vec<i64> = table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .filter(|id| !after_ids.contains(id))
        .collect();
    let after_refs = ref_names(after.metadata())?;
    let mut ref_names: Vec<String> = ref_names(table.metadata())?
        .difference(&after_refs)
        .cloned()
        .collect();
    snapshot_ids.sort_unstable();
    ref_names.sort();
    Ok(ExpiryPreview {
        snapshot_ids,
        ref_names,
    })
}

/// Build the real transaction only when preview proved there is work. This
/// keeps the 0.9 caller guarantee that a no-op expiry emits no catalog commit.
pub async fn expiry_transaction(
    table: &Table,
    mode: ExpiryMode,
) -> Result<(ExpiryPreview, Option<Transaction>)> {
    let preview = preview_expiry(table, mode).await?;
    if preview.is_empty() {
        return Ok((preview, None));
    }
    let tx = build_expiry_transaction(table, mode)?;
    Ok((preview, Some(tx)))
}

fn missing_optional_columns(
    table: &Table,
    desired: &[(String, Type)],
) -> Result<Vec<(String, Type)>> {
    let current = table.metadata().current_schema();
    let mut seen = HashSet::new();
    let mut missing = Vec::new();
    for (name, field_type) in desired {
        if !seen.insert(name.as_str()) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("duplicate desired column {name}"),
            ));
        }
        if let Some(id) = current.field_id_by_name(name) {
            let existing = current
                .as_struct()
                .field_by_id(id)
                .expect("field id resolved from the same schema");
            if existing.field_type.as_ref() != field_type {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("column {name} has a different stored type"),
                ));
            }
        } else {
            missing.push((name.clone(), field_type.clone()));
        }
    }
    Ok(missing)
}

fn build_schema_transaction(table: &Table, missing: &[(String, Type)]) -> Result<Transaction> {
    let tx = Transaction::new(table);
    let mut action = tx.update_schema();
    for (name, field_type) in missing {
        action = action.add_column(AddColumn::optional(name, field_type.clone()));
    }
    action.apply(tx)
}

fn build_expiry_transaction(table: &Table, mode: ExpiryMode) -> Result<Transaction> {
    let tx = Transaction::new(table);
    match mode {
        ExpiryMode::CountOnly { retain_last } => tx
            .expire_snapshots()
            .retain_last(retain_last.max(1))
            .expire_older_than_ms(i64::MAX)
            .apply(tx),
        ExpiryMode::AgeAndCount {
            retain_last,
            older_than_ms,
        } => tx
            .expire_snapshots()
            .retain_last(retain_last.max(1))
            .expire_older_than_ms(older_than_ms)
            .apply(tx),
    }
}

fn ref_names(metadata: &TableMetadata) -> Result<HashSet<String>> {
    let value = serde_json::to_value(metadata)?;
    Ok(value
        .get("refs")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|refs| refs.keys().cloned())
        .collect())
}

async fn commit_preview(table: &Table, tx: Transaction) -> Result<Table> {
    let catalog = PreviewCatalog::new(table.clone());
    tx.commit(&catalog).await
}

/// A catalog that applies metadata commits in memory. It deliberately has no
/// object-store mutation path, which makes expiry preview non-destructive.
struct PreviewCatalog {
    table: Mutex<Table>,
}

impl PreviewCatalog {
    fn new(table: Table) -> Self {
        Self {
            table: Mutex::new(table),
        }
    }

    fn unsupported<T>() -> Result<T> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "preview catalog supports load/update only",
        ))
    }
}

impl Debug for PreviewCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PreviewCatalog").finish()
    }
}

#[async_trait]
impl Catalog for PreviewCatalog {
    async fn list_namespaces(&self, _: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        Self::unsupported()
    }

    async fn create_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> Result<Namespace> {
        Self::unsupported()
    }

    async fn get_namespace(&self, _: &NamespaceIdent) -> Result<Namespace> {
        Self::unsupported()
    }

    async fn namespace_exists(&self, _: &NamespaceIdent) -> Result<bool> {
        Self::unsupported()
    }

    async fn update_namespace(&self, _: &NamespaceIdent, _: HashMap<String, String>) -> Result<()> {
        Self::unsupported()
    }

    async fn drop_namespace(&self, _: &NamespaceIdent) -> Result<()> {
        Self::unsupported()
    }

    async fn list_tables(&self, _: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Self::unsupported()
    }

    async fn create_table(&self, _: &NamespaceIdent, _: TableCreation) -> Result<Table> {
        Self::unsupported()
    }

    async fn load_table(&self, _: &TableIdent) -> Result<Table> {
        Ok(self.table.lock().expect("preview catalog lock").clone())
    }

    async fn drop_table(&self, _: &TableIdent) -> Result<()> {
        Self::unsupported()
    }

    async fn purge_table(&self, _: &TableIdent) -> Result<()> {
        Self::unsupported()
    }

    async fn table_exists(&self, _: &TableIdent) -> Result<bool> {
        Ok(true)
    }

    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> Result<()> {
        Self::unsupported()
    }

    async fn register_table(&self, _: &TableIdent, _: String) -> Result<Table> {
        Self::unsupported()
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let mut table = self.table.lock().expect("preview catalog lock");
        let updated = commit.apply(table.clone())?;
        *table = updated.clone();
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::io::FileIO;
    use iceberg::spec::{PrimitiveType, TableMetadata, Type};
    use iceberg::table::Table;
    use iceberg::{NamespaceIdent, Runtime, TableIdent};

    use super::*;

    fn table_from_json(json: &str) -> Table {
        let mut value: serde_json::Value = serde_json::from_str(json).unwrap();
        // One upstream empty-table fixture predates its own added columns and
        // carries a stale last-column-id. Normalize that packaging artifact.
        let max_id = value["schemas"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|schema| schema["fields"].as_array().into_iter().flatten())
            .filter_map(|field| field["id"].as_i64())
            .max()
            .unwrap_or_default();
        if value["last-column-id"].as_i64().unwrap_or_default() < max_id {
            value["last-column-id"] = max_id.into();
        }
        let metadata: TableMetadata = serde_json::from_value(value).unwrap();
        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("fixture".to_string()),
                "events".to_string(),
            ))
            .metadata(metadata)
            .metadata_location(
                "memory://warehouse/events/metadata/00001-00000000-0000-0000-0000-000000000001.metadata.json",
            )
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::current())
            .build()
            .unwrap()
    }

    fn desired() -> Vec<(String, Type)> {
        vec![
            ("x".to_string(), Type::Primitive(PrimitiveType::Long)),
            (
                "severity".to_string(),
                Type::Primitive(PrimitiveType::String),
            ),
            ("score".to_string(), Type::Primitive(PrimitiveType::Long)),
        ]
    }

    #[tokio::test]
    async fn repeated_optional_additions_are_a_noop_and_old_rows_remain_nullable() {
        let table = table_from_json(include_str!(
            "../../candidate/iceberg/testdata/example_empty_table_metadata_v2.json"
        ));
        let (added, transaction) = additive_schema_transaction(&table, &desired()).unwrap();
        assert_eq!(added, 2);
        assert!(transaction.is_some());

        let metadata = preview_additive_schema(&table, &desired()).await.unwrap();
        let widened = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(metadata)
            .metadata_location(
                "memory://warehouse/events/metadata/00002-00000000-0000-0000-0000-000000000002.metadata.json",
            )
            .file_io(FileIO::new_with_memory())
            .runtime(Runtime::current())
            .build()
            .unwrap();
        for name in ["severity", "score"] {
            let id = widened
                .metadata()
                .current_schema()
                .field_id_by_name(name)
                .unwrap();
            let field = widened
                .metadata()
                .current_schema()
                .as_struct()
                .field_by_id(id)
                .unwrap();
            assert!(!field.required, "old files fill {name} with null");
        }
        let (added_again, transaction_again) =
            additive_schema_transaction(&widened, &desired()).unwrap();
        assert_eq!(added_again, 0);
        assert!(transaction_again.is_none(), "no empty schema commit");
    }

    #[tokio::test]
    async fn count_only_expiry_has_exact_preview_and_keeps_head_and_refs() {
        let table = table_from_json(include_str!(
            "../../candidate/iceberg/testdata/example_table_metadata_v2_deep_history.json"
        ));
        let mode = ExpiryMode::CountOnly { retain_last: 2 };
        let (preview, transaction) = expiry_transaction(&table, mode).await.unwrap();
        assert_eq!(preview.snapshot_ids.len(), 3);
        assert!(preview.ref_names.is_empty());
        assert!(transaction.is_some());
        let current = table.metadata().current_snapshot_id().unwrap();
        assert!(!preview.snapshot_ids.contains(&current));
        assert_eq!(
            table
                .metadata()
                .snapshot_for_ref("main")
                .unwrap()
                .snapshot_id(),
            current
        );

        let keep_all = ExpiryMode::CountOnly { retain_last: 5 };
        let (empty, transaction) = expiry_transaction(&table, keep_all).await.unwrap();
        assert!(empty.is_empty());
        assert!(transaction.is_none(), "no empty expiry commit");
    }

    #[tokio::test]
    async fn age_and_count_is_planned_separately_and_remains_metadata_only() {
        let table = table_from_json(include_str!(
            "../../candidate/iceberg/testdata/example_table_metadata_v2_deep_history.json"
        ));
        let preview = preview_expiry(
            &table,
            ExpiryMode::AgeAndCount {
                retain_last: 1,
                older_than_ms: 1_580_000_000_000,
            },
        )
        .await
        .unwrap();
        assert_eq!(preview.snapshot_ids.len(), 3);
        assert!(preview.ref_names.is_empty());
        // The upstream action returned TableUpdate values only. The adapter has
        // no FileIO delete path, so preview and apply perform no physical deletion.
    }
}
