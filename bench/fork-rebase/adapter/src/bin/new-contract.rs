use fork_rebase_adapter::{
    ExpiryMode, additive_schema_transaction, preview_additive_schema, preview_expiry,
};
use iceberg::io::FileIO;
use iceberg::spec::{PrimitiveType, TableMetadata, Type};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, Runtime, TableIdent};
use serde_json::json;

fn table_from_json(json: &str) -> Table {
    let mut value: serde_json::Value = serde_json::from_str(json).unwrap();
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
        .metadata_location("memory://warehouse/events/metadata/00001-00000000-0000-0000-0000-000000000001.metadata.json")
        .file_io(FileIO::new_with_memory())
        .runtime(Runtime::current())
        .build()
        .unwrap()
}

#[tokio::main]
async fn main() {
    let empty = table_from_json(include_str!(
        "../../../candidate/iceberg/testdata/example_empty_table_metadata_v2.json"
    ));
    let desired = vec![
        ("x".to_string(), Type::Primitive(PrimitiveType::Long)),
        (
            "severity".to_string(),
            Type::Primitive(PrimitiveType::String),
        ),
        ("score".to_string(), Type::Primitive(PrimitiveType::Long)),
    ];
    let (added, transaction) = additive_schema_transaction(&empty, &desired).unwrap();
    assert_eq!(added, 2);
    assert!(transaction.is_some());
    let widened_metadata = preview_additive_schema(&empty, &desired).await.unwrap();
    let widened = Table::builder()
        .identifier(empty.identifier().clone())
        .metadata(widened_metadata)
        .metadata_location("memory://warehouse/events/metadata/00002-00000000-0000-0000-0000-000000000002.metadata.json")
        .file_io(FileIO::new_with_memory())
        .runtime(Runtime::current())
        .build()
        .unwrap();
    let (repeated_added, repeated_tx) = additive_schema_transaction(&widened, &desired).unwrap();
    assert!(repeated_tx.is_none());

    let history = table_from_json(include_str!(
        "../../../candidate/iceberg/testdata/example_table_metadata_v2_deep_history.json"
    ));
    let count_only = preview_expiry(&history, ExpiryMode::CountOnly { retain_last: 2 })
        .await
        .unwrap();
    let age_and_count = preview_expiry(
        &history,
        ExpiryMode::AgeAndCount {
            retain_last: 1,
            older_than_ms: 1_580_000_000_000,
        },
    )
    .await
    .unwrap();
    let no_op = preview_expiry(&history, ExpiryMode::CountOnly { retain_last: 5 })
        .await
        .unwrap();
    let current = history.metadata().current_snapshot_id().unwrap();
    let nullable = ["severity", "score"].iter().map(|name| {
        let id = widened
            .metadata()
            .current_schema()
            .field_id_by_name(name)
            .unwrap();
        !widened
            .metadata()
            .current_schema()
            .as_struct()
            .field_by_id(id)
            .unwrap()
            .required
    });

    println!(
        "{}",
        json!({
            "expiry": {
                "age_and_count": age_and_count.snapshot_ids,
                "count_only": count_only.snapshot_ids,
                "current": current,
                "no_empty_commit": no_op.is_empty(),
                "physical_deletes": 0,
                "refs": if history.metadata().snapshot_for_ref("main").is_some() { vec!["main"] } else { vec![] },
            },
            "schema": {
                "added": ["severity", "score"],
                "empty_table": empty.metadata().snapshots().count() == 0,
                "old_row_nulls": nullable.collect::<Vec<_>>(),
                "repeated_added": repeated_added,
            }
        })
    );
}
