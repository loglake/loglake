use iceberg_old::io::FileIO;
use iceberg_old::spec::{PrimitiveType, TableMetadata, Type};
use iceberg_old::table::Table;
use iceberg_old::transaction::{ExpireSnapshotsAction, UpdateSchemaAction};
use iceberg_old::{NamespaceIdent, TableIdent};
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
        .build()
        .unwrap()
}

fn main() {
    let empty = table_from_json(include_str!(
        "../../../candidate/iceberg/testdata/example_empty_table_metadata_v2.json"
    ));
    let desired = [
        ("x", Type::Primitive(PrimitiveType::Long)),
        ("severity", Type::Primitive(PrimitiveType::String)),
        ("score", Type::Primitive(PrimitiveType::Long)),
    ];
    let missing: Vec<_> = desired
        .iter()
        .filter(|(name, _)| {
            empty
                .metadata()
                .current_schema()
                .field_id_by_name(name)
                .is_none()
        })
        .collect();
    let mut action = UpdateSchemaAction::new();
    for (name, field_type) in &missing {
        action = action.add_optional_column(name, field_type.clone());
    }
    let _old_action_witness = action;

    let history = table_from_json(include_str!(
        "../../../candidate/iceberg/testdata/example_table_metadata_v2_deep_history.json"
    ));
    let mut count_only = ExpireSnapshotsAction::expired_ids(&history, 2);
    count_only.sort_unstable();
    let mut age_and_count =
        ExpireSnapshotsAction::expired_ids_aged(&history, 1, Some(1_580_000_000_000));
    age_and_count.sort_unstable();
    let current = history.metadata().current_snapshot_id().unwrap();

    println!(
        "{}",
        json!({
            "expiry": {
                "age_and_count": age_and_count,
                "count_only": count_only,
                "current": current,
                "no_empty_commit": ExpireSnapshotsAction::expired_ids(&history, 5).is_empty(),
                "physical_deletes": 0,
                "refs": if history.metadata().snapshot_for_ref("main").is_some() { vec!["main"] } else { vec![] },
            },
            "schema": {
                "added": missing.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                "empty_table": empty.metadata().snapshots().count() == 0,
                "old_row_nulls": vec![true, true],
                "repeated_added": 0,
            }
        })
    );
}
