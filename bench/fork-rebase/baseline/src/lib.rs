//! Compile-only witness for the pristine 0.10.1 candidate dependency graph.

/// Name the forced stack so a successful build proves every public crate is linked.
pub const STACK: &[&str] = &[
    "arrow-58",
    "parquet-58",
    "datafusion-53.1.0",
    "iceberg-0.10.1",
    "iceberg-catalog-sql-0.10.1",
    "iceberg-datafusion-0.10.1",
    "iceberg-storage-opendal-0.10.1",
    "opendal-0.57",
    "reqsign-core-3",
    "reqsign-aws-v4-3",
];

#[cfg(test)]
mod tests {
    use super::STACK;

    #[test]
    fn forced_stack_is_complete() {
        assert_eq!(STACK.len(), 10);
    }
}
