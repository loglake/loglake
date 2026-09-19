use std::process::Command;

#[test]
fn old_and_new_processes_emit_the_same_normalized_contract() {
    let old = Command::new(env!("CARGO_BIN_EXE_old-contract"))
        .output()
        .expect("run old contract process");
    assert!(old.status.success(), "old process failed: {old:?}");

    let new = Command::new(env!("CARGO_BIN_EXE_new-contract"))
        .output()
        .expect("run new contract process");
    assert!(new.status.success(), "new process failed: {new:?}");

    assert_eq!(
        old.stdout,
        new.stdout,
        "normalized JSON changed across the 0.9/0.10 process boundary\nold: {}\nnew: {}",
        String::from_utf8_lossy(&old.stdout),
        String::from_utf8_lossy(&new.stdout),
    );
}
