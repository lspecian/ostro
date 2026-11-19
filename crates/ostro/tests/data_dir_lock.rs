//! Exclusive data-dir lock through the real binary (plan 05-26): a second `ostro dev` on
//! a data dir that a running `ostro dev` holds exits nonzero with one clear stderr line;
//! after the holder is SIGKILLed (its `LOCK` file stays behind) the next start succeeds,
//! because the flock belongs to the dead process's descriptor, not to the file.
#![cfg(unix)]

mod common;

use common::{SERIAL, Spawned, T, dev, free_addr, http, run_to_end};

#[tokio::test]
async fn second_dev_on_same_dir_fails_and_sigkill_releases_the_lock() {
    let _g = SERIAL.lock().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let data = tmp.path().join("data");
    let data_arg = data.to_str().expect("utf-8 path").to_owned();
    let client = http();

    let addr = free_addr();
    let mut first = Spawned::spawn(dev(
        tmp.path(),
        &["--data-dir", &data_arg, "--listen", &addr.to_string()],
    ));
    first.wait_readyz(&client, addr).await;
    assert_eq!(first.ready_line(T).data_dir, data);

    // Second process, same dir, another port: fails fast with the in-use error.
    let addr2 = free_addr();
    let (st, out, err) = run_to_end(dev(
        tmp.path(),
        &["--data-dir", &data_arg, "--listen", &addr2.to_string()],
    ));
    assert!(
        !st.success(),
        "second start must fail: {st:?}\n{out}\n{err}"
    );
    assert_eq!(st.code(), Some(1), "{err}");
    let lock = data.join("LOCK");
    let expected = format!(
        "ostro: error: data dir {} is in use by another ostro process (lock {})",
        data.display(),
        lock.display()
    );
    let lines: Vec<&str> = err
        .lines()
        .filter(|l| l.starts_with("ostro: error:"))
        .collect();
    assert_eq!(lines, vec![expected.as_str()], "stderr: {err}");
    assert!(
        !out.contains("listening on"),
        "the second process never served: {out}"
    );

    // The first server still serves.
    let r = client
        .get(format!("http://{addr}/readyz"))
        .send()
        .await
        .expect("readyz");
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    // SIGKILL: no shutdown path runs, the LOCK file stays, the OS drops the flock.
    first.signal("KILL");
    let st = first.wait_exit(T);
    assert!(!st.success(), "killed: {st:?}");
    assert!(lock.is_file(), "stale LOCK file left behind");

    let addr3 = free_addr();
    let mut third = Spawned::spawn(dev(
        tmp.path(),
        &["--data-dir", &data_arg, "--listen", &addr3.to_string()],
    ));
    third.wait_readyz(&client, addr3).await;
    assert_eq!(third.ready_line(T).data_dir, data);
    third.stop_clean("INT");

    // And a graceful stop released it again.
    let addr4 = free_addr();
    let mut fourth = Spawned::spawn(dev(
        tmp.path(),
        &["--data-dir", &data_arg, "--listen", &addr4.to_string()],
    ));
    fourth.wait_readyz(&client, addr4).await;
    fourth.stop_clean("TERM");
}
