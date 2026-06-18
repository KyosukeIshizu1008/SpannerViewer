//! Spanner に並行 INSERT 負荷をかけて、監視ツールの CPU グラフを動かすための負荷生成器。
//!
//! 事前にテーブルを作成しておくこと（デフォルト名 LoadTest）:
//!   CREATE TABLE LoadTest (
//!     Id      STRING(36) NOT NULL,
//!     Payload STRING(MAX),
//!   ) PRIMARY KEY (Id);
//!
//! 実行:
//!   export SPANNER_PROJECT=your-project
//!   export SPANNER_INSTANCE=your-instance
//!   export SPANNER_DATABASE=your-db
//!   export LOAD_CONCURRENCY=32      # 同時実行ワーカー数（負荷の主な調整つまみ）
//!   export LOAD_BATCH=100           # 1コミットあたりの行数
//!   export LOAD_DURATION_SECS=120   # 省略時は Ctrl-C まで継続
//!   cargo run --release --bin loadgen

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use gcloud_spanner::client::{Client, ClientConfig};
use gcloud_spanner::mutation::insert;
use gcloud_spanner::statement::ToKind;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let project = env_required("SPANNER_PROJECT")?;
    let instance = env_required("SPANNER_INSTANCE")?;
    let database = env_required("SPANNER_DATABASE")?;
    let table = env_or("LOAD_TABLE", "LoadTest");
    let concurrency: usize = env_parse("LOAD_CONCURRENCY", 32);
    let batch: usize = env_parse("LOAD_BATCH", 100);
    let duration_secs: Option<u64> = std::env::var("LOAD_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok());

    let db = format!("projects/{project}/instances/{instance}/databases/{database}");
    println!("接続先: {db}");
    println!(
        "table={table} concurrency={concurrency} batch={batch} duration={}",
        duration_secs
            .map(|d| format!("{d}s"))
            .unwrap_or_else(|| "Ctrl-C まで".into())
    );

    // ADC 認証でクライアント生成
    let config = ClientConfig::default()
        .with_auth()
        .await
        .context("ADC 認証に失敗。`gcloud auth application-default login` を確認")?;
    let client = Arc::new(
        Client::new(&db, config)
            .await
            .context("Spanner クライアント生成に失敗")?,
    );

    let total = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let deadline = duration_secs.map(|d| start + Duration::from_secs(d));

    // Ctrl-C で停止フラグを立てる
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("\n停止要求を受信。ワーカーを止めています…");
                shutdown.store(true, Ordering::SeqCst);
            }
        });
    }

    // 進捗レポーター（2秒ごとに rows/sec を表示）
    {
        let total = total.clone();
        let errors = errors.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let now = total.load(Ordering::Relaxed);
                let rate = (now - last) as f64 / 2.0;
                last = now;
                println!(
                    "  inserted={now} errors={} rate={rate:.0} rows/s",
                    errors.load(Ordering::Relaxed)
                );
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
            }
        });
    }

    // ワーカー起動
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let total = total.clone();
        let errors = errors.clone();
        let shutdown = shutdown.clone();
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            while !shutdown.load(Ordering::SeqCst) {
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        break;
                    }
                }

                let mut mutations = Vec::with_capacity(batch);
                for _ in 0..batch {
                    let id = Uuid::new_v4().to_string();
                    let payload = format!("payload-{}", Uuid::new_v4());
                    let cols = ["Id", "Payload"];
                    let vals: [&dyn ToKind; 2] = [&id, &payload];
                    mutations.push(insert(&table, &cols, &vals));
                }

                match client.apply(mutations).await {
                    Ok(_) => {
                        total.fetch_add(batch as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!("commit error: {e}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    shutdown.store(true, Ordering::SeqCst);

    let elapsed = start.elapsed().as_secs_f64();
    let inserted = total.load(Ordering::Relaxed);
    println!(
        "\n完了: {inserted} 行を {elapsed:.1}s で挿入 (平均 {:.0} rows/s, errors={})",
        inserted as f64 / elapsed.max(0.001),
        errors.load(Ordering::Relaxed)
    );

    Ok(())
}

fn env_required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).with_context(|| format!("環境変数 {key} が未設定です"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
