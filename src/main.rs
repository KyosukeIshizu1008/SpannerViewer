mod app;
mod monitoring;
mod query;

use std::sync::mpsc;

fn main() -> eframe::Result<()> {
    // rustls の暗号プロバイダを明示選択（aws-lc-rs と ring が両方ツリーにあるため）。
    // 既にインストール済みでもエラーにはならないので無視してよい。
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // 設定は環境変数から（雛形なので最小構成）
    let project = std::env::var("SPANNER_PROJECT").unwrap_or_default();
    let instance = std::env::var("SPANNER_INSTANCE").unwrap_or_default();
    let database = std::env::var("SPANNER_DATABASE").unwrap_or_default();

    // MONITOR_MOCK=1 で合成データモード（実 Spanner / 認証不要）
    let mock = matches!(
        std::env::var("MONITOR_MOCK").ok().as_deref(),
        Some("1") | Some("true")
    );

    // モック時は間隔を短く（既定2秒）してグラフの動きを見やすくする
    let default_interval = if mock { 2 } else { 30 };
    let interval_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_interval);

    if mock {
        eprintln!("モックモードで起動します（MONITOR_MOCK=1）。合成データを表示します。");
    } else if project.is_empty() || instance.is_empty() {
        eprintln!("環境変数 SPANNER_PROJECT と SPANNER_INSTANCE を設定してください。");
    }

    let mon_cfg = monitoring::Config {
        project: project.clone(),
        instance: instance.clone(),
        interval_secs,
        mock,
    };
    let q_cfg = query::Config {
        project,
        instance,
        database,
        mock,
    };

    // 監視サンプル: 背景 → UI
    let (sample_tx, sample_rx) = mpsc::channel::<monitoring::Sample>();
    // クエリ要求: UI → 背景（async 受信のため tokio チャネル）。種別付き。
    let (req_tx, req_rx) =
        tokio::sync::mpsc::unbounded_channel::<(query::Target, String)>();
    // クエリ結果: 背景 → UI
    let (res_tx, res_rx) = mpsc::channel::<query::QueryOutcome>();
    // スキーマ図: 背景 → UI
    let (schema_tx, schema_rx) = mpsc::channel::<query::SchemaGraph>();

    // 背景で 1 つのランタイムを回し、監視ループとクエリループを同時実行
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async move {
            tokio::join!(
                monitoring::poll_loop(mon_cfg, sample_tx),
                query::query_loop(q_cfg, req_rx, res_tx, schema_tx),
            );
        });
    });

    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_inner_size([1000.0, 720.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Spanner Viewer",
        native_options,
        Box::new(|cc| {
            Ok(Box::new(app::MonitorApp::new(
                cc, sample_rx, req_tx, res_rx, schema_rx,
            )))
        }),
    )
}
