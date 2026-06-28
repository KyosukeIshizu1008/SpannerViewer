mod app;
mod csvview;
mod k8s;
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
        eprintln!(
            "接続先が未設定です。GCP 認証（gcloud ADC ログイン、または GOOGLE_APPLICATION_CREDENTIALS \
             のサービスアカウント鍵）の上、右上 or 設定でプロジェクト/インスタンス/DB を選んでください。\
             直接 SPANNER_PROJECT / SPANNER_INSTANCE / SPANNER_DATABASE を指定しても可。\
             エミュレータを使う場合は SPANNER_EMULATOR_HOST を設定してください。"
        );
    }

    let mon_cfg = monitoring::Config { mock };
    let conn_info = if mock {
        "モックモード".to_string()
    } else if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
        format!("エミュレータ · {project}/{instance}/{database}")
    } else {
        format!("{project}/{instance}/{database}")
    };
    let q_cfg = query::Config {
        project,
        instance,
        database,
        mock,
    };

    // 設定パネルから変更できる共有ポーリング間隔
    let poll_interval = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(interval_secs));

    // 監視サンプル: 背景 → UI
    let (sample_tx, sample_rx) = mpsc::channel::<monitoring::Sample>();
    // クエリ要求: UI → 背景（async 受信のため tokio チャネル）。種別付き。
    let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<(query::Target, String)>();
    // クエリ結果: 背景 → UI
    let (res_tx, res_rx) = mpsc::channel::<query::QueryOutcome>();
    // CSV インポート要求: UI → 背景、結果: 背景 → UI
    let (import_req_tx, import_req_rx) = tokio::sync::mpsc::unbounded_channel::<query::ImportRequest>();
    let (import_res_tx, import_res_rx) = mpsc::channel::<query::ImportProgress>();
    // GCS インポート: 要求（取得 / 一覧）UI → 背景、結果 背景 → UI
    let (gcs_req_tx, gcs_req_rx) = tokio::sync::mpsc::unbounded_channel::<query::GcsRequest>();
    let (gcs_res_tx, gcs_res_rx) = mpsc::channel::<query::GcsResponse>();
    // CSV↔DB 照合: 要求 UI → 背景、進捗/結果 背景 → UI
    let (verify_req_tx, verify_req_rx) = tokio::sync::mpsc::unbounded_channel::<query::VerifyRequest>();
    let (verify_res_tx, verify_res_rx) = mpsc::channel::<query::VerifyProgress>();
    // スキーマ図: 背景 → UI
    let (schema_tx, schema_rx) = mpsc::channel::<query::SchemaGraph>();
    // 実行計画: 背景 → UI
    let (plan_tx, plan_rx) = mpsc::channel::<query::PlanOutcome>();
    // k8s 監視: 背景 → UI
    let (kube_metrics_tx, kube_metrics_rx) = mpsc::channel::<k8s::KubeMetrics>();
    // k8s 構成図: 要求 UI → 背景（対象 namespace）、結果 背景 → UI
    let (kube_topo_req_tx, kube_topo_req_rx) =
        tokio::sync::mpsc::unbounded_channel::<Option<String>>();
    let (kube_topo_tx, kube_topo_rx) = mpsc::channel::<k8s::ArchGraph>();
    // k8s ログ追従: 要求 → ストリームイベント
    let (kube_log_req_tx, kube_log_req_rx) = tokio::sync::mpsc::unbounded_channel::<k8s::LogReq>();
    let (kube_log_tx, kube_log_rx) = mpsc::channel::<k8s::LogEvent>();
    // k8s イベント: 要求（対象 namespace） → 結果
    let (kube_ev_req_tx, kube_ev_req_rx) = tokio::sync::mpsc::unbounded_channel::<Option<String>>();
    let (kube_ev_tx, kube_ev_rx) = mpsc::channel::<k8s::EventsResult>();
    // k8s 操作: 要求 → 結果
    let (kube_action_req_tx, kube_action_req_rx) =
        tokio::sync::mpsc::unbounded_channel::<k8s::ActionReq>();
    let (kube_action_tx, kube_action_rx) = mpsc::channel::<k8s::ActionResult>();
    // k8s 汎用リソースブラウザ: 要求 → 結果
    let (kube_res_req_tx, kube_res_req_rx) =
        tokio::sync::mpsc::unbounded_channel::<k8s::ResourceReq>();
    let (kube_res_tx, kube_res_rx) = mpsc::channel::<k8s::ResourceResult>();
    // k8s port-forward: 要求 → 状態イベント
    let (kube_pf_req_tx, kube_pf_req_rx) =
        tokio::sync::mpsc::unbounded_channel::<k8s::PortForwardReq>();
    let (kube_pf_tx, kube_pf_rx) = mpsc::channel::<k8s::PortForwardEvent>();

    // 背景で 1 つのランタイムを回し、各ループを同時実行
    let bg_interval = poll_interval.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(async move {
            // 各ループは独立タスクで起動する。tokio::join! だと 1 つの
            // ループがパニックすると block_on 全体が巻き込まれて背景ランタイム
            // ごと停止し、以後 UI からの送信がすべて静かに失敗する。タスク化
            // すればパニックは当該タスクに封じ込められ、他のループは生き残る。
            let handles = vec![
                tokio::spawn(monitoring::poll_loop(
                    mon_cfg,
                    bg_interval.clone(),
                    sample_tx,
                )),
                tokio::spawn(query::query_loop(
                    q_cfg.clone(),
                    req_rx,
                    res_tx,
                    schema_tx,
                    plan_tx,
                )),
                tokio::spawn(query::import_loop(q_cfg.clone(), import_req_rx, import_res_tx)),
                tokio::spawn(query::gcs_loop(q_cfg.clone(), gcs_req_rx, gcs_res_tx)),
                tokio::spawn(query::verify_loop(q_cfg, verify_req_rx, verify_res_tx)),
                tokio::spawn(k8s::monitor_loop(
                    k8s::Config { mock },
                    bg_interval,
                    kube_metrics_tx,
                )),
                tokio::spawn(k8s::topology_loop(
                    k8s::Config { mock },
                    kube_topo_req_rx,
                    kube_topo_tx,
                )),
                tokio::spawn(k8s::logs_loop(
                    k8s::Config { mock },
                    kube_log_req_rx,
                    kube_log_tx,
                )),
                tokio::spawn(k8s::events_loop(
                    k8s::Config { mock },
                    kube_ev_req_rx,
                    kube_ev_tx,
                )),
                tokio::spawn(k8s::action_loop(
                    k8s::Config { mock },
                    kube_action_req_rx,
                    kube_action_tx,
                )),
                tokio::spawn(k8s::resource_loop(
                    k8s::Config { mock },
                    kube_res_req_rx,
                    kube_res_tx,
                )),
                tokio::spawn(k8s::pf_loop(
                    k8s::Config { mock },
                    kube_pf_req_rx,
                    kube_pf_tx,
                )),
            ];
            for h in handles {
                let _ = h.await;
            }
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
                app::Channels {
                    sample_rx,
                    req_tx,
                    res_rx,
                    import_req_tx,
                    import_res_rx,
                    gcs_req_tx,
                    gcs_res_rx,
                    verify_req_tx,
                    verify_res_rx,
                    schema_rx,
                    plan_rx,
                    kube_metrics_rx,
                    kube_topo_req_tx,
                    kube_topo_rx,
                    kube_log_req_tx,
                    kube_log_rx,
                    kube_ev_req_tx,
                    kube_ev_rx,
                    kube_action_req_tx,
                    kube_action_rx,
                    kube_res_req_tx,
                    kube_res_rx,
                    kube_pf_req_tx,
                    kube_pf_rx,
                    poll_interval,
                    conn_info,
                },
                cc,
            )))
        }),
    )
}
