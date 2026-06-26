use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use eframe::egui;
use egui_plot::{Legend, Line, Plot, PlotPoints};
use tokio::sync::mpsc::UnboundedSender;

use crate::k8s;
use crate::monitoring::Sample;
use crate::query::{self, EdgeKind, QueryOutcome, SchemaGraph, TableNode, Target};

// ── カラーパレット（モダンダーク） ──
const ACCENT: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const CPU_COLOR: egui::Color32 = egui::Color32::from_rgb(251, 146, 60); // amber/orange
const STORAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const TEXT: egui::Color32 = egui::Color32::from_rgb(226, 232, 240); // 明るいテキスト
const MUTED: egui::Color32 = egui::Color32::from_rgb(148, 163, 184); // 補助テキスト
/// 構成図の通信矢印（Service→Pod）。
const COMM_COLOR: egui::Color32 = egui::Color32::from_rgb(52, 211, 153); // emerald

/// 背景ワーカーへの送信失敗時に表示するメッセージ。
const WORKER_GONE: &str = "バックグラウンド処理が停止しています。アプリを再起動してください。";

#[derive(PartialEq, Eq, Clone, Copy)]
enum Section {
    Spanner,
    Kube,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum View {
    Monitor,
    Data,
    Schema,
    Import,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum KubeView {
    Monitor,
    Resources,
    Diagram,
    Events,
}

/// アクティブな port-forward の 1 エントリ。
struct PfEntry {
    id: u64,
    label: String,
    status: String,
    active: bool,
}

/// CSV インポートダイアログの状態。テーブルの「CSV をインポート」から開く。
struct ImportDialog {
    /// インポート先テーブル名。
    table: String,
    /// テーブルのカラム（名前・型・PK）。マッピング表示と型変換に使う。
    table_columns: Vec<query::Column>,
    /// 取得元（ファイル/GCS）。インポート時はここからストリーミングする。
    source: query::ImportSource,
    /// 表示用のソース名（ファイル名 / gs:// パス）。
    file_name: String,
    /// プレビュー用の生バイト（文字コード/区切り変更時に再パースする。先頭のみ）。
    preview_bytes: Vec<u8>,
    /// マッピング表示用のプレビュー行（先頭の数行のみ。全行は溜めない）。
    records: Vec<Vec<String>>,
    /// 文字コード。
    encoding: query::Encoding,
    /// 区切り文字。
    delimiter: u8,
    /// 不正行をスキップして続行するか。
    skip_bad_rows: bool,
    /// NULL として扱う文字列（空なら無効）。
    null_token: String,
    /// 先頭行をヘッダとして扱うか。
    has_header: bool,
    /// CSV 側の列見出し（has_header に応じて算出）。
    csv_headers: Vec<String>,
    /// テーブル各カラムに割り当てる CSV 列インデックス（None = スキップ）。
    /// `table_columns` と同じ並び・長さ。
    mapping: Vec<Option<usize>>,
    mode: query::ImportMode,
    empty_as_null: bool,
    /// 前回の途中（チェックポイント）を無視して最初からやり直すか。
    fresh: bool,
    /// パース時の注記（プレビュー打ち切りなど）。
    note: Option<String>,
    /// 設定エラー（マッピング未指定など）の即時表示。
    config_msg: Option<String>,
}

/// 取込中の進捗スナップショット（リアルタイム表示用）。
struct ImportProg {
    /// 進捗割合 0.0..1.0（ソース全体サイズが不明なら None＝不確定表示）。
    frac: Option<f32>,
    /// これまでに書き込めた行数。
    written: usize,
    /// 読み出した累積バイト数。
    bytes_done: u64,
    /// ソース全体のバイト数（不明なら None）。
    bytes_total: Option<u64>,
}

/// インポートジョブの状態。
#[derive(Clone, Copy, PartialEq, Eq)]
enum JobStatus {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

/// 順次キューの 1 ジョブ。複数テーブルを並べて投入できる。
struct ImportJob {
    /// 正規リクエスト（送信時はこれを clone する。cancel は送信クローンと共有）。
    req: query::ImportRequest,
    /// 表示用のソース名。
    source_name: String,
    /// 背景へ送信済みか。
    sent: bool,
    status: JobStatus,
    /// 実行開始時刻（ETA/速度の算出用）。
    started: Option<std::time::Instant>,
    progress: Option<ImportProg>,
    result: Option<String>,
    /// 完了時の結果（レポート用の件数など）。
    outcome: Option<query::ImportOutcome>,
}

impl ImportJob {
    fn is_active(&self) -> bool {
        matches!(self.status, JobStatus::Queued | JobStatus::Running)
    }
}

/// GCS フォルダ一括投入の保留状態。List 応答で各 CSV を enqueue するための雛形。
struct BulkSpec {
    /// 雛形リクエスト（source を各オブジェクトに差し替えて使う）。
    template: query::ImportRequest,
}

impl ImportDialog {
    /// has_header に応じて CSV 見出しと自動マッピングを作り直す。
    fn recompute(&mut self) {
        let ncols = self.records.iter().map(|r| r.len()).max().unwrap_or(0);
        self.csv_headers = if self.has_header {
            let head = self.records.first().cloned().unwrap_or_default();
            (0..ncols)
                .map(|i| {
                    head.get(i)
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("列{}", i + 1))
                })
                .collect()
        } else {
            (0..ncols).map(|i| format!("列{}", i + 1)).collect()
        };
        // テーブル列名 → CSV 見出し（大文字小文字・前後空白を無視）で自動対応付け。
        let lower: Vec<String> = self
            .csv_headers
            .iter()
            .map(|h| h.trim().to_lowercase())
            .collect();
        self.mapping = self
            .table_columns
            .iter()
            .map(|c| {
                let name = c.name.trim().to_lowercase();
                lower.iter().position(|h| *h == name)
            })
            .collect();
    }

    /// 文字コード/区切りを変えたとき、プレビューを生バイトから再パースして
    /// 見出し・マッピングを作り直す。
    fn reparse_preview(&mut self) {
        self.records =
            query::parse_preview(&self.preview_bytes, self.encoding, self.delimiter, PREVIEW_ROWS + 1);
        self.recompute();
    }

    /// ヘッダを除いたデータ行。
    fn data_rows(&self) -> &[Vec<String>] {
        if self.has_header && !self.records.is_empty() {
            &self.records[1..]
        } else {
            &self.records
        }
    }

    fn data_rows_count(&self) -> usize {
        self.data_rows().len()
    }
}

/// GCS から CSV を取り込むための入力ダイアログ。
/// URI を入力 → 背景で取得 → 成功したら ImportDialog（マッピング画面）へ引き継ぐ。
struct GcsDialog {
    /// インポート先テーブル（取得成功後に ImportDialog へ渡す）。
    target: TableNode,
    /// 入力中の `gs://bucket/path.csv`。
    uri: String,
    /// 進捗・エラー表示。
    status: Option<String>,
    /// 一覧したバケット名（folders/objects から URI を組み立てるのに使う）。
    bucket: String,
    /// 一覧結果: 直下の擬似フォルダ（末尾 / 付きフルパス）。
    folders: Vec<String>,
    /// 一覧結果: 直下のオブジェクト（フルパス）。
    objects: Vec<String>,
    /// 現在一覧している `gs://bucket/prefix`（ブラウズ位置の見出し）。
    listed_at: Option<String>,
}

/// リソースブラウザの行操作（描画中に収集し、借用解消後に実行する）。
enum RowAction {
    Yaml(Option<String>, String),
    Describe(Option<String>, String),
    EditYaml(Option<String>, String),
    Logs(String, String),
    Exec(String, String),        // ns, pod
    PortForward(String, String), // ns, target（"pod/foo" / "svc/bar"）
    Restart(Option<String>, String),
    Scale(Option<String>, String, i32),
    Delete(Option<String>, String),
}

/// リソースブラウザで選べる種別（表示名, kubectl の種別名）。
const KUBE_KINDS: &[(&str, &str)] = &[
    ("Pods", "pods"),
    ("Deployments", "deployments"),
    ("StatefulSets", "statefulsets"),
    ("DaemonSets", "daemonsets"),
    ("ReplicaSets", "replicasets"),
    ("Services", "services"),
    ("Ingresses", "ingresses"),
    ("ConfigMaps", "configmaps"),
    ("Secrets", "secrets"),
    ("PersistentVolumeClaims", "persistentvolumeclaims"),
    ("PersistentVolumes", "persistentvolumes"),
    ("Jobs", "jobs"),
    ("CronJobs", "cronjobs"),
    ("HorizontalPodAutoscalers", "horizontalpodautoscalers"),
    ("Nodes", "nodes"),
    ("Namespaces", "namespaces"),
    ("Endpoints", "endpoints"),
    ("ServiceAccounts", "serviceaccounts"),
    ("NetworkPolicies", "networkpolicies"),
    ("StorageClasses", "storageclasses"),
];

/// 行に検索語が含まれるか（ASCII 大文字小文字無視）。
fn line_contains_ci(line: &str, query: &str) -> bool {
    let q = query.trim();
    if q.is_empty() {
        return true;
    }
    find_ci(line, q, 0).is_some()
}

/// `text` の `from` バイト以降から `query` を ASCII 大小無視で探し、開始バイト位置を返す。
/// char 境界に揃った一致だけを返すので、戻り値での部分文字列スライスは安全。
fn find_ci(text: &str, query: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let q = query.as_bytes();
    if q.is_empty() || q.len() > bytes.len() {
        return None;
    }
    let mut i = from;
    while i + q.len() <= bytes.len() {
        if text.is_char_boundary(i)
            && text.is_char_boundary(i + q.len())
            && bytes[i..i + q.len()].eq_ignore_ascii_case(q)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// 検索語を強調した LayoutJob を作る（モノスペース、一致部分を着色）。
fn highlight_job(text: &str, query: &str, wrap_width: f32) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = wrap_width;
    let font = egui::FontId::monospace(13.0);
    let base = egui::TextFormat {
        font_id: font.clone(),
        color: TEXT,
        ..Default::default()
    };
    let q = query.trim();
    if q.is_empty() {
        job.append(text, 0.0, base);
        return job;
    }
    let hl = egui::TextFormat {
        font_id: font,
        color: egui::Color32::BLACK,
        background: egui::Color32::from_rgb(250, 204, 21),
        ..Default::default()
    };
    let mut pos = 0;
    while let Some(start) = find_ci(text, q, pos) {
        let end = start + q.len();
        if start > pos {
            job.append(&text[pos..start], 0.0, base.clone());
        }
        job.append(&text[start..end], 0.0, hl.clone());
        pos = end;
    }
    if pos < text.len() {
        job.append(&text[pos..], 0.0, base);
    }
    job
}

fn is_scalable(kind: &str) -> bool {
    matches!(
        kind,
        "deployments" | "statefulsets" | "replicasets" | "replicationcontrollers"
    )
}

fn is_restartable(kind: &str) -> bool {
    matches!(kind, "deployments" | "statefulsets" | "daemonsets")
}

/// セル値の比較。数値・サイズ・期間っぽい値はできるだけ数値順に並べる。
fn cmp_cell(a: &str, b: &str) -> std::cmp::Ordering {
    match (parse_num_prefix(a), parse_num_prefix(b)) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.cmp(b),
    }
}

/// 先頭の数値部分を取り出す（"3", "120Mi", "5d" など）。取れなければ None。
fn parse_num_prefix(s: &str) -> Option<f64> {
    let t = s.trim();
    let end = t
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(t.len());
    if end == 0 {
        return None;
    }
    t[..end].parse::<f64>().ok()
}

/// 背景スレッドとの全チャネル + 設定。
pub struct Channels {
    pub sample_rx: Receiver<Sample>,
    pub req_tx: UnboundedSender<(Target, String)>,
    pub res_rx: Receiver<QueryOutcome>,
    pub import_req_tx: UnboundedSender<query::ImportRequest>,
    pub import_res_rx: Receiver<query::ImportProgress>,
    pub gcs_req_tx: UnboundedSender<query::GcsRequest>,
    pub gcs_res_rx: Receiver<query::GcsResponse>,
    pub schema_rx: Receiver<SchemaGraph>,
    pub kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    pub kube_topo_req_tx: UnboundedSender<Option<String>>,
    pub kube_topo_rx: Receiver<k8s::KubeTopology>,
    pub kube_log_req_tx: UnboundedSender<k8s::LogReq>,
    pub kube_log_rx: Receiver<k8s::LogEvent>,
    pub kube_ev_req_tx: UnboundedSender<Option<String>>,
    pub kube_ev_rx: Receiver<k8s::EventsResult>,
    pub kube_action_req_tx: UnboundedSender<k8s::ActionReq>,
    pub kube_action_rx: Receiver<k8s::ActionResult>,
    pub kube_res_req_tx: UnboundedSender<k8s::ResourceReq>,
    pub kube_res_rx: Receiver<k8s::ResourceResult>,
    pub kube_pf_req_tx: UnboundedSender<k8s::PortForwardReq>,
    pub kube_pf_rx: Receiver<k8s::PortForwardEvent>,
    pub poll_interval: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub conn_info: String,
}

pub struct MonitorApp {
    // 監視
    sample_rx: Receiver<Sample>,
    samples: VecDeque<Sample>,
    last_error: Option<String>,
    max_points: usize,

    // クエリ系（データ / スキーマ）
    req_tx: UnboundedSender<(Target, String)>,
    res_rx: Receiver<QueryOutcome>,
    schema_rx: Receiver<SchemaGraph>,
    // CSV インポート
    import_req_tx: UnboundedSender<query::ImportRequest>,
    import_res_rx: Receiver<query::ImportProgress>,
    import_dialog: Option<ImportDialog>,
    import_pending: bool,
    /// 順次インポートキュー（複数テーブルを順番に処理）。
    import_jobs: Vec<ImportJob>,
    /// 証跡レポートの保存先（スクショ受信待ち。受信したら PNG を書いて Finder で開く）。
    pending_report_dir: Option<std::path::PathBuf>,
    /// GCS フォルダ一括投入の保留（List 応答待ち）。
    pending_bulk: Option<BulkSpec>,
    /// インポートタブで選択中の取り込み先テーブル名。
    import_table_pick: String,
    // GCS インポート（CSV 取得 → ImportDialog へ）
    gcs_req_tx: UnboundedSender<query::GcsRequest>,
    gcs_res_rx: Receiver<query::GcsResponse>,
    gcs_dialog: Option<GcsDialog>,
    gcs_pending: bool,
    sql: String,
    data_result: Option<QueryOutcome>,
    data_pending: bool,
    schema_graph: Option<SchemaGraph>,
    schema_pending: bool,
    // DataGrip 風データビューア
    data_sort: Option<(usize, bool)>, // (列, 昇順)
    data_search: String,              // 結果内フィルタ
    data_history: Vec<String>,        // 実行した SQL の履歴（新しい順）
    tree_expanded: HashSet<String>,   // オブジェクトツリーで展開中のテーブル

    // スキーマ図のパン/ズーム・編集状態
    diagram_pan: egui::Vec2,
    diagram_zoom: f32,
    node_positions: HashMap<String, egui::Pos2>,
    selected: Option<String>,
    copy_note: Option<String>,

    // Kubernetes
    kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    kube_metrics: Option<k8s::KubeMetrics>,
    kube_req_tx: UnboundedSender<Option<String>>,
    kube_graph_rx: Receiver<k8s::KubeTopology>,
    kube_graph: Option<k8s::KubeTopology>,
    kube_pending: bool,
    kube_selected: Option<String>,
    kube_pan: egui::Vec2,
    kube_zoom: f32,
    kube_view: KubeView,

    // k8s ログ（追従ストリーム）
    kube_log_req_tx: UnboundedSender<k8s::LogReq>,
    kube_log_rx: Receiver<k8s::LogEvent>,
    kube_log_title: String,
    kube_log_buf: String,
    kube_log_open: bool,
    kube_log_following: bool,
    log_search: String, // ログ検索語（大文字小文字無視）
    log_filter: bool,   // 一致行のみ表示

    // k8s イベント
    kube_ev_req_tx: UnboundedSender<Option<String>>,
    kube_ev_rx: Receiver<k8s::EventsResult>,
    kube_events: Option<k8s::EventsResult>,
    kube_ev_pending: bool,

    // k8s 操作
    kube_action_req_tx: UnboundedSender<k8s::ActionReq>,
    kube_action_rx: Receiver<k8s::ActionResult>,
    confirm: Option<(String, k8s::ActionReq)>, // 破壊的操作の確認ダイアログ

    // k8s 汎用リソースブラウザ
    kube_res_req_tx: UnboundedSender<k8s::ResourceReq>,
    kube_res_rx: Receiver<k8s::ResourceResult>,
    res_kind: String,   // 表示中の種別（例: "pods"）
    kube_ns: String,    // namespace 絞り込み（空 = 全 namespace）
    res_filter: String, // 名前フィルタ
    res_list: Option<k8s::ResourceList>,
    res_pending: bool,
    res_sort: Option<(usize, bool)>, // (列インデックス, 昇順)
    kube_namespaces: Vec<String>,    // セレクタ用 namespace 一覧
    kube_ns_loaded: bool,            // namespace 一覧を取得済みか

    // YAML エディタ
    yaml_open: bool,
    yaml_title: String,
    yaml_buf: String,

    // exec（コンテナ内コマンド実行）ダイアログ
    exec_open: bool,
    exec_ns: String,
    exec_pod: String,
    exec_container: String,
    exec_cmd: String,

    // port-forward
    kube_pf_req_tx: UnboundedSender<k8s::PortForwardReq>,
    kube_pf_rx: Receiver<k8s::PortForwardEvent>,
    forwards: Vec<PfEntry>,
    pf_next_id: u64,
    pf_open: bool,
    pf_target: String, // 例: "pod/foo" / "svc/bar"
    pf_ns: String,
    pf_local: String,
    pf_remote: String,

    // 設定
    poll_interval: std::sync::Arc<std::sync::atomic::AtomicU64>,
    conn_info: String,
    settings_open: bool,
    // Spanner 接続環境（複数登録・切替）
    env_profiles: Vec<EnvProfile>,
    active_env: Option<String>,
    env_form: EnvProfile,
    contexts: Vec<String>,
    current_context: Option<String>,
    contexts_loaded: bool,

    // GCP 認証（gcloud ADC ログイン）
    auth_running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    auth_status: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    // 起動時 ADC ログイン確認とログイン誘導ダイアログ
    auth_ok: std::sync::Arc<std::sync::Mutex<Option<bool>>>, // None=確認中
    auth_check_started: bool,
    auth_was_running: bool,
    login_dialog: bool,
    login_dismissed: bool,

    // 環境の自動検出（gcloud で instance/database を列挙）
    discover_running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[allow(clippy::type_complexity)]
    discover_result: std::sync::Arc<std::sync::Mutex<Option<Result<Vec<EnvProfile>, String>>>>,
    discover_project: String,

    // ADC で project/instance/database をカスケード選択（REST + 1 回のログイン）
    pick_busy: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pick_result: std::sync::Arc<std::sync::Mutex<Option<PickMsg>>>,
    pick_projects: Vec<String>,
    pick_instances: Vec<String>,
    pick_databases: Vec<String>,
    pick_project: String,
    pick_instance: String,
    pick_database: String,
    pick_error: Option<String>,
    // プロジェクトが大量にある組織向けの絞り込み入力（兼: 一覧に出ない ID の手動指定）。
    pick_project_filter: String,
    // 列挙権限が無く一覧に出ないとき用の手動入力（instance / database）。
    pick_instance_manual: String,
    pick_database_manual: String,

    section: Section,
    view: View,
}

/// project/instance/database カスケード選択の背景取得結果。
enum PickMsg {
    Projects(Result<Vec<String>, String>),
    Instances(Result<Vec<String>, String>),
    Databases(Result<Vec<String>, String>),
}

impl MonitorApp {
    pub fn new(ch: Channels, cc: &eframe::CreationContext<'_>) -> Self {
        install_japanese_font(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);

        // 登録済みの接続環境を読み込み、選択中があれば適用する
        let store = load_envs();
        let mut conn_info = ch.conn_info;
        // トップのカスケード選択の初期値（active プロファイル or 起動時 env）。
        let (mut init_p, mut init_i, mut init_d) = (
            std::env::var("SPANNER_PROJECT").unwrap_or_default(),
            std::env::var("SPANNER_INSTANCE").unwrap_or_default(),
            std::env::var("SPANNER_DATABASE").unwrap_or_default(),
        );
        if let Some(active) = &store.active {
            if let Some(p) = store.profiles.iter().find(|p| &p.name == active) {
                crate::query::set_spanner_env(crate::query::SpannerEnv {
                    project: p.project.clone(),
                    instance: p.instance.clone(),
                    database: p.database.clone(),
                });
                conn_info = format!("{} · {}/{}/{}", p.name, p.project, p.instance, p.database);
                init_p = p.project.clone();
                init_i = p.instance.clone();
                init_d = p.database.clone();
            }
        }

        Self {
            sample_rx: ch.sample_rx,
            samples: VecDeque::new(),
            last_error: None,
            max_points: 480,
            req_tx: ch.req_tx,
            res_rx: ch.res_rx,
            schema_rx: ch.schema_rx,
            import_req_tx: ch.import_req_tx,
            import_res_rx: ch.import_res_rx,
            import_dialog: None,
            import_pending: false,
            import_jobs: Vec::new(),
            pending_report_dir: None,
            pending_bulk: None,
            import_table_pick: String::new(),
            gcs_req_tx: ch.gcs_req_tx,
            gcs_res_rx: ch.gcs_res_rx,
            gcs_dialog: None,
            gcs_pending: false,
            sql: "SELECT * FROM LoadTest LIMIT 100".to_string(),
            data_result: None,
            data_pending: false,
            schema_graph: None,
            schema_pending: false,
            data_sort: None,
            data_search: String::new(),
            data_history: Vec::new(),
            tree_expanded: HashSet::new(),
            diagram_pan: egui::vec2(40.0, 40.0),
            diagram_zoom: 1.0,
            node_positions: load_layout(),
            selected: None,
            copy_note: None,
            kube_metrics_rx: ch.kube_metrics_rx,
            kube_metrics: None,
            kube_req_tx: ch.kube_topo_req_tx,
            kube_graph_rx: ch.kube_topo_rx,
            kube_graph: None,
            kube_pending: false,
            kube_selected: None,
            kube_pan: egui::vec2(40.0, 40.0),
            kube_zoom: 1.0,
            kube_view: KubeView::Monitor,
            kube_log_req_tx: ch.kube_log_req_tx,
            kube_log_rx: ch.kube_log_rx,
            kube_log_title: String::new(),
            kube_log_buf: String::new(),
            kube_log_open: false,
            kube_log_following: false,
            log_search: String::new(),
            log_filter: false,
            kube_ev_req_tx: ch.kube_ev_req_tx,
            kube_ev_rx: ch.kube_ev_rx,
            kube_events: None,
            kube_ev_pending: false,
            kube_action_req_tx: ch.kube_action_req_tx,
            kube_action_rx: ch.kube_action_rx,
            confirm: None,
            kube_res_req_tx: ch.kube_res_req_tx,
            kube_res_rx: ch.kube_res_rx,
            res_kind: "pods".to_string(),
            kube_ns: "default".to_string(),
            res_filter: String::new(),
            res_list: None,
            res_pending: false,
            res_sort: None,
            kube_namespaces: Vec::new(),
            kube_ns_loaded: false,
            yaml_open: false,
            yaml_title: String::new(),
            yaml_buf: String::new(),
            exec_open: false,
            exec_ns: String::new(),
            exec_pod: String::new(),
            exec_container: String::new(),
            exec_cmd: "ls -la".to_string(),
            kube_pf_req_tx: ch.kube_pf_req_tx,
            kube_pf_rx: ch.kube_pf_rx,
            forwards: Vec::new(),
            pf_next_id: 1,
            pf_open: false,
            pf_target: String::new(),
            pf_ns: String::new(),
            pf_local: String::new(),
            pf_remote: String::new(),
            poll_interval: ch.poll_interval,
            conn_info,
            settings_open: false,
            env_profiles: store.profiles,
            active_env: store.active,
            env_form: EnvProfile::default(),
            contexts: Vec::new(),
            current_context: None,
            contexts_loaded: false,
            auth_running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_status: std::sync::Arc::new(std::sync::Mutex::new(None)),
            auth_ok: std::sync::Arc::new(std::sync::Mutex::new(None)),
            auth_check_started: false,
            auth_was_running: false,
            login_dialog: false,
            login_dismissed: false,
            discover_running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            discover_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            discover_project: String::new(),
            pick_busy: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pick_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            pick_project_filter: String::new(),
            pick_instance_manual: String::new(),
            pick_database_manual: String::new(),
            pick_projects: Vec::new(),
            pick_instances: Vec::new(),
            pick_databases: Vec::new(),
            pick_project: init_p,
            pick_instance: init_i,
            pick_database: init_d,
            pick_error: None,
            section: Section::Spanner,
            view: View::Monitor,
        }
    }

    /// `gcloud auth application-default login` をバックグラウンドで実行（ブラウザ認証）。
    fn gcp_login(&mut self) {
        use std::sync::atomic::Ordering;
        if self.auth_running.load(Ordering::Relaxed) {
            return;
        }
        self.auth_running.store(true, Ordering::Relaxed);
        *self.auth_status.lock().unwrap() =
            Some("ログイン中…（ブラウザで認証してください）".into());
        let running = self.auth_running.clone();
        let status = self.auth_status.clone();
        std::thread::spawn(move || {
            let out = std::process::Command::new(gcloud_bin())
                .args(["auth", "application-default", "login"])
                .output();
            let msg = match out {
                Ok(o) if o.status.success() => {
                    "ログイン成功（ADC 設定済み）。接続先を選び直すか再起動で反映されます。".to_string()
                }
                Ok(o) => {
                    let err = String::from_utf8_lossy(&o.stderr);
                    format!("失敗: {}", err.lines().last().unwrap_or("").trim())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => GCLOUD_MISSING.to_string(),
                Err(e) => format!("gcloud 実行失敗: {e}"),
            };
            *status.lock().unwrap() = Some(msg);
            running.store(false, Ordering::Relaxed);
        });
    }

    /// 起動時/ログイン後に ADC ログイン状態を確認し、未ログインならダイアログを出す。
    fn check_login_state(&mut self, ctx: &egui::Context) {
        // エミュレータ接続時は認証不要。
        if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
            return;
        }
        // 起動直後に 1 回チェック。
        if !self.auth_check_started {
            self.auth_check_started = true;
            self.start_adc_check(ctx);
        }
        // ログイン処理が終わった瞬間に再チェック（成功していればダイアログが消える）。
        let running = self
            .auth_running
            .load(std::sync::atomic::Ordering::Relaxed);
        if self.auth_was_running && !running {
            self.start_adc_check(ctx);
        }
        self.auth_was_running = running;
        // チェック結果を評価。
        let result = *self.auth_ok.lock().unwrap();
        if let Some(ok) = result {
            if ok {
                self.login_dialog = false;
            } else if !self.login_dismissed {
                self.login_dialog = true;
            }
        }
        self.login_window(ctx);
    }

    /// ADC（ログイン状態）を背景で確認する。結果は auth_ok に入る。
    fn start_adc_check(&self, ctx: &egui::Context) {
        *self.auth_ok.lock().unwrap() = None; // 確認中
        let slot = self.auth_ok.clone();
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let ok = run_blocking(query::check_adc()).is_ok();
            *slot.lock().unwrap() = Some(ok);
            ctx.request_repaint();
        });
    }

    /// 未ログイン時のログイン誘導ダイアログ。
    fn login_window(&mut self, ctx: &egui::Context) {
        if !self.login_dialog {
            return;
        }
        let running = self
            .auth_running
            .load(std::sync::atomic::Ordering::Relaxed);
        let status = self.auth_status.lock().unwrap().clone();
        let mut login = false;
        let mut dismiss = false;
        egui::Window::new("GCP ログイン")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(
                    egui::RichText::new("GCP にログインしていません（ADC 認証情報が見つかりません）。")
                        .strong(),
                );
                ui.label(
                    egui::RichText::new(
                        "Spanner への接続・監視・プロジェクト/DB の一覧にはログインが必要です。",
                    )
                    .color(MUTED)
                    .small(),
                );
                // gcloud が無い場合はインストール先を先に案内。
                if !gcloud_found() {
                    ui.add_space(4.0);
                    ui.colored_label(
                        egui::Color32::from_rgb(251, 191, 36),
                        "gcloud が未インストールです。先に Google Cloud SDK が必要です:",
                    );
                    ui.hyperlink("https://cloud.google.com/sdk/docs/install");
                }
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!running, egui::Button::new("ログイン（gcloud ADC）"))
                        .on_hover_text("gcloud auth application-default login を実行（ブラウザ認証）")
                        .clicked()
                    {
                        login = true;
                    }
                    if running {
                        ui.spinner();
                        ui.label(
                            egui::RichText::new("ブラウザで認証してください…")
                                .color(MUTED)
                                .small(),
                        );
                    }
                    if ui.button("後で").clicked() {
                        dismiss = true;
                    }
                });
                if let Some(s) = &status {
                    ui.add_space(2.0);
                    ui.label(egui::RichText::new(s).color(ACCENT).small());
                }
                ui.add_space(4.0);
                ui.label(
                    egui::RichText::new(
                        "※ エミュレータを使う場合は SPANNER_EMULATOR_HOST を設定して再起動するとログイン不要です。",
                    )
                    .color(MUTED)
                    .small(),
                );
            });
        if login {
            self.gcp_login();
        }
        if dismiss {
            self.login_dialog = false;
            self.login_dismissed = true;
        }
    }

    /// gcloud で接続環境（instance/database）を自動検出して登録する。
    fn discover_envs(&mut self) {
        use std::sync::atomic::Ordering;
        if self.discover_running.load(Ordering::Relaxed) {
            return;
        }
        self.discover_running.store(true, Ordering::Relaxed);
        *self.discover_result.lock().unwrap() = None;
        let project = self.discover_project.trim().to_string();
        let running = self.discover_running.clone();
        let result = self.discover_result.clone();
        std::thread::spawn(move || {
            let r = gcloud_discover(project);
            *result.lock().unwrap() = Some(r);
            running.store(false, Ordering::Relaxed);
        });
    }

    /// 自動検出の結果が届いていれば env_profiles に取り込む。
    fn drain_discovery(&mut self) {
        let taken = self.discover_result.lock().unwrap().take();
        if let Some(r) = taken {
            match r {
                Ok(found) => {
                    let mut added = 0;
                    for p in found {
                        if !self.env_profiles.iter().any(|e| e.name == p.name) {
                            self.env_profiles.push(p);
                            added += 1;
                        }
                    }
                    save_envs(&self.env_profiles, &self.active_env);
                    self.copy_note = Some(format!("環境を {added} 件検出しました"));
                }
                Err(e) => self.copy_note = Some(format!("検出に失敗: {e}")),
            }
        }
    }

    /// ADC で一覧を取得する背景スレッドを起動する共通処理。
    fn spawn_pick<F>(&self, ctx: &egui::Context, f: F)
    where
        F: FnOnce() -> PickMsg + Send + 'static,
    {
        if self.pick_busy.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return; // 取得中
        }
        let busy = self.pick_busy.clone();
        let result = self.pick_result.clone();
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let msg = f();
            *result.lock().unwrap() = Some(msg);
            busy.store(false, std::sync::atomic::Ordering::Relaxed);
            ctx.request_repaint();
        });
    }

    fn load_projects(&self, ctx: &egui::Context) {
        self.spawn_pick(ctx, || PickMsg::Projects(run_blocking(query::list_projects())));
    }

    fn load_instances(&self, ctx: &egui::Context, project: String) {
        self.spawn_pick(ctx, move || {
            PickMsg::Instances(run_blocking(query::list_instances(&project)))
        });
    }

    fn load_databases(&self, ctx: &egui::Context, project: String, instance: String) {
        self.spawn_pick(ctx, move || {
            PickMsg::Databases(run_blocking(query::list_databases(&project, &instance)))
        });
    }

    /// カスケード選択の取得結果を取り込む。
    fn drain_pick(&mut self) {
        let Some(msg) = self.pick_result.lock().unwrap().take() else {
            return;
        };
        match msg {
            PickMsg::Projects(Ok(v)) => {
                self.pick_projects = v;
                self.pick_error = None;
            }
            PickMsg::Instances(Ok(v)) => {
                self.pick_instances = v;
                self.pick_error = None;
            }
            PickMsg::Databases(Ok(v)) => {
                self.pick_databases = v;
                self.pick_error = None;
            }
            PickMsg::Projects(Err(e))
            | PickMsg::Instances(Err(e))
            | PickMsg::Databases(Err(e)) => {
                self.pick_error = Some(e);
            }
        }
    }

    /// バックグラウンドスレッドから届いたデータを取り込む
    fn drain(&mut self) {
        self.drain_discovery();
        self.drain_pick();
        while let Ok(s) = self.sample_rx.try_recv() {
            match &s.error {
                Some(e) => self.last_error = Some(e.clone()),
                None => self.last_error = None,
            }
            self.samples.push_back(s);
            while self.samples.len() > self.max_points {
                self.samples.pop_front();
            }
        }
        while let Ok(out) = self.res_rx.try_recv() {
            self.data_pending = false;
            self.data_result = Some(out);
        }
        while let Ok(g) = self.schema_rx.try_recv() {
            self.schema_pending = false;
            self.schema_graph = Some(g);
        }
        while let Ok(ev) = self.import_res_rx.try_recv() {
            // 背景は逐次処理なので、進捗/完了は「実行中ジョブ」に属する。
            let running = self
                .import_jobs
                .iter_mut()
                .find(|j| j.status == JobStatus::Running);
            match ev {
                query::ImportProgress::Progress {
                    written,
                    bytes_done,
                    bytes_total,
                    ..
                } => {
                    let frac = progress_fraction(bytes_done, bytes_total);
                    if let Some(j) = running {
                        j.progress = Some(ImportProg {
                            frac,
                            written,
                            bytes_done,
                            bytes_total,
                        });
                    }
                }
                query::ImportProgress::Done(out) => {
                    let msg = format_import_result(&out);
                    if let Some(j) = running {
                        j.status = if out.cancelled {
                            JobStatus::Cancelled
                        } else if out.error.is_some() {
                            JobStatus::Failed
                        } else {
                            JobStatus::Done
                        };
                        j.progress = None;
                        j.result = Some(msg.clone());
                        j.outcome = Some(out); // レポート用に件数を保持
                    }
                    self.copy_note = Some(msg);
                    // 次のジョブへ。
                    self.pump_import_queue();
                }
            }
        }
        while let Ok(resp) = self.gcs_res_rx.try_recv() {
            self.gcs_pending = false;
            match resp {
                query::GcsResponse::Fetched(out) => match out.data {
                    // 取得成功 → GCS ダイアログを閉じてマッピング画面へ引き継ぐ。
                    Some(bytes) => {
                        if let Some(d) = self.gcs_dialog.take() {
                            self.build_import_dialog(
                                d.target,
                                query::ImportSource::Gcs(out.uri.clone()),
                                out.uri,
                                bytes,
                            );
                        }
                    }
                    None => {
                        let msg = format!("GCS 取得失敗: {}", out.error.unwrap_or_default());
                        if let Some(d) = &mut self.gcs_dialog {
                            d.status = Some(msg.clone());
                        }
                        self.copy_note = Some(msg);
                    }
                },
                query::GcsResponse::Listed(out) => {
                    if let Some(bulk) = self.pending_bulk.take() {
                        // フォルダ一括投入: 直下の *.csv をそれぞれジョブ化。
                        if let Some(e) = out.error {
                            self.copy_note = Some(format!("フォルダ一覧に失敗: {e}"));
                        } else {
                            let csvs = csv_object_uris(&out.bucket, &out.objects);
                            let added = csvs.len();
                            for (uri, name) in csvs {
                                let mut req = bulk.template.clone();
                                req.source = query::ImportSource::Gcs(uri);
                                req.cancel = std::sync::Arc::new(
                                    std::sync::atomic::AtomicBool::new(false),
                                );
                                self.push_job(req, name);
                            }
                            self.copy_note = Some(if added == 0 {
                                "このフォルダに CSV はありません".into()
                            } else {
                                format!("{added} 件の CSV をキューに追加しました")
                            });
                        }
                    } else if let Some(d) = &mut self.gcs_dialog {
                        match out.error {
                            Some(e) => d.status = Some(format!("一覧取得失敗: {e}")),
                            None => {
                                let n = out.folders.len() + out.objects.len();
                                d.folders = out.folders;
                                d.objects = out.objects;
                                d.listed_at = Some(format!("gs://{}/{}", out.bucket, out.prefix));
                                d.bucket = out.bucket;
                                d.status = Some(if n == 0 {
                                    "（この階層に CSV はありません）".into()
                                } else {
                                    format!("{n} 件")
                                });
                            }
                        }
                    }
                }
            }
        }
        while let Ok(m) = self.kube_metrics_rx.try_recv() {
            self.kube_metrics = Some(m);
        }
        while let Ok(g) = self.kube_graph_rx.try_recv() {
            self.kube_pending = false;
            self.kube_graph = Some(g);
        }
        while let Ok(ev) = self.kube_log_rx.try_recv() {
            match ev {
                k8s::LogEvent::Start(title) => {
                    self.kube_log_title = title;
                    self.kube_log_buf.clear();
                    self.kube_log_following = true;
                }
                k8s::LogEvent::Line(l) => {
                    self.kube_log_buf.push_str(&l);
                    self.kube_log_buf.push('\n');
                    // バッファが肥大化しないよう前方を切り詰め（char 境界で）
                    if self.kube_log_buf.len() > 200_000 {
                        let mut start = self.kube_log_buf.len() - 150_000;
                        while !self.kube_log_buf.is_char_boundary(start) {
                            start += 1;
                        }
                        self.kube_log_buf = self.kube_log_buf[start..].to_string();
                    }
                }
                k8s::LogEvent::Error(e) => {
                    self.kube_log_buf.push_str(&format!("[error] {e}\n"));
                    self.kube_log_following = false;
                }
            }
        }
        while let Ok(e) = self.kube_ev_rx.try_recv() {
            self.kube_ev_pending = false;
            self.kube_events = Some(e);
        }
        while let Ok(r) = self.kube_action_rx.try_recv() {
            self.copy_note = Some(r.message);
            if let Some((title, text)) = r.describe {
                self.kube_log_title = title;
                self.kube_log_buf = text;
                self.kube_log_open = true;
                self.kube_log_following = false;
            }
        }
        while let Ok(r) = self.kube_res_rx.try_recv() {
            match r {
                k8s::ResourceResult::List(list) => {
                    self.res_pending = false;
                    // 表示中の種別と一致する結果だけ採用（種別切替直後の取り違え防止）。
                    // 不一致なら res_list は None のままで、次フレームに自動再取得される。
                    if list.kind == self.res_kind {
                        self.res_sort = None;
                        self.res_list = Some(list);
                    }
                }
                k8s::ResourceResult::Text { title, body } => {
                    // YAML / describe はログ窓を再利用して表示
                    self.kube_log_title = title;
                    self.kube_log_buf = body;
                    self.kube_log_open = true;
                    self.kube_log_following = false;
                }
                k8s::ResourceResult::EditText { title, body } => {
                    self.yaml_title = title;
                    self.yaml_buf = body;
                    self.yaml_open = true;
                }
                k8s::ResourceResult::Namespaces(list) => {
                    self.kube_namespaces = list;
                    self.kube_ns_loaded = true;
                }
            }
        }
        while let Ok(ev) = self.kube_pf_rx.try_recv() {
            match ev {
                k8s::PortForwardEvent::Started { id, label } => {
                    self.upsert_forward(id, &label, "転送中", true);
                }
                k8s::PortForwardEvent::Line { id, text } => {
                    if let Some(f) = self.forwards.iter_mut().find(|f| f.id == id) {
                        f.status = text;
                    }
                }
                k8s::PortForwardEvent::Error { id, msg } => {
                    if let Some(f) = self.forwards.iter_mut().find(|f| f.id == id) {
                        f.status = format!("エラー: {msg}");
                        f.active = false;
                    }
                }
                k8s::PortForwardEvent::Stopped { id } => {
                    if let Some(f) = self.forwards.iter_mut().find(|f| f.id == id) {
                        f.active = false;
                        if !f.status.starts_with("エラー") {
                            f.status = "停止".into();
                        }
                    }
                }
            }
        }
    }

    fn upsert_forward(&mut self, id: u64, label: &str, status: &str, active: bool) {
        if let Some(f) = self.forwards.iter_mut().find(|f| f.id == id) {
            f.label = label.to_string();
            f.status = status.to_string();
            f.active = active;
        } else {
            self.forwards.push(PfEntry {
                id,
                label: label.to_string(),
                status: status.to_string(),
                active,
            });
        }
    }

    /// クラスター(context)切替。kubectl context を変え、namespace 一覧と全ビューを取り直す。
    fn on_cluster_changed(&mut self, name: String) {
        k8s::set_context(Some(name.clone()));
        self.current_context = Some(name);
        // 新クラスターの namespace を取り直す
        self.kube_ns_loaded = false;
        self.kube_namespaces.clear();
        self.kube_ns = "default".to_string();
        // 監視は次のポーリングで新 context を反映（context_args を毎回読むため）
        self.kube_metrics = None;
        self.on_namespace_changed();
    }

    /// 登録済み環境（プロファイル）の i 番目に接続を切り替える。
    fn select_env_profile(&mut self, i: usize) {
        if let Some(p) = self.env_profiles.get(i).cloned() {
            crate::query::set_spanner_env(crate::query::SpannerEnv {
                project: p.project.clone(),
                instance: p.instance.clone(),
                database: p.database.clone(),
            });
            self.active_env = Some(p.name.clone());
            self.conn_info = format!("{} · {}/{}/{}", p.name, p.project, p.instance, p.database);
            self.data_result = None;
            self.schema_graph = None; // 新環境のスキーマを取り直す
            self.kube_metrics = None;
            save_envs(&self.env_profiles, &self.active_env);
            self.copy_note = Some(format!("環境を切り替えました: {}", p.name));
        }
    }

    /// カスケード選択（pick_project/instance/database）の接続を適用する。
    /// プロファイルにも登録して再起動後も使えるようにする。
    fn apply_picked_connection(&mut self) {
        let (project, instance, database) = (
            self.pick_project.clone(),
            self.pick_instance.clone(),
            self.pick_database.clone(),
        );
        if project.is_empty() || instance.is_empty() || database.is_empty() {
            return;
        }
        crate::query::set_spanner_env(crate::query::SpannerEnv {
            project: project.clone(),
            instance: instance.clone(),
            database: database.clone(),
        });
        let name = format!("{project}/{instance}/{database}");
        if !self.env_profiles.iter().any(|e| e.name == name) {
            self.env_profiles.push(EnvProfile {
                name: name.clone(),
                project: project.clone(),
                instance: instance.clone(),
                database: database.clone(),
            });
        }
        self.active_env = Some(name.clone());
        save_envs(&self.env_profiles, &self.active_env);
        self.conn_info = format!("{name} · {project}/{instance}/{database}");
        self.data_result = None;
        self.schema_graph = None; // 新環境のスキーマを取り直す
        self.kube_metrics = None;
        self.copy_note = Some(format!("接続を {name} に切り替えました"));
    }

    /// namespace 変更時に全ビューを取り直す（監視はクライアント側フィルタ）。
    fn on_namespace_changed(&mut self) {
        self.kube_graph = None; // 図 → 自動再取得
        self.kube_events = None; // イベント → 自動再取得
        self.res_list = None; // リソース → 自動再取得
        self.res_sort = None;
    }

    /// 選択中の namespace（空 = 全て → None）。
    fn ns_opt(&self) -> Option<String> {
        let n = self.kube_ns.trim();
        if n.is_empty() {
            None
        } else {
            Some(n.to_string())
        }
    }

    fn run_kube_topo(&mut self) {
        if self.kube_req_tx.send(self.ns_opt()).is_ok() {
            self.kube_pending = true;
        }
    }

    fn run_kube_events(&mut self) {
        if self.kube_ev_req_tx.send(self.ns_opt()).is_ok() {
            self.kube_ev_pending = true;
        }
    }

    /// 現在の種別・namespace 絞り込みでリソース一覧を取得する。
    fn run_resource_list(&mut self) {
        let ns = self.kube_ns.trim();
        let namespace = if ns.is_empty() {
            None
        } else {
            Some(ns.to_string())
        };
        let req = k8s::ResourceReq::List {
            kind: self.res_kind.clone(),
            namespace,
        };
        if self.kube_res_req_tx.send(req).is_ok() {
            self.res_pending = true;
        }
    }

    /// namespace セレクタ用の一覧を取得する。
    fn run_namespaces(&mut self) {
        self.kube_ns_loaded = true; // 多重送信防止（結果が来たら一覧を更新）
        let _ = self.kube_res_req_tx.send(k8s::ResourceReq::Namespaces);
    }

    /// 種別を切り替えて即取得する。
    fn select_kind(&mut self, kind: &str) {
        if self.res_kind != kind {
            self.res_kind = kind.to_string();
            self.res_list = None;
            self.res_sort = None;
            self.run_resource_list();
        }
    }

    fn request_yaml(&mut self, ns: Option<String>, name: &str) {
        let _ = self.kube_res_req_tx.send(k8s::ResourceReq::Yaml {
            kind: self.res_kind.clone(),
            ns,
            name: name.to_string(),
        });
    }

    fn request_describe(&mut self, ns: Option<String>, name: &str) {
        let _ = self.kube_res_req_tx.send(k8s::ResourceReq::Describe {
            kind: self.res_kind.clone(),
            ns,
            name: name.to_string(),
        });
    }

    fn open_logs(&mut self, ns: &str, pod: &str, container: &str) {
        let req = k8s::LogReq {
            ns: ns.to_string(),
            pod: pod.to_string(),
            container: container.to_string(),
        };
        if self.kube_log_req_tx.send(req).is_ok() {
            self.kube_log_open = true;
            self.kube_log_following = true;
            self.kube_log_buf.clear();
        }
    }

    fn send_action(&mut self, req: k8s::ActionReq) {
        let _ = self.kube_action_req_tx.send(req);
    }

    fn latest_ok(&self) -> Option<&Sample> {
        self.samples.iter().rev().find(|s| s.error.is_none())
    }

    /// 指定 SQL を実行する（履歴に記録）。選択範囲実行からも使う。
    fn run_sql(&mut self, sql: String) {
        let sql = sql.trim().to_string();
        if sql.is_empty() {
            return;
        }
        // 履歴（直近と重複しなければ先頭に追加、上限 50）
        if self.data_history.first().map(String::as_str) != Some(sql.as_str()) {
            self.data_history.insert(0, sql.clone());
            self.data_history.truncate(50);
        }
        self.data_sort = None;
        if self.req_tx.send((Target::Data, sql)).is_ok() {
            self.data_pending = true;
        } else {
            // 送信失敗 = 背景ワーカーが停止している。無反応にせずエラーで知らせる。
            self.data_pending = false;
            self.data_result = Some(QueryOutcome {
                error: Some(WORKER_GONE.into()),
                ..Default::default()
            });
        }
    }

    fn run_schema(&mut self) {
        if self.req_tx.send((Target::Schema, String::new())).is_ok() {
            self.schema_pending = true;
        } else {
            self.schema_pending = false;
            self.schema_graph = Some(SchemaGraph {
                error: Some(WORKER_GONE.into()),
                ..Default::default()
            });
        }
    }
}

impl eframe::App for MonitorApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain();
        self.drain_screenshot(ctx);
        self.check_login_state(ctx);
        // 取込中は進捗バーを滑らかに更新するため高頻度で再描画。
        if self.import_pending {
            ctx.request_repaint_after(Duration::from_millis(100));
        } else {
            ctx.request_repaint_after(Duration::from_secs(1));
        }

        // VS Code 風の左アクティビティバー
        egui::SidePanel::left("activity")
            .exact_width(54.0)
            .resizable(false)
            .frame(
                egui::Frame::none()
                    .fill(BASE)
                    .stroke(egui::Stroke::new(1.0, BORDER)),
            )
            .show(ctx, |ui| {
                self.activity_bar(ui);
            });

        // ビュー切替タブ（セクションごとに内容が変わる）
        // 接続切替の操作はクロージャ内で借用中なので、解放後に適用する。
        // トップのカスケード選択（借用解消後に適用）。
        let mut tb_load_instances: Option<String> = None;
        let mut tb_load_databases: Option<(String, String)> = None;
        let mut tb_apply = false;
        egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.add_space(4.0);
                let gap = 20.0;
                match self.section {
                    Section::Spanner => {
                        if tab(ui, self.view == View::Monitor, "監視") {
                            self.view = View::Monitor;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.view == View::Data, "データ") {
                            self.view = View::Data;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.view == View::Schema, "スキーマ") {
                            self.view = View::Schema;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.view == View::Import, "インポート") {
                            self.view = View::Import;
                        }
                        // 右寄せで project / instance / DB のカスケード選択。
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let emu = std::env::var("SPANNER_EMULATOR_HOST").is_ok();
                            let busy = self.pick_busy.load(std::sync::atomic::Ordering::Relaxed);
                            // ③ DB（右端）。開いたとき空なら自動取得。選んだら接続適用。
                            egui::ComboBox::from_id_salt("tb_db")
                                .selected_text(combo_text(&self.pick_database, "DB"))
                                .width(150.0)
                                .show_ui(ui, |ui| {
                                    if self.pick_databases.is_empty()
                                        && !busy
                                        && !self.pick_project.is_empty()
                                        && !self.pick_instance.is_empty()
                                    {
                                        tb_load_databases = Some((
                                            self.pick_project.clone(),
                                            self.pick_instance.clone(),
                                        ));
                                        ui.label(
                                            egui::RichText::new("取得中…").color(MUTED).small(),
                                        );
                                    }
                                    for db in &self.pick_databases {
                                        if ui
                                            .selectable_label(&self.pick_database == db, db)
                                            .clicked()
                                        {
                                            self.pick_database = db.clone();
                                            tb_apply = true;
                                        }
                                    }
                                });
                            // ② インスタンス。開いたとき空なら自動取得。
                            egui::ComboBox::from_id_salt("tb_inst")
                                .selected_text(combo_text(&self.pick_instance, "インスタンス"))
                                .width(160.0)
                                .show_ui(ui, |ui| {
                                    let proj = self.pick_project.clone();
                                    if self.pick_instances.is_empty() && !busy && !proj.is_empty() {
                                        tb_load_instances = Some(proj.clone());
                                        ui.label(
                                            egui::RichText::new("取得中…").color(MUTED).small(),
                                        );
                                    }
                                    for inst in &self.pick_instances {
                                        if ui
                                            .selectable_label(&self.pick_instance == inst, inst)
                                            .clicked()
                                        {
                                            self.pick_instance = inst.clone();
                                            self.pick_database.clear();
                                            self.pick_databases.clear();
                                            tb_load_databases = Some((proj.clone(), inst.clone()));
                                        }
                                    }
                                });
                            // プロジェクトは設定画面で選ぶ。ここでは現在値の表示のみ。
                            let proj_text = if self.pick_project.is_empty() {
                                "プロジェクト未設定".to_string()
                            } else {
                                self.pick_project.clone()
                            };
                            ui.label(egui::RichText::new(proj_text).color(MUTED).small())
                                .on_hover_text(
                                    "プロジェクトの変更は「設定 → 接続先を選択（ADC）」から",
                                );
                            if busy {
                                ui.spinner();
                            }
                            // ラベルは短く。詳細（エミュ印・取得エラー）はホバーで出す。
                            let label = if emu { "接続(emu):" } else { "接続:" };
                            let color = if emu {
                                egui::Color32::from_rgb(251, 191, 36)
                            } else {
                                MUTED
                            };
                            let resp = ui.label(egui::RichText::new(label).color(color).small());
                            if emu {
                                resp.on_hover_text(
                                    "エミュレータ接続中。プロジェクト/インスタンス/DB の一覧取得には\
                                     実 Spanner 接続（ADC ログイン）が必要です。",
                                );
                            } else if let Some(e) = &self.pick_error {
                                resp.on_hover_text(format!("一覧取得エラー: {e}"));
                            }
                        });
                    }
                    Section::Kube => {
                        if tab(ui, self.kube_view == KubeView::Monitor, "監視") {
                            self.kube_view = KubeView::Monitor;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.kube_view == KubeView::Resources, "リソース") {
                            self.kube_view = KubeView::Resources;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.kube_view == KubeView::Diagram, "図") {
                            self.kube_view = KubeView::Diagram;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.kube_view == KubeView::Events, "イベント") {
                            self.kube_view = KubeView::Events;
                        }
                        // クラスター(context) → namespace の2段をセクション共通の
                        // トップレベル選択として右寄せで表示する。
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            // namespace（右端）
                            let ns_label = if self.kube_ns.is_empty() {
                                "(全 namespace)".to_string()
                            } else {
                                self.kube_ns.clone()
                            };
                            let mut ns_changed: Option<String> = None;
                            egui::ComboBox::from_id_salt("kube_ns_global")
                                .selected_text(ns_label)
                                .width(170.0)
                                .show_ui(ui, |ui| {
                                    if ui
                                        .selectable_label(self.kube_ns.is_empty(), "(全 namespace)")
                                        .clicked()
                                    {
                                        ns_changed = Some(String::new());
                                    }
                                    for ns in &self.kube_namespaces {
                                        if ui.selectable_label(&self.kube_ns == ns, ns).clicked() {
                                            ns_changed = Some(ns.clone());
                                        }
                                    }
                                });
                            if let Some(ns) = ns_changed {
                                if ns != self.kube_ns {
                                    self.kube_ns = ns;
                                    self.on_namespace_changed();
                                }
                            }
                            ui.label(egui::RichText::new("NS:").color(MUTED).small());

                            ui.add_space(8.0);

                            // クラスター(context)（namespace の左）
                            let cl_label = self
                                .current_context
                                .clone()
                                .unwrap_or_else(|| "(既定)".to_string());
                            let mut cl_changed: Option<String> = None;
                            egui::ComboBox::from_id_salt("kube_cluster_global")
                                .selected_text(cl_label)
                                .width(180.0)
                                .show_ui(ui, |ui| {
                                    if self.contexts.is_empty() {
                                        ui.label(
                                            egui::RichText::new("(context なし)").color(MUTED),
                                        );
                                    }
                                    for c in &self.contexts {
                                        if ui
                                            .selectable_label(
                                                self.current_context.as_deref() == Some(c),
                                                c,
                                            )
                                            .clicked()
                                        {
                                            cl_changed = Some(c.clone());
                                        }
                                    }
                                });
                            if let Some(c) = cl_changed {
                                if self.current_context.as_deref() != Some(c.as_str()) {
                                    self.on_cluster_changed(c);
                                }
                            }
                            ui.label(egui::RichText::new("クラスタ:").color(MUTED).small());
                        });
                    }
                }
            });
            ui.add_space(8.0);
        });
        // トップのカスケード選択（借用解消後）。
        if let Some(p) = tb_load_instances {
            self.load_instances(ctx, p);
        }
        if let Some((p, i)) = tb_load_databases {
            self.load_databases(ctx, p, i);
        }
        if tb_apply {
            self.apply_picked_connection();
        }

        // 図・イベントは初回表示時に自動取得（データ/インポートのテーブル一覧もスキーマを使う）
        if self.section == Section::Spanner
            && (self.view == View::Schema
                || self.view == View::Data
                || self.view == View::Import)
            && self.schema_graph.is_none()
            && !self.schema_pending
        {
            self.run_schema();
        }
        if self.section == Section::Kube
            && self.kube_view == KubeView::Diagram
            && self.kube_graph.is_none()
            && !self.kube_pending
        {
            self.run_kube_topo();
        }
        if self.section == Section::Kube
            && self.kube_view == KubeView::Events
            && self.kube_events.is_none()
            && !self.kube_ev_pending
        {
            self.run_kube_events();
        }
        if self.section == Section::Kube
            && self.kube_view == KubeView::Resources
            && self.res_list.is_none()
            && !self.res_pending
        {
            self.run_resource_list();
        }
        // クラスター(context)一覧は Kubernetes セクションで一度だけ取得（同期）
        if self.section == Section::Kube && !self.contexts_loaded {
            let (list, current) = k8s::list_contexts_blocking();
            self.contexts = list;
            self.current_context = current;
            self.contexts_loaded = true;
        }
        // namespace 一覧は Kubernetes セクション全体で使う（トップレベル選択）
        if self.section == Section::Kube && !self.kube_ns_loaded {
            self.run_namespaces();
        }

        match self.section {
            Section::Spanner => match self.view {
                View::Schema => self.schema_view(ctx),
                View::Monitor => self.monitor_view(ctx),
                View::Data => self.data_view(ctx),
                View::Import => self.import_view(ctx),
            },
            Section::Kube => match self.kube_view {
                KubeView::Monitor => self.kube_monitor_view(ctx),
                KubeView::Resources => self.kube_resource_view(ctx),
                KubeView::Diagram => self.kube_diagram_view(ctx),
                KubeView::Events => self.kube_events_view(ctx),
            },
        }

        self.settings_window(ctx);
        // インポートのマッピング/進捗は専用「インポート」タブ内にインライン描画する。
        self.gcs_window(ctx);
        self.logs_window(ctx);
        self.confirm_window(ctx);
        self.yaml_editor_window(ctx);
        self.exec_window(ctx);
        self.pf_window(ctx);
        self.forwards_window(ctx);
    }
}

impl MonitorApp {
    /// 左アクティビティバー: セクション切替（Spanner / Kubernetes）。
    fn activity_bar(&mut self, ui: &mut egui::Ui) {
        ui.add_space(12.0);
        if activity_item(
            ui,
            self.section == Section::Spanner,
            draw_db_icon,
            "Spanner",
        ) {
            self.section = Section::Spanner;
        }
        if activity_item(
            ui,
            self.section == Section::Kube,
            draw_k8s_icon,
            "Kubernetes",
        ) {
            self.section = Section::Kube;
        }
        // 設定（歯車）はバー下部に
        ui.with_layout(egui::Layout::bottom_up(egui::Align::Center), |ui| {
            ui.add_space(10.0);
            if activity_item(ui, self.settings_open, draw_gear_icon, "設定") {
                self.settings_open = !self.settings_open;
            }
        });
    }

    fn monitor_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("status").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if let Some(s) = self.latest_ok() {
                    chip(ui, "CPU", &format!("{:.1}%", s.cpu_percent), CPU_COLOR);
                    if s.storage_limit > 0.0 {
                        chip(
                            ui,
                            "Storage",
                            &format!(
                                "{:.1}%  ({} / {})",
                                s.storage_used / s.storage_limit * 100.0,
                                human_bytes(s.storage_used),
                                human_bytes(s.storage_limit),
                            ),
                            STORAGE_COLOR,
                        );
                    } else {
                        chip(ui, "Storage", &human_bytes(s.storage_used), STORAGE_COLOR);
                    }
                    if s.processing_units > 0.0 {
                        chip(ui, "容量", &capacity_label(s.processing_units), ACCENT);
                    }
                } else {
                    ui.label(egui::RichText::new("データ取得待ち…").color(MUTED));
                }
            });
            if let Some(e) = &self.last_error {
                ui.colored_label(
                    egui::Color32::from_rgb(248, 113, 113),
                    format!("エラー: {e}"),
                );
            }
            ui.add_space(6.0);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let t0 = self
                .samples
                .iter()
                .find(|s| s.error.is_none())
                .map(|s| s.t)
                .unwrap_or(0.0);

            let cpu: PlotPoints = self
                .samples
                .iter()
                .filter(|s| s.error.is_none())
                .map(|s| [(s.t - t0) / 60.0, s.cpu_percent])
                .collect();

            ui.label(
                egui::RichText::new("CPU 使用率 (%) — 横軸: 計測開始からの経過 (分)").color(MUTED),
            );
            Plot::new("cpu_plot")
                .height(260.0)
                .legend(Legend::default())
                .include_y(0.0)
                .include_y(120.0)
                .set_margin_fraction(egui::vec2(0.02, 0.0)) // 縦余白0 → 0%より下を描かない
                .allow_scroll(false) // スクロール/ズーム/ドラッグで負側へ動くのを防止
                .allow_zoom(false)
                .allow_drag(false)
                .allow_boxed_zoom(false)
                .show(ui, |pui| {
                    pui.line(
                        Line::new(cpu)
                            .name("CPU %")
                            .color(CPU_COLOR)
                            .width(1.8)
                            .fill(0.0),
                    );
                });

            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);

            let storage_pct: PlotPoints = self
                .samples
                .iter()
                .filter(|s| s.error.is_none() && s.storage_limit > 0.0)
                .map(|s| [(s.t - t0) / 60.0, s.storage_used / s.storage_limit * 100.0])
                .collect();

            ui.label(egui::RichText::new("ストレージ使用率 (%) — 横軸: 経過 (分)").color(MUTED));
            Plot::new("storage_plot")
                .height(260.0)
                .legend(Legend::default())
                .include_y(0.0)
                .include_y(120.0)
                .set_margin_fraction(egui::vec2(0.02, 0.0)) // 縦余白0 → 0%より下を描かない
                .allow_scroll(false) // スクロール/ズーム/ドラッグで負側へ動くのを防止
                .allow_zoom(false)
                .allow_drag(false)
                .allow_boxed_zoom(false)
                .show(ui, |pui| {
                    pui.line(
                        Line::new(storage_pct)
                            .name("Storage %")
                            .color(STORAGE_COLOR)
                            .width(1.8)
                            .fill(0.0),
                    );
                });
        });
    }

    fn data_view(&mut self, ctx: &egui::Context) {
        // 実行したい SQL（ツリー/履歴クリックで設定し、借用解消後に実行）
        let mut load_run: Option<String> = None;
        let mut ddl_copy: Option<String> = None;
        // CSV インポート対象テーブル（借用解消後にダイアログを開く）
        let mut import_open: Option<TableNode> = None;
        let mut gcs_open: Option<TableNode> = None;

        // 左: オブジェクトツリー
        egui::SidePanel::left("db_objects")
            .default_width(240.0)
            .width_range(160.0..=420.0)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("データベース").strong());
                    if ui
                        .add_enabled(!self.schema_pending, egui::Button::new("⟳").small())
                        .on_hover_text("スキーマを再取得")
                        .clicked()
                    {
                        self.schema_graph = None;
                        self.run_schema();
                    }
                });
                ui.label(
                    egui::RichText::new("インポートは上の「インポート」タブから")
                        .color(MUTED)
                        .small(),
                );
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if self.schema_pending && self.schema_graph.is_none() {
                            ui.label(egui::RichText::new("読み込み中…").color(MUTED));
                            return;
                        }
                        let Some(g) = &self.schema_graph else {
                            ui.label(egui::RichText::new("スキーマ未取得").color(MUTED));
                            return;
                        };
                        if let Some(e) = &g.error {
                            ui.colored_label(
                                egui::Color32::from_rgb(248, 113, 113),
                                format!("エラー: {e}"),
                            );
                            return;
                        }
                        ui.label(
                            egui::RichText::new(format!("{} テーブル", g.nodes.len()))
                                .color(MUTED)
                                .small(),
                        );
                        for node in &g.nodes {
                            let expanded = self.tree_expanded.contains(&node.name);
                            ui.horizontal(|ui| {
                                let tri = if expanded { "▼" } else { "▶" };
                                if ui
                                    .add(
                                        egui::Label::new(format!("{tri} {}", node.name))
                                            .sense(egui::Sense::click()),
                                    )
                                    .on_hover_text("クリックで展開 / 右クリックでメニュー")
                                    .clicked()
                                {
                                    if expanded {
                                        self.tree_expanded.remove(&node.name);
                                    } else {
                                        self.tree_expanded.insert(node.name.clone());
                                    }
                                }
                                // 名前の右に小さなインポートボタン。
                                if ui
                                    .small_button("⬆")
                                    .on_hover_text(format!("{} に CSV をインポート", node.name))
                                    .clicked()
                                {
                                    import_open = Some(node.clone());
                                }
                            })
                            .response
                            .context_menu(|ui| {
                                if ui.button("SELECT * を実行").clicked() {
                                    load_run =
                                        Some(format!("SELECT * FROM `{}` LIMIT 100", node.name));
                                    ui.close_menu();
                                }
                                if ui.button("CSV をインポート…").clicked() {
                                    import_open = Some(node.clone());
                                    ui.close_menu();
                                }
                                if ui.button("GCS から CSV をインポート…").clicked() {
                                    gcs_open = Some(node.clone());
                                    ui.close_menu();
                                }
                                if ui.button("テーブル名をコピー").clicked() {
                                    ui.ctx().copy_text(node.name.clone());
                                    ui.close_menu();
                                }
                                if ui.button("DDL をコピー").clicked() {
                                    ddl_copy = Some(build_ddl(node));
                                    ui.close_menu();
                                }
                            });
                            if expanded {
                                ui.indent(&node.name, |ui| {
                                    for c in &node.columns {
                                        let key = if c.pk { "🔑" } else { "•" };
                                        let label = format!("{key} {}  {}", c.name, c.ty);
                                        let color = if c.pk { PK_COLOR } else { TEXT };
                                        if ui
                                            .add(
                                                egui::Label::new(
                                                    egui::RichText::new(label)
                                                        .color(color)
                                                        .monospace()
                                                        .small(),
                                                )
                                                .sense(egui::Sense::click()),
                                            )
                                            .on_hover_text("クリックで列名コピー")
                                            .clicked()
                                        {
                                            ui.ctx().copy_text(c.name.clone());
                                        }
                                    }
                                    if !node.indexes.is_empty() {
                                        ui.label(
                                            egui::RichText::new("インデックス")
                                                .color(MUTED)
                                                .small(),
                                        );
                                        for idx in &node.indexes {
                                            ui.label(
                                                egui::RichText::new(format!("  🔎 {idx}"))
                                                    .color(ACCENT)
                                                    .monospace()
                                                    .small(),
                                            );
                                        }
                                    }
                                });
                            }
                        }
                    });
            });

        // 上: SQL エディタ + 実行 / 選択実行 / 履歴
        egui::TopBottomPanel::top("query_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            let output = egui::TextEdit::multiline(&mut self.sql)
                .desired_rows(3)
                .desired_width(f32::INFINITY)
                .code_editor()
                .show(ui);
            // 選択範囲（あれば）を取り出す
            let selected: Option<String> = output.cursor_range.and_then(|cr| {
                let [a, b] = cr.sorted_cursors();
                let (s, e) = (a.ccursor.index, b.ccursor.index);
                if e > s {
                    Some(self.sql.chars().skip(s).take(e - s).collect())
                } else {
                    None
                }
            });
            ui.horizontal(|ui| {
                let run = ui
                    .add_enabled(!self.data_pending, egui::Button::new("実行"))
                    .clicked();
                let cmd_enter =
                    ui.input(|i| i.modifiers.command && i.key_pressed(egui::Key::Enter));
                if (run || cmd_enter) && !self.data_pending {
                    load_run = Some(self.sql.clone());
                }
                if let Some(sel) = &selected {
                    if ui
                        .add_enabled(!self.data_pending, egui::Button::new("選択を実行"))
                        .on_hover_text("選択した範囲だけ実行")
                        .clicked()
                    {
                        load_run = Some(sel.clone());
                    }
                }
                // 履歴
                if !self.data_history.is_empty() {
                    egui::ComboBox::from_id_salt("sql_history")
                        .selected_text("履歴")
                        .width(120.0)
                        .show_ui(ui, |ui| {
                            for h in &self.data_history {
                                let label: String = h.chars().take(80).collect();
                                if ui.selectable_label(false, label).clicked() {
                                    self.sql = h.clone();
                                }
                            }
                        });
                }
                result_status(ui, self.data_pending, self.data_result.as_ref());
            });
            ui.add_space(4.0);
        });

        // 中央: 強化グリッド
        let mut new_sort: Option<Option<(usize, bool)>> = None;
        let mut save_msg: Option<String> = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(result) = &self.data_result else {
                centered_hint(ui, "SQL を入力して「実行」を押してください");
                return;
            };
            if result.error.is_some() {
                return; // エラーは上部ステータスに表示済み
            }
            if result.columns.is_empty() {
                ui.label(egui::RichText::new("結果なし").color(MUTED));
                return;
            }

            // 結果ツールバー: 検索 + CSV出力 + 行数
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label("🔍");
                ui.add(
                    egui::TextEdit::singleline(&mut self.data_search)
                        .hint_text("結果を絞り込み")
                        .desired_width(200.0),
                );
                if ui.button("✕").on_hover_text("検索クリア").clicked() {
                    self.data_search.clear();
                }
                if ui
                    .button("CSV をコピー")
                    .on_hover_text("結果全体を CSV としてクリップボードへ")
                    .clicked()
                {
                    ui.ctx().copy_text(to_csv(result));
                }
                if ui
                    .button("CSV保存")
                    .on_hover_text("結果を CSV ファイルに保存（~/Downloads）")
                    .clicked()
                {
                    save_msg = Some(match save_csv(result) {
                        Ok(p) => format!("保存しました: {}", p.display()),
                        Err(e) => format!("保存に失敗: {e}"),
                    });
                }
                if self.data_sort.is_some() && ui.button("並び解除").clicked() {
                    new_sort = Some(None);
                }
                // コピー/保存などの通知をその場に表示
                if let Some(note) = &self.copy_note {
                    ui.label(egui::RichText::new(note).color(ACCENT).small());
                }
            });
            ui.separator();

            new_sort = data_result_grid(ui, result, &self.data_search, self.data_sort).or(new_sort);
        });

        if let Some(s) = new_sort {
            self.data_sort = s;
        }
        if let Some(m) = save_msg {
            self.copy_note = Some(m);
        }
        if let Some(ddl) = ddl_copy {
            ctx.copy_text(ddl);
            self.copy_note = Some("DDL をコピーしました".into());
        }
        if let Some(node) = import_open {
            self.open_import_dialog(node);
        }
        if let Some(node) = gcs_open {
            self.open_gcs_dialog(node);
        }
        if let Some(sql) = load_run {
            self.sql = sql.clone();
            self.run_sql(sql);
        }
    }

    // ── CSV インポート ──

    /// 指定テーブル向けにファイルを選んでインポートダイアログを開く。
    /// マッピング用に先頭プレフィックスだけ読む（全行は溜めない。取込時に再ストリーム）。
    fn open_import_dialog(&mut self, node: TableNode) {
        let Some(path) = pick_csv_file() else {
            return; // キャンセル
        };
        let file_name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());
        let bytes = match read_file_prefix(&path, PREVIEW_BYTES) {
            Ok(b) => b,
            Err(e) => {
                self.copy_note = Some(format!("CSV を読めません: {e}"));
                return;
            }
        };
        self.build_import_dialog(node, query::ImportSource::File(path), file_name, bytes);
    }

    /// プレビュー（生バイト）からインポートダイアログ（マッピング画面）を組み立てて開く。
    /// 実データはここに溜めず、`source` から取込時にストリーミングする。
    fn build_import_dialog(
        &mut self,
        node: TableNode,
        source: query::ImportSource,
        display_name: String,
        preview_bytes: Vec<u8>,
    ) {
        let encoding = query::Encoding::Utf8;
        let delimiter = b',';
        let records = query::parse_preview(&preview_bytes, encoding, delimiter, PREVIEW_ROWS + 1);
        if records.is_empty() {
            self.copy_note = Some("CSV が空です".into());
            return;
        }
        let mut dialog = ImportDialog {
            table: node.name,
            table_columns: node.columns,
            source,
            file_name: display_name,
            preview_bytes,
            records,
            encoding,
            delimiter,
            skip_bad_rows: false,
            null_token: String::new(),
            has_header: true,
            csv_headers: Vec::new(),
            mapping: Vec::new(),
            mode: query::ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            note: Some("プレビューは先頭のみ。取込時に全行をストリーミングします。".into()),
            config_msg: None,
        };
        dialog.recompute();
        self.import_dialog = Some(dialog);
        // マッピングは専用「インポート」タブに表示する。
        self.section = Section::Spanner;
        self.view = View::Import;
    }

    /// 指定テーブル向けに GCS インポート（URI 入力）ダイアログを開く。
    fn open_gcs_dialog(&mut self, node: TableNode) {
        self.gcs_dialog = Some(GcsDialog {
            target: node,
            uri: "gs://".to_string(),
            status: None,
            bucket: String::new(),
            folders: Vec::new(),
            objects: Vec::new(),
            listed_at: None,
        });
    }

    /// GCS ダイアログの URI を背景へ送って取得を開始する。
    fn start_gcs_fetch(&mut self) {
        let Some(d) = &self.gcs_dialog else { return };
        let uri = d.uri.trim().to_string();
        if uri.is_empty() || uri == "gs://" {
            if let Some(d) = &mut self.gcs_dialog {
                d.status = Some("gs://bucket/path.csv を入力してください".into());
            }
            return;
        }
        if self.gcs_req_tx.send(query::GcsRequest::Fetch(uri)).is_ok() {
            self.gcs_pending = true;
            if let Some(d) = &mut self.gcs_dialog {
                d.status = Some("取得中…".into());
            }
        } else if let Some(d) = &mut self.gcs_dialog {
            d.status = Some(WORKER_GONE.into());
        }
    }

    /// 指定 `gs://bucket/prefix` の一覧を背景へ要求する。
    fn start_gcs_list(&mut self, location: String) {
        if self.gcs_pending {
            return;
        }
        if self.gcs_req_tx.send(query::GcsRequest::List(location)).is_ok() {
            self.gcs_pending = true;
            if let Some(d) = &mut self.gcs_dialog {
                d.status = Some("一覧取得中…".into());
            }
        } else if let Some(d) = &mut self.gcs_dialog {
            d.status = Some(WORKER_GONE.into());
        }
    }

    /// 設定ダイアログの内容から ImportRequest を組み立てる（検証込み）。
    /// マッピング不足・主キー未割当はエラーメッセージで返す。
    fn dialog_request(&self, dry_run: bool) -> Result<query::ImportRequest, String> {
        let Some(d) = &self.import_dialog else {
            return Err("ダイアログがありません".into());
        };
        let columns: Vec<query::ImportColumn> = d
            .table_columns
            .iter()
            .zip(d.mapping.iter())
            .filter_map(|(col, m)| {
                m.map(|src| query::ImportColumn {
                    name: col.name.clone(),
                    ty: col.ty.clone(),
                    src_index: src,
                })
            })
            .collect();
        if columns.is_empty() {
            return Err("マッピングされた列がありません".into());
        }
        // 実行前検証: 主キー列が未割当だと必ず失敗する。
        let unmapped_pk = unmapped_pks(&d.table_columns, &d.mapping);
        if !unmapped_pk.is_empty() {
            return Err(format!(
                "主キー列が未割当です: {}（マッピングしてください）",
                unmapped_pk.join(", ")
            ));
        }
        let null_token = (!d.null_token.is_empty()).then(|| d.null_token.clone());
        Ok(query::ImportRequest {
            table: d.table.clone(),
            columns,
            source: d.source.clone(),
            has_header: d.has_header,
            mode: d.mode,
            empty_as_null: d.empty_as_null,
            fresh: d.fresh,
            encoding: d.encoding,
            delimiter: d.delimiter,
            skip_bad_rows: d.skip_bad_rows,
            dry_run,
            null_token,
            cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// ダイアログの内容を 1 ジョブとしてキューに積み、必要なら実行を開始する。
    /// `dry_run=true` のときは書き込まず検証だけ行う。
    fn enqueue_import(&mut self, dry_run: bool) {
        match self.dialog_request(dry_run) {
            Ok(req) => {
                let source_name = self
                    .import_dialog
                    .as_ref()
                    .map(|d| d.file_name.clone())
                    .unwrap_or_default();
                self.import_dialog = None;
                self.push_job(req, source_name);
            }
            Err(e) => {
                if let Some(d) = &mut self.import_dialog {
                    d.config_msg = Some(e);
                }
            }
        }
    }

    /// 同じ設定で、GCS の同一フォルダ内の全 CSV をジョブ化する（バルク投入）。
    /// 現在の設定で List を要求し、応答で各 CSV を enqueue する。
    fn start_bulk_gcs(&mut self) {
        let req = match self.dialog_request(false) {
            Ok(r) => r,
            Err(e) => {
                if let Some(d) = &mut self.import_dialog {
                    d.config_msg = Some(e);
                }
                return;
            }
        };
        let query::ImportSource::Gcs(uri) = &req.source else {
            return;
        };
        // フォルダ = 末尾の "/" まで。
        let folder = match uri.rfind('/') {
            Some(i) => uri[..=i].to_string(),
            None => uri.clone(),
        };
        self.pending_bulk = Some(BulkSpec { template: req });
        self.import_dialog = None;
        // 同フォルダを一覧する（応答で各 CSV を enqueue）。
        let _ = self.gcs_req_tx.send(query::GcsRequest::List(folder));
    }

    /// リクエストを 1 ジョブとしてキューに積み、キューを進める。
    fn push_job(&mut self, req: query::ImportRequest, source_name: String) {
        self.import_jobs.push(ImportJob {
            req,
            source_name,
            sent: false,
            status: JobStatus::Queued,
            started: None,
            progress: None,
            result: None,
            outcome: None,
        });
        self.pump_import_queue();
    }

    /// キューを進める: 実行中が無ければ先頭の待機ジョブを背景へ送る。
    fn pump_import_queue(&mut self) {
        let any_running = self
            .import_jobs
            .iter()
            .any(|j| j.status == JobStatus::Running);
        if any_running {
            return;
        }
        let Some(job) = self
            .import_jobs
            .iter_mut()
            .find(|j| j.status == JobStatus::Queued && !j.sent)
        else {
            self.import_pending = false;
            return;
        };
        // cancel は送信クローンと共有されるので、後から job.req.cancel で中断できる。
        if self.import_req_tx.send(job.req.clone()).is_ok() {
            job.sent = true;
            job.status = JobStatus::Running;
            job.started = Some(std::time::Instant::now());
            job.progress = Some(ImportProg {
                frac: Some(0.0),
                written: 0,
                bytes_done: 0,
                bytes_total: None,
            });
            self.import_pending = true;
        } else {
            job.status = JobStatus::Failed;
            job.result = Some(WORKER_GONE.into());
        }
    }

    /// GCS の URI を入力して CSV を取得するダイアログ。
    fn gcs_window(&mut self, ctx: &egui::Context) {
        if self.gcs_dialog.is_none() {
            return;
        }
        let pending = self.gcs_pending;
        let mut open = true;
        let mut do_fetch = false;
        // ブラウズ操作（クロージャ内で借用中なので、解放後に実行する）。
        let mut list_loc: Option<String> = None; // この場所を一覧する
        let mut fetch_uri: Option<String> = None; // この URI を取得＆インポートする

        if let Some(d) = &mut self.gcs_dialog {
            let title = format!("GCS から CSV → {}", d.target.name);
            egui::Window::new(title)
                .open(&mut open)
                .collapsible(false)
                .resizable(true)
                .default_width(520.0)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label(
                        egui::RichText::new("URI を直接入力するか、「一覧」でバケットを参照します")
                            .color(MUTED)
                            .small(),
                    );
                    ui.add_space(4.0);
                    let resp = ui.add(
                        egui::TextEdit::singleline(&mut d.uri)
                            .hint_text("gs://my-bucket/path/to/data.csv")
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace),
                    );
                    // Enter で取得開始。
                    if resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) && !pending
                    {
                        do_fetch = true;
                    }
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(!pending, egui::Button::new("取得してインポート"))
                            .clicked()
                        {
                            do_fetch = true;
                        }
                        if ui
                            .add_enabled(!pending, egui::Button::new("一覧"))
                            .on_hover_text("入力中の gs://bucket/prefix 直下を一覧します")
                            .clicked()
                        {
                            list_loc = Some(d.uri.clone());
                        }
                        if pending {
                            ui.spinner();
                        }
                    });
                    if let Some(s) = &d.status {
                        ui.add_space(2.0);
                        ui.label(egui::RichText::new(s).color(ACCENT));
                    }

                    // ── ブラウザ（一覧した場合のみ） ──
                    if let Some(at) = &d.listed_at {
                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new(at).monospace().small().color(MUTED),
                            );
                            // 親フォルダへ。
                            if let Some(parent) = parent_location(&d.bucket, at) {
                                if ui.small_button("上へ").clicked() {
                                    list_loc = Some(parent);
                                }
                            }
                        });
                        egui::ScrollArea::vertical()
                            .max_height(260.0)
                            .auto_shrink([false, true])
                            .show(ui, |ui| {
                                for folder in &d.folders {
                                    // 末尾の "/" でフォルダと分かる。
                                    let label = leaf_name(folder);
                                    if ui
                                        .add(
                                            egui::Label::new(label)
                                                .sense(egui::Sense::click()),
                                        )
                                        .clicked()
                                    {
                                        list_loc = Some(format!("gs://{}/{}", d.bucket, folder));
                                    }
                                }
                                for obj in &d.objects {
                                    let is_csv = obj.to_lowercase().ends_with(".csv");
                                    let name = leaf_name(obj);
                                    let text = egui::RichText::new(name)
                                        .color(if is_csv { ACCENT } else { MUTED });
                                    let resp = ui
                                        .add(egui::Label::new(text).sense(egui::Sense::click()))
                                        .on_hover_text("クリックで取得してインポート");
                                    if resp.clicked() {
                                        fetch_uri = Some(format!("gs://{}/{}", d.bucket, obj));
                                    }
                                }
                            });
                    }

                    ui.add_space(2.0);
                    ui.label(
                        egui::RichText::new("読み取りには対象バケットへの閲覧権限が必要です。")
                            .color(MUTED)
                            .small(),
                    );
                });
        }

        if !open {
            self.gcs_dialog = None;
            return;
        }
        // ファイル選択 → URI を確定して取得開始。
        if let Some(uri) = fetch_uri {
            if let Some(d) = &mut self.gcs_dialog {
                d.uri = uri;
            }
            self.start_gcs_fetch();
        } else if do_fetch {
            self.start_gcs_fetch();
        } else if let Some(loc) = list_loc {
            if let Some(d) = &mut self.gcs_dialog {
                d.uri = loc.clone();
            }
            self.start_gcs_list(loc);
        }
    }

    /// 専用「インポート」タブ。テーブル選択 → ソース選択 → マッピング/進捗をインライン表示。
    fn import_view(&mut self, ctx: &egui::Context) {
        let mut open_local: Option<TableNode> = None;
        let mut open_gcs: Option<TableNode> = None;
        // ジョブ一覧の操作（借用解消後に適用）。
        let mut cancel_idx: Option<usize> = None;
        let mut remove_idx: Option<usize> = None;
        let mut requeue_idx: Option<usize> = None;
        let mut clear_done = false;
        let mut do_report = false;
        let tables: Vec<TableNode> = self
            .schema_graph
            .as_ref()
            .filter(|g| g.error.is_none())
            .map(|g| g.nodes.clone())
            .unwrap_or_default();

        egui::CentralPanel::default().show(ctx, |ui| {
            // マッピング/取込中は本体をインライン描画。
            if self.import_dialog.is_some() {
                self.import_dialog_body(ui);
                return;
            }
            // ── ランディング（取り込み先テーブル＋ソース選択） ──
            ui.add_space(10.0);
            ui.heading("CSV インポート");
            ui.label(
                egui::RichText::new(
                    "CSV をテーブルへ高速取り込み（ストリーミング＋並列 BatchWrite・低メモリ）。",
                )
                .color(MUTED),
            );
            ui.add_space(14.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("① 取り込み先テーブル").strong());
                let sel_text = if self.import_table_pick.is_empty() {
                    "選択…".to_string()
                } else {
                    self.import_table_pick.clone()
                };
                egui::ComboBox::from_id_salt("import_table_pick")
                    .selected_text(sel_text)
                    .width(280.0)
                    .show_ui(ui, |ui| {
                        if tables.is_empty() {
                            ui.label("テーブルがありません（スキーマ未取得）");
                        }
                        for t in &tables {
                            ui.selectable_value(
                                &mut self.import_table_pick,
                                t.name.clone(),
                                &t.name,
                            );
                        }
                    });
            });
            let sel = tables
                .iter()
                .find(|t| t.name == self.import_table_pick)
                .cloned();
            ui.add_space(12.0);
            ui.label(egui::RichText::new("② CSV ソースを選ぶ").strong());
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let enabled = sel.is_some();
                if ui
                    .add_enabled(enabled, egui::Button::new("ローカル CSV を選択…"))
                    .clicked()
                {
                    open_local = sel.clone();
                }
                if ui
                    .add_enabled(enabled, egui::Button::new("GCS から選択…"))
                    .clicked()
                {
                    open_gcs = sel.clone();
                }
            });
            if sel.is_none() {
                ui.add_space(6.0);
                ui.colored_label(
                    egui::Color32::from_rgb(251, 191, 36),
                    "先に取り込み先テーブルを選択してください。",
                );
            }

            // ── ジョブ一覧（順次キュー） ──
            if !self.import_jobs.is_empty() {
                ui.add_space(16.0);
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("インポートジョブ").strong());
                    if ui
                        .button("証跡レポート出力")
                        .on_hover_text(
                            "各テーブルの件数・方式・結果を Markdown/CSV で出力し、\
                             ジョブ一覧のスクリーンショット PNG も保存します（~/Downloads）。",
                        )
                        .clicked()
                    {
                        do_report = true;
                    }
                    if ui.small_button("完了を消去").clicked() {
                        clear_done = true;
                    }
                });
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for (i, job) in self.import_jobs.iter().enumerate() {
                            let (badge, color) = match job.status {
                                JobStatus::Queued => ("待機", MUTED),
                                JobStatus::Running => ("実行中", ACCENT),
                                JobStatus::Done => ("完了", egui::Color32::from_rgb(74, 222, 128)),
                                JobStatus::Failed => ("失敗", egui::Color32::from_rgb(248, 113, 113)),
                                JobStatus::Cancelled => {
                                    ("中断", egui::Color32::from_rgb(251, 191, 36))
                                }
                            };
                            egui::Frame::none()
                                .fill(ELEVATED)
                                .rounding(egui::Rounding::same(6.0))
                                .inner_margin(egui::Margin::same(8.0))
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.label(egui::RichText::new(badge).color(color).strong());
                                        ui.label(
                                            egui::RichText::new(format!(
                                                "{} ← {}",
                                                job.req.table, job.source_name
                                            ))
                                            .strong(),
                                        );
                                        if job.req.dry_run {
                                            ui.label(
                                                egui::RichText::new("[検証]").color(MUTED).small(),
                                            );
                                        }
                                        // 右寄せの操作ボタン。
                                        ui.with_layout(
                                            egui::Layout::right_to_left(egui::Align::Center),
                                            |ui| match job.status {
                                                JobStatus::Running => {
                                                    if ui.small_button("⏹ 中断").clicked() {
                                                        cancel_idx = Some(i);
                                                    }
                                                }
                                                JobStatus::Queued => {
                                                    if ui.small_button("✕ 取消").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                                JobStatus::Failed | JobStatus::Cancelled => {
                                                    if ui
                                                        .small_button("↻ 再キュー")
                                                        .on_hover_text("続きから再開します")
                                                        .clicked()
                                                    {
                                                        requeue_idx = Some(i);
                                                    }
                                                    if ui.small_button("✕").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                                JobStatus::Done => {
                                                    if ui.small_button("✕").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                            },
                                        );
                                    });
                                    // 進捗バー（実行中）。
                                    if let Some(p) = &job.progress {
                                        let written = fmt_count(p.written);
                                        // 速度・ETA（経過時間と進捗から算出）。
                                        let rate = import_rate_eta(job.started, p);
                                        match p.frac {
                                            Some(f) => {
                                                let bytes = match p.bytes_total {
                                                    Some(t) => format!(
                                                        "  ·  {} / {}",
                                                        human_bytes(p.bytes_done as f64),
                                                        human_bytes(t as f64),
                                                    ),
                                                    None => String::new(),
                                                };
                                                let text = format!(
                                                    "{:.0}%  ·  {written} 行{bytes}{rate}",
                                                    f * 100.0
                                                );
                                                ui.add(
                                                    egui::ProgressBar::new(f)
                                                        .text(text)
                                                        .fill(ACCENT)
                                                        .desired_width(f32::INFINITY),
                                                );
                                            }
                                            None => {
                                                ui.add(
                                                    egui::ProgressBar::new(0.0)
                                                        .text(format!("取込中…  {written} 行{rate}"))
                                                        .animate(true)
                                                        .desired_width(f32::INFINITY),
                                                );
                                            }
                                        }
                                    }
                                    if let Some(r) = &job.result {
                                        ui.label(egui::RichText::new(r).color(MUTED).small());
                                    }
                                });
                            ui.add_space(4.0);
                        }
                    });
            }
        });

        if let Some(n) = open_local {
            self.open_import_dialog(n);
        }
        if let Some(n) = open_gcs {
            self.open_gcs_dialog(n);
        }
        if let Some(i) = cancel_idx {
            // 実行中ジョブに中断要求（送信クローンと cancel を共有）。
            if let Some(j) = self.import_jobs.get(i) {
                j.req.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
        if let Some(i) = requeue_idx {
            if let Some(old) = self.import_jobs.get(i) {
                let mut req = old.req.clone();
                req.cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let source_name = old.source_name.clone();
                self.push_job(req, source_name);
            }
        }
        if let Some(i) = remove_idx {
            if i < self.import_jobs.len() && self.import_jobs[i].status != JobStatus::Running {
                self.import_jobs.remove(i);
            }
        }
        if clear_done {
            self.import_jobs.retain(|j| j.is_active());
        }
        if do_report {
            self.export_import_report(ctx);
        }
    }

    /// 証跡レポート（Markdown/CSV）を出力し、スクリーンショットを要求する。
    fn export_import_report(&mut self, ctx: &egui::Context) {
        if self.import_jobs.is_empty() {
            self.copy_note = Some("ジョブがありません".into());
            return;
        }
        let ts = chrono::Local::now();
        let home = std::env::var("HOME")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("."));
        let base = {
            let d = home.join("Downloads");
            if d.is_dir() {
                d
            } else {
                home
            }
        };
        let dir = base.join(format!("spanner_import_report_{}", ts.format("%Y%m%d_%H%M%S")));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            self.copy_note = Some(format!("レポート用フォルダ作成に失敗: {e}"));
            return;
        }
        let md = report_markdown(&self.import_jobs, &ts);
        let csv = report_csv(&self.import_jobs);
        let _ = std::fs::write(dir.join("report.md"), md);
        let _ = std::fs::write(dir.join("report.csv"), csv);
        // スクショは次フレームで Event::Screenshot として届く。届いたら同フォルダへ保存。
        self.pending_report_dir = Some(dir.clone());
        ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot);
        self.copy_note = Some(format!("レポートを出力中…: {}", dir.display()));
    }

    /// スクリーンショット応答が来ていれば PNG として保存し、Finder で開く。
    fn drain_screenshot(&mut self, ctx: &egui::Context) {
        let shot = ctx.input(|i| {
            i.raw.events.iter().find_map(|e| match e {
                egui::Event::Screenshot { image, .. } => Some(image.clone()),
                _ => None,
            })
        });
        if let Some(image) = shot {
            if let Some(dir) = self.pending_report_dir.take() {
                let png = dir.join("screenshot.png");
                let saved = save_screenshot_png(&png, &image).is_ok();
                // フォルダを Finder で開く（macOS）。
                let _ = std::process::Command::new("open").arg(&dir).spawn();
                self.copy_note = Some(if saved {
                    format!("証跡レポートを保存しました: {}", dir.display())
                } else {
                    format!("レポートは保存（スクショは失敗）: {}", dir.display())
                });
            }
        }
    }

    /// インポートのマッピング・オプション・進捗をインライン描画する（インポートタブ内）。
    fn import_dialog_body(&mut self, ui: &mut egui::Ui) {
        let mut do_import = false;
        let mut do_dry = false;
        let mut do_bulk = false;
        let mut repick = false;
        let mut close = false;

        if let Some(d) = &mut self.import_dialog {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui.button("← テーブル選択へ戻る").clicked() {
                    close = true;
                }
                ui.label(egui::RichText::new(format!("インポート先 → {}", d.table)).strong());
            });
            ui.separator();
            {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("ファイル:").color(MUTED));
                        ui.label(&d.file_name);
                        if ui.small_button("選び直す").clicked() {
                            repick = true;
                        }
                    });
                    let data_rows = d.data_rows_count();
                    ui.label(
                        egui::RichText::new(format!(
                            "{} 列 · プレビュー {} 行（全体は取込時にストリーミング）",
                            d.csv_headers.len(),
                            data_rows
                        ))
                        .color(MUTED)
                        .small(),
                    );
                    if let Some(n) = &d.note {
                        ui.colored_label(egui::Color32::from_rgb(251, 191, 36), n);
                    }

                    ui.add_space(4.0);
                    // 文字コード・区切り（変更したらプレビューを再パース）。
                    ui.horizontal(|ui| {
                        ui.label("文字コード:");
                        let mut enc_changed = false;
                        egui::ComboBox::from_id_salt("import_encoding")
                            .selected_text(match d.encoding {
                                query::Encoding::Utf8 => "UTF-8",
                                query::Encoding::ShiftJis => "Shift-JIS",
                            })
                            .show_ui(ui, |ui| {
                                enc_changed |= ui
                                    .selectable_value(&mut d.encoding, query::Encoding::Utf8, "UTF-8")
                                    .changed();
                                enc_changed |= ui
                                    .selectable_value(
                                        &mut d.encoding,
                                        query::Encoding::ShiftJis,
                                        "Shift-JIS (CP932)",
                                    )
                                    .changed();
                            });
                        ui.add_space(8.0);
                        ui.label("区切り:");
                        let mut delim_changed = false;
                        let delim_label = match d.delimiter {
                            b'\t' => "タブ",
                            b';' => "セミコロン",
                            _ => "カンマ",
                        };
                        egui::ComboBox::from_id_salt("import_delimiter")
                            .selected_text(delim_label)
                            .show_ui(ui, |ui| {
                                delim_changed |=
                                    ui.selectable_value(&mut d.delimiter, b',', "カンマ ,").changed();
                                delim_changed |=
                                    ui.selectable_value(&mut d.delimiter, b'\t', "タブ").changed();
                                delim_changed |= ui
                                    .selectable_value(&mut d.delimiter, b';', "セミコロン ;")
                                    .changed();
                            });
                        if enc_changed || delim_changed {
                            d.reparse_preview();
                        }
                    });
                    if ui
                        .checkbox(&mut d.has_header, "先頭行をヘッダとして扱う")
                        .changed()
                    {
                        d.recompute();
                    }
                    ui.checkbox(&mut d.empty_as_null, "空欄を NULL として扱う");
                    ui.horizontal(|ui| {
                        ui.label("NULL トークン:");
                        ui.add(
                            egui::TextEdit::singleline(&mut d.null_token)
                                .hint_text("例: NULL, \\N（空なら無効）")
                                .desired_width(160.0),
                        )
                        .on_hover_text("この文字列のセルを NULL として書き込みます（空欄扱いとは別）。");
                    });
                    ui.checkbox(&mut d.skip_bad_rows, "不正な行はスキップして続行（リジェクトに記録）")
                        .on_hover_text(
                            "型変換やコミットに失敗した行を飛ばして続けます。\
                             飛ばした行は CSV のリジェクトファイルに書き出します。",
                        );
                    ui.horizontal(|ui| {
                        ui.label("方式:");
                        ui.radio_value(&mut d.mode, query::ImportMode::Insert, "挿入のみ");
                        ui.radio_value(
                            &mut d.mode,
                            query::ImportMode::InsertOrUpdate,
                            "上書き挿入",
                        )
                        .on_hover_text("主キーが既存なら更新（INSERT OR UPDATE）");
                    });
                    ui.label(
                        egui::RichText::new(
                            "取込はソースから直接ストリーミングし、並列 BatchWrite で投入します（高速・低メモリ）。",
                        )
                        .color(MUTED)
                        .small(),
                    );
                    if d.mode == query::ImportMode::Insert {
                        ui.colored_label(
                            egui::Color32::from_rgb(251, 191, 36),
                            "⚠ BatchWrite はリプレイ保護がありません。再実行時の重複を避けるため「上書き挿入」を推奨します。",
                        );
                    }
                    ui.checkbox(&mut d.fresh, "最初からやり直す（前回の途中を無視）")
                        .on_hover_text(
                            "通常は前回コミット済みのバッチを自動でスキップして続きから取り込みます。\
                             これを ON にすると最初から全て取り込み直します。",
                        );

                    ui.separator();
                    ui.label(egui::RichText::new("列のマッピング").strong());
                    ui.label(
                        egui::RichText::new("テーブル列 ← CSV 列")
                            .color(MUTED)
                            .small(),
                    );
                    ui.add_space(2.0);

                    egui::ScrollArea::vertical()
                        .max_height(260.0)
                        .auto_shrink([false, true])
                        .show(ui, |ui| {
                            egui::Grid::new("import_map_grid")
                                .num_columns(2)
                                .spacing([12.0, 6.0])
                                .striped(true)
                                .show(ui, |ui| {
                                    for (ci, col) in d.table_columns.iter().enumerate() {
                                        let key = if col.pk { "🔑 " } else { "" };
                                        ui.label(
                                            egui::RichText::new(format!(
                                                "{key}{}  {}",
                                                col.name, col.ty
                                            ))
                                            .monospace()
                                            .small(),
                                        );
                                        let cur = d.mapping[ci];
                                        let sel_text = match cur {
                                            Some(i) => d
                                                .csv_headers
                                                .get(i)
                                                .cloned()
                                                .unwrap_or_else(|| format!("列{}", i + 1)),
                                            None => "（スキップ）".to_string(),
                                        };
                                        egui::ComboBox::from_id_salt(("import_map", ci))
                                            .selected_text(sel_text)
                                            .width(220.0)
                                            .show_ui(ui, |ui| {
                                                ui.selectable_value(
                                                    &mut d.mapping[ci],
                                                    None,
                                                    "（スキップ）",
                                                );
                                                for (hi, h) in d.csv_headers.iter().enumerate() {
                                                    ui.selectable_value(
                                                        &mut d.mapping[ci],
                                                        Some(hi),
                                                        h,
                                                    );
                                                }
                                            });
                                        ui.end_row();
                                    }
                                });
                        });

                    ui.separator();
                    ui.horizontal(|ui| {
                        let mapped = d.mapping.iter().filter(|m| m.is_some()).count();
                        if ui
                            .add_enabled(mapped > 0, egui::Button::new("キューに追加して実行"))
                            .on_hover_text("ジョブとしてキューに積みます（実行中があれば順番待ち）。")
                            .clicked()
                        {
                            do_import = true;
                        }
                        if ui
                            .add_enabled(mapped > 0, egui::Button::new("検証のみ"))
                            .on_hover_text("書き込まずに全行を型チェックして件数・エラーを確認します。")
                            .clicked()
                        {
                            do_dry = true;
                        }
                        // GCS ソースなら、同フォルダの全 CSV を同設定で一括投入。
                        if matches!(d.source, query::ImportSource::Gcs(_))
                            && ui
                                .add_enabled(
                                    mapped > 0,
                                    egui::Button::new("同フォルダの全CSVをキュー"),
                                )
                                .on_hover_text(
                                    "この GCS フォルダ直下の *.csv を、同じマッピング/設定で\
                                     1 つずつジョブにします（同一レイアウト前提）。",
                                )
                                .clicked()
                        {
                            do_bulk = true;
                        }
                        ui.label(
                            egui::RichText::new(format!("{mapped} 列を書き込み"))
                                .color(MUTED)
                                .small(),
                        );
                    });
                    if let Some(r) = &d.config_msg {
                        ui.colored_label(egui::Color32::from_rgb(248, 113, 113), r);
                    }
            }
        }

        if close {
            self.import_dialog = None;
            return;
        }
        if repick {
            // テーブル情報を保ったままファイルを選び直す。
            if let Some(d) = self.import_dialog.take() {
                self.open_import_dialog(TableNode {
                    name: d.table,
                    columns: d.table_columns,
                    indexes: Vec::new(),
                });
            }
        }
        if do_import {
            self.enqueue_import(false);
        } else if do_dry {
            self.enqueue_import(true);
        } else if do_bulk {
            self.start_bulk_gcs();
        }
    }

    // ── Kubernetes ビュー ──

    fn kube_monitor_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("kube_status").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Kubernetes 監視").strong());
                match &self.kube_metrics {
                    Some(m) if m.error.is_none() => {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} ノード / {} Pod",
                                m.nodes.len(),
                                m.pods.len()
                            ))
                            .color(MUTED),
                        );
                    }
                    Some(m) => {
                        ui.colored_label(
                            egui::Color32::from_rgb(248, 113, 113),
                            format!("エラー: {}", m.error.as_deref().unwrap_or("")),
                        );
                    }
                    None => {
                        ui.label(egui::RichText::new("取得待ち…").color(MUTED));
                    }
                }
            });
            ui.add_space(6.0);
        });

        let mut log_req: Option<(String, String, String)> = None;
        let mut action_req: Option<k8s::ActionReq> = None;
        let mut confirm_req: Option<(String, k8s::ActionReq)> = None;
        let ns_sel = self.kube_ns.clone(); // 選択中 namespace（空 = 全て）
        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(m) = &self.kube_metrics else {
                centered_hint(ui, "kubectl から取得中…");
                return;
            };
            if m.error.is_some() {
                centered_hint(
                    ui,
                    "クラスタに接続できません（kubectl とクラスタ接続を確認）",
                );
                return;
            }
            // 選択 namespace で Pod を絞り込む（空なら全て）
            let pods: Vec<&k8s::PodInfo> = m
                .pods
                .iter()
                .filter(|p| ns_sel.is_empty() || p.ns == ns_sel)
                .collect();
            let pod_count = pods.len();
            let init_count: usize = pods
                .iter()
                .map(|p| p.containers.iter().filter(|c| c.init).count())
                .sum();
            let container_count: usize = pods
                .iter()
                .map(|p| p.containers.iter().filter(|c| !c.init).count())
                .sum();
            let running_count = pods.iter().filter(|p| p.phase == "Running").count();

            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.add_space(4.0);
                // サマリ（チップ）— namespace 選択時はその範囲で集計
                ui.horizontal(|ui| {
                    if ns_sel.is_empty() {
                        chip(ui, "ノード", &m.nodes.len().to_string(), ACCENT);
                    } else {
                        chip(ui, "NS", &ns_sel, ACCENT);
                    }
                    chip(ui, "Pod", &pod_count.to_string(), ACCENT);
                    chip(
                        ui,
                        "コンテナ",
                        &(container_count + init_count).to_string(),
                        CPU_COLOR,
                    );
                    chip(ui, "うちinit", &init_count.to_string(), MUTED);
                    chip(ui, "Running", &running_count.to_string(), STORAGE_COLOR);
                });
                ui.add_space(8.0);

                // namespace 別の集計（全 namespace 表示時のみ）
                if ns_sel.is_empty() && !m.namespaces.is_empty() {
                    ui.label(egui::RichText::new("Namespace 別").color(ACCENT).strong());
                    egui::Grid::new("kube_ns")
                        .striped(true)
                        .spacing([18.0, 4.0])
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Namespace").color(MUTED).small());
                            ui.label(egui::RichText::new("Pod").color(MUTED).small());
                            ui.label(egui::RichText::new("コンテナ").color(MUTED).small());
                            ui.end_row();
                            for ns in &m.namespaces {
                                ui.label(&ns.name);
                                ui.label(ns.pods.to_string());
                                ui.label(ns.containers.to_string());
                                ui.end_row();
                            }
                        });
                    ui.add_space(8.0);
                }

                ui.label(egui::RichText::new("ノード").color(ACCENT).strong());
                for n in &m.nodes {
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new(&n.name).strong());
                        ui.label(
                            egui::RichText::new(format!(
                                "Pod {} / コンテナ {}",
                                n.pods, n.containers
                            ))
                            .color(MUTED)
                            .small(),
                        );
                    });
                    usage_bar(ui, "CPU", n.cpu_pct, CPU_COLOR);
                    usage_bar(ui, "Mem", n.mem_pct, STORAGE_COLOR);
                    ui.add_space(4.0);
                }
                ui.separator();
                ui.label(
                    egui::RichText::new("コンテナ（Pod をクリックで展開）")
                        .color(ACCENT)
                        .strong(),
                );
                ui.add_space(2.0);
                for &p in &pods {
                    let id = ui.make_persistent_id(("kpod", p.ns.as_str(), p.name.as_str()));
                    egui::collapsing_header::CollapsingState::load_with_default_open(
                        ui.ctx(),
                        id,
                        false,
                    )
                    .show_header(ui, |ui| {
                        status_dot(ui, phase_color(&p.phase));
                        ui.label(egui::RichText::new(format!("{}/{}", p.ns, p.name)).strong());
                        ui.label(
                            egui::RichText::new(format!("({}コンテナ)", p.containers.len()))
                                .color(MUTED)
                                .small(),
                        );
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.small_button("削除").clicked() {
                                confirm_req = Some((
                                    format!("Pod {}/{} を削除しますか？", p.ns, p.name),
                                    k8s::ActionReq::DeletePod {
                                        ns: p.ns.clone(),
                                        pod: p.name.clone(),
                                    },
                                ));
                            }
                            if ui.small_button("詳細").clicked() {
                                action_req = Some(k8s::ActionReq::Describe {
                                    ns: p.ns.clone(),
                                    kind: "pod".into(),
                                    name: p.name.clone(),
                                });
                            }
                            ui.label(egui::RichText::new(&p.age).color(MUTED).small());
                            ui.label(
                                egui::RichText::new(format!("再起動 {}", p.restarts))
                                    .color(if p.restarts > 0 { CPU_COLOR } else { MUTED })
                                    .small(),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.0}m / {:.0}Mi",
                                    p.cpu_milli, p.mem_mib
                                ))
                                .color(MUTED)
                                .small(),
                            );
                        });
                    })
                    .body(|ui| {
                        egui::Grid::new(("kc", p.ns.as_str(), p.name.as_str()))
                            .striped(true)
                            .num_columns(6)
                            .spacing([14.0, 3.0])
                            .show(ui, |ui| {
                                for c in &p.containers {
                                    status_dot(ui, container_color(c));
                                    let nm = if c.init {
                                        format!("{} (init)", c.name)
                                    } else {
                                        c.name.clone()
                                    };
                                    ui.label(nm);
                                    ui.label(egui::RichText::new(&c.image).color(MUTED));
                                    ui.label(
                                        egui::RichText::new(format!(
                                            "{:.0}m / {:.0}Mi",
                                            c.cpu_milli, c.mem_mib
                                        ))
                                        .color(MUTED),
                                    );
                                    ui.label(
                                        egui::RichText::new(format!(
                                            "再起動{} · {}",
                                            c.restarts, c.state
                                        ))
                                        .color(MUTED)
                                        .small(),
                                    );
                                    if ui.small_button("ログ").clicked() {
                                        log_req =
                                            Some((p.ns.clone(), p.name.clone(), c.name.clone()));
                                    }
                                    ui.end_row();
                                }
                            });
                    });
                }
            });
        });
        if let Some((ns, pod, c)) = log_req {
            self.open_logs(&ns, &pod, &c);
        }
        if let Some(a) = action_req {
            self.send_action(a);
        }
        if let Some(c) = confirm_req {
            self.confirm = Some(c);
        }
    }

    /// 破壊的操作の確認ダイアログ。
    fn confirm_window(&mut self, ctx: &egui::Context) {
        let Some((msg, _)) = &self.confirm else {
            return;
        };
        let msg = msg.clone();
        let mut decision: Option<bool> = None;
        egui::Window::new("確認")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(msg);
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add(egui::Button::new("実行").fill(egui::Color32::from_rgb(220, 60, 60)))
                        .clicked()
                    {
                        decision = Some(true);
                    }
                    if ui.button("キャンセル").clicked() {
                        decision = Some(false);
                    }
                });
            });
        match decision {
            Some(true) => {
                if let Some((_, req)) = self.confirm.take() {
                    self.send_action(req);
                }
            }
            Some(false) => self.confirm = None,
            None => {}
        }
    }

    fn kube_events_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("kube_ev_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("クラスタイベント").strong());
                if ui
                    .add_enabled(!self.kube_ev_pending, egui::Button::new("更新"))
                    .clicked()
                {
                    self.run_kube_events();
                }
                if self.kube_ev_pending {
                    ui.spinner();
                } else if let Some(r) = &self.kube_events {
                    if let Some(e) = &r.error {
                        ui.colored_label(
                            egui::Color32::from_rgb(248, 113, 113),
                            format!("エラー: {e}"),
                        );
                    } else {
                        ui.label(
                            egui::RichText::new(format!("{} 件", r.events.len())).color(MUTED),
                        );
                    }
                }
            });
            ui.add_space(6.0);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(r) = &self.kube_events else {
                centered_hint(ui, "取得中…");
                return;
            };
            if r.error.is_some() {
                centered_hint(ui, "クラスタに接続できません");
                return;
            }
            if r.events.is_empty() {
                centered_hint(ui, "イベントはありません");
                return;
            }
            egui::ScrollArea::vertical().show(ui, |ui| {
                egui::Grid::new("kube_events")
                    .striped(true)
                    .num_columns(5)
                    .spacing([14.0, 4.0])
                    .show(ui, |ui| {
                        for e in &r.events {
                            let color = if e.warning {
                                egui::Color32::from_rgb(248, 113, 113)
                            } else {
                                egui::Color32::from_rgb(34, 197, 94)
                            };
                            status_dot(ui, color);
                            ui.label(egui::RichText::new(&e.reason).strong());
                            ui.label(egui::RichText::new(&e.object).color(MUTED));
                            ui.label(
                                egui::RichText::new(format!("×{} · {}", e.count, e.age))
                                    .color(MUTED)
                                    .small(),
                            );
                            ui.label(&e.message);
                            ui.end_row();
                        }
                    });
            });
        });
    }

    /// 汎用リソースブラウザ。種別を選んで一覧 → 行ごとに YAML/describe/削除/scale/restart/ログ。
    fn kube_resource_view(&mut self, ctx: &egui::Context) {
        // 上部コントロール（種別・namespace・検索・更新）
        egui::TopBottomPanel::top("kube_res_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("リソース").strong());

                let cur_label = KUBE_KINDS
                    .iter()
                    .find(|(_, k)| *k == self.res_kind)
                    .map(|(l, _)| *l)
                    .unwrap_or(self.res_kind.as_str());
                let mut chosen: Option<&str> = None;
                egui::ComboBox::from_id_salt("res_kind")
                    .selected_text(cur_label)
                    .show_ui(ui, |ui| {
                        for (label, kind) in KUBE_KINDS {
                            if ui
                                .selectable_label(self.res_kind == *kind, *label)
                                .clicked()
                            {
                                chosen = Some(kind);
                            }
                        }
                    });
                if let Some(k) = chosen {
                    self.select_kind(k);
                }

                // namespace は上部タブ列の共通セレクタで切り替える
                ui.label(egui::RichText::new("検索:").color(MUTED).small());
                ui.add(
                    egui::TextEdit::singleline(&mut self.res_filter)
                        .hint_text("名前で絞り込み")
                        .desired_width(160.0),
                );
                if ui
                    .add_enabled(!self.res_pending, egui::Button::new("更新"))
                    .clicked()
                {
                    self.run_resource_list();
                }

                if self.res_pending {
                    ui.spinner();
                } else if let Some(l) = &self.res_list {
                    if let Some(e) = &l.error {
                        ui.colored_label(
                            egui::Color32::from_rgb(248, 113, 113),
                            format!("エラー: {e}"),
                        );
                    } else {
                        ui.label(egui::RichText::new(format!("{} 件", l.rows.len())).color(MUTED));
                    }
                }
            });
            ui.add_space(6.0);
        });

        let mut new_sort: Option<Option<(usize, bool)>> = None;
        let mut action: Option<RowAction> = None;
        let red = egui::Color32::from_rgb(248, 113, 113);

        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(list) = &self.res_list else {
                centered_hint(ui, "取得中…");
                return;
            };
            if list.error.is_some() {
                centered_hint(ui, "クラスタに接続できません");
                return;
            }
            if list.rows.is_empty() {
                centered_hint(ui, "リソースがありません");
                return;
            }

            // フィルタ + ソート済みの表示順
            let filter = self.res_filter.trim().to_lowercase();
            let mut order: Vec<usize> = list
                .rows
                .iter()
                .enumerate()
                .filter(|(_, r)| filter.is_empty() || r.name.to_lowercase().contains(&filter))
                .map(|(i, _)| i)
                .collect();
            if let Some((col, asc)) = self.res_sort {
                order.sort_by(|&a, &b| {
                    let va = list.rows[a]
                        .cells
                        .get(col)
                        .map(String::as_str)
                        .unwrap_or("");
                    let vb = list.rows[b]
                        .cells
                        .get(col)
                        .map(String::as_str)
                        .unwrap_or("");
                    let o = cmp_cell(va, vb);
                    if asc {
                        o
                    } else {
                        o.reverse()
                    }
                });
            }

            let kind = self.res_kind.clone();
            let ncols = list.columns.len() + usize::from(list.namespaced);
            egui::ScrollArea::both().show(ui, |ui| {
                egui::Grid::new("kube_res")
                    .striped(true)
                    .num_columns(ncols.max(1))
                    .spacing([14.0, 4.0])
                    .show(ui, |ui| {
                        // ヘッダ（クリックでソート）
                        if list.namespaced {
                            ui.label(egui::RichText::new("NAMESPACE").color(ACCENT).strong());
                        }
                        for (ci, c) in list.columns.iter().enumerate() {
                            let arrow = match self.res_sort {
                                Some((sc, asc)) if sc == ci => {
                                    if asc {
                                        " ▲"
                                    } else {
                                        " ▼"
                                    }
                                }
                                _ => "",
                            };
                            let resp = ui.add(
                                egui::Label::new(
                                    egui::RichText::new(format!("{c}{arrow}"))
                                        .color(ACCENT)
                                        .strong(),
                                )
                                .sense(egui::Sense::click()),
                            );
                            if resp.clicked() {
                                let asc = !matches!(self.res_sort, Some((sc, a)) if sc == ci && a);
                                new_sort = Some(Some((ci, asc)));
                            }
                        }
                        ui.end_row();

                        // 行
                        for &i in &order {
                            let row = &list.rows[i];
                            let ns_opt = if row.namespace.is_empty() {
                                None
                            } else {
                                Some(row.namespace.clone())
                            };
                            if list.namespaced {
                                ui.label(egui::RichText::new(&row.namespace).color(MUTED));
                            }
                            for (ci, cell) in row.cells.iter().enumerate() {
                                let text = if ci == 0 {
                                    egui::RichText::new(cell).strong()
                                } else {
                                    egui::RichText::new(cell)
                                };
                                let resp = ui
                                    .add(egui::Label::new(text).sense(egui::Sense::click()))
                                    .on_hover_text("右クリックで操作 / クリックでコピー");
                                resp.context_menu(|ui| {
                                    let name = row.name.clone();
                                    if ui.button("YAML を表示").clicked() {
                                        action =
                                            Some(RowAction::Yaml(ns_opt.clone(), name.clone()));
                                        ui.close_menu();
                                    }
                                    if ui.button("describe").clicked() {
                                        action =
                                            Some(RowAction::Describe(ns_opt.clone(), name.clone()));
                                        ui.close_menu();
                                    }
                                    if ui.button("YAML を編集").clicked() {
                                        action =
                                            Some(RowAction::EditYaml(ns_opt.clone(), name.clone()));
                                        ui.close_menu();
                                    }
                                    if kind == "pods" && ui.button("ログを追従").clicked() {
                                        action = Some(RowAction::Logs(
                                            row.namespace.clone(),
                                            name.clone(),
                                        ));
                                        ui.close_menu();
                                    }
                                    if kind == "pods" && ui.button("コマンド実行 (exec)").clicked()
                                    {
                                        action = Some(RowAction::Exec(
                                            row.namespace.clone(),
                                            name.clone(),
                                        ));
                                        ui.close_menu();
                                    }
                                    if matches!(kind.as_str(), "pods" | "services")
                                        && ui.button("port-forward").clicked()
                                    {
                                        let prefix = if kind == "services" { "svc" } else { "pod" };
                                        action = Some(RowAction::PortForward(
                                            row.namespace.clone(),
                                            format!("{prefix}/{name}"),
                                        ));
                                        ui.close_menu();
                                    }
                                    if is_restartable(&kind)
                                        && ui.button("再起動 (rollout restart)").clicked()
                                    {
                                        action =
                                            Some(RowAction::Restart(ns_opt.clone(), name.clone()));
                                        ui.close_menu();
                                    }
                                    if is_scalable(&kind) {
                                        ui.menu_button("スケール", |ui| {
                                            for n in [0, 1, 2, 3, 5, 10] {
                                                if ui.button(format!("{n} レプリカ")).clicked()
                                                {
                                                    action = Some(RowAction::Scale(
                                                        ns_opt.clone(),
                                                        name.clone(),
                                                        n,
                                                    ));
                                                    ui.close_menu();
                                                }
                                            }
                                        });
                                    }
                                    ui.separator();
                                    if ui.button(egui::RichText::new("削除").color(red)).clicked()
                                    {
                                        action =
                                            Some(RowAction::Delete(ns_opt.clone(), name.clone()));
                                        ui.close_menu();
                                    }
                                });
                                if resp.clicked() {
                                    ui.ctx().copy_text(cell.clone());
                                }
                            }
                            ui.end_row();
                        }
                    });
            });
        });

        if let Some(s) = new_sort {
            self.res_sort = s;
        }
        if let Some(a) = action {
            self.apply_row_action(a);
        }
    }

    /// リソース行の操作を実行する。破壊的操作は確認ダイアログ経由。
    fn apply_row_action(&mut self, a: RowAction) {
        let kind = self.res_kind.clone();
        match a {
            RowAction::Yaml(ns, name) => self.request_yaml(ns, &name),
            RowAction::Describe(ns, name) => self.request_describe(ns, &name),
            RowAction::EditYaml(ns, name) => {
                let _ = self
                    .kube_res_req_tx
                    .send(k8s::ResourceReq::YamlForEdit { kind, ns, name });
            }
            RowAction::Logs(ns, pod) => self.open_logs(&ns, &pod, ""),
            RowAction::Exec(ns, pod) => {
                self.exec_ns = ns;
                self.exec_pod = pod;
                self.exec_container = String::new();
                self.exec_open = true;
            }
            RowAction::PortForward(ns, target) => {
                self.pf_ns = ns;
                self.pf_target = target;
                self.pf_local = String::new();
                self.pf_remote = String::new();
                self.pf_open = true;
            }
            RowAction::Restart(ns, name) => {
                self.send_action(k8s::ActionReq::RestartAny { kind, ns, name });
                self.run_resource_list();
            }
            RowAction::Scale(ns, name, replicas) => {
                self.send_action(k8s::ActionReq::ScaleAny {
                    kind,
                    ns,
                    name,
                    replicas,
                });
                self.run_resource_list();
            }
            RowAction::Delete(ns, name) => {
                let msg = format!("{kind}/{name} を削除しますか？この操作は取り消せません。");
                self.confirm = Some((msg, k8s::ActionReq::DeleteAny { kind, ns, name }));
            }
        }
    }

    /// YAML エディタ窓。編集して「適用」で kubectl apply。
    fn yaml_editor_window(&mut self, ctx: &egui::Context) {
        if !self.yaml_open {
            return;
        }
        let mut open = self.yaml_open;
        let mut apply = false;
        egui::Window::new(format!("YAML 編集 — {}", self.yaml_title))
            .open(&mut open)
            .default_size([720.0, 560.0])
            .collapsible(false)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .add(egui::Button::new("適用 (apply)").fill(ACCENT.gamma_multiply(0.6)))
                        .clicked()
                    {
                        apply = true;
                    }
                    ui.label(
                        egui::RichText::new("kubectl apply -f - で適用されます")
                            .color(MUTED)
                            .small(),
                    );
                });
                ui.separator();
                egui::ScrollArea::both().show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut self.yaml_buf)
                            .code_editor()
                            .desired_width(f32::INFINITY)
                            .desired_rows(28),
                    );
                });
            });
        self.yaml_open = open;
        if apply {
            self.send_action(k8s::ActionReq::Apply {
                yaml: self.yaml_buf.clone(),
            });
            self.yaml_open = false;
            self.run_resource_list();
        }
    }

    /// exec ダイアログ。コンテナ内でコマンドを 1 回実行し、出力をログ窓に表示。
    fn exec_window(&mut self, ctx: &egui::Context) {
        if !self.exec_open {
            return;
        }
        let mut open = self.exec_open;
        let mut run = false;
        egui::Window::new("コマンド実行 (exec)")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(
                    egui::RichText::new(format!("{}/{}", self.exec_ns, self.exec_pod)).color(MUTED),
                );
                ui.horizontal(|ui| {
                    ui.label("コンテナ:");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.exec_container)
                            .hint_text("既定")
                            .desired_width(160.0),
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("コマンド:");
                    let r =
                        ui.add(egui::TextEdit::singleline(&mut self.exec_cmd).desired_width(340.0));
                    if r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        run = true;
                    }
                });
                ui.add_space(6.0);
                if ui.button("実行").clicked() {
                    run = true;
                }
                ui.label(
                    egui::RichText::new("sh -c でコンテナ内実行。出力はログ窓に表示。")
                        .color(MUTED)
                        .small(),
                );
            });
        self.exec_open = open;
        if run && !self.exec_cmd.trim().is_empty() {
            self.send_action(k8s::ActionReq::Exec {
                ns: self.exec_ns.clone(),
                pod: self.exec_pod.clone(),
                container: self.exec_container.trim().to_string(),
                command: self.exec_cmd.clone(),
            });
            self.exec_open = false;
        }
    }

    /// port-forward 開始ダイアログ。
    fn pf_window(&mut self, ctx: &egui::Context) {
        if !self.pf_open {
            return;
        }
        let mut open = self.pf_open;
        let mut start = false;
        egui::Window::new("port-forward")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(
                    egui::RichText::new(format!("{} ({})", self.pf_target, self.pf_ns))
                        .color(MUTED),
                );
                ui.horizontal(|ui| {
                    ui.label("ローカル:");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.pf_local)
                            .hint_text("8080")
                            .desired_width(80.0),
                    );
                    ui.label("→ リモート:");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.pf_remote)
                            .hint_text("80")
                            .desired_width(80.0),
                    );
                });
                ui.add_space(6.0);
                if ui.button("開始").clicked() {
                    start = true;
                }
            });
        self.pf_open = open;
        if start {
            match (
                self.pf_local.trim().parse::<u16>(),
                self.pf_remote.trim().parse::<u16>(),
            ) {
                (Ok(local), Ok(remote)) => {
                    let id = self.pf_next_id;
                    self.pf_next_id += 1;
                    let _ = self.kube_pf_req_tx.send(k8s::PortForwardReq::Start {
                        id,
                        ns: self.pf_ns.clone(),
                        target: self.pf_target.clone(),
                        local,
                        remote,
                    });
                    self.pf_open = false;
                }
                _ => self.copy_note = Some("ポート番号が不正です".into()),
            }
        }
    }

    /// アクティブな port-forward 一覧（右下に常駐）。
    fn forwards_window(&mut self, ctx: &egui::Context) {
        if self.forwards.is_empty() {
            return;
        }
        let mut stop_id: Option<u64> = None;
        let mut remove_id: Option<u64> = None;
        egui::Window::new(format!("port-forward ({})", self.forwards.len()))
            .anchor(egui::Align2::RIGHT_BOTTOM, [-12.0, -12.0])
            .resizable(false)
            .show(ctx, |ui| {
                for f in &self.forwards {
                    ui.horizontal(|ui| {
                        let dot = if f.active {
                            egui::Color32::from_rgb(34, 197, 94)
                        } else {
                            MUTED
                        };
                        status_dot(ui, dot);
                        ui.label(egui::RichText::new(&f.label).strong());
                        if f.active {
                            if ui.small_button("停止").clicked() {
                                stop_id = Some(f.id);
                            }
                        } else if ui.small_button("消去").clicked() {
                            remove_id = Some(f.id);
                        }
                    });
                    ui.label(egui::RichText::new(&f.status).color(MUTED).small());
                    ui.separator();
                }
            });
        if let Some(id) = stop_id {
            let _ = self.kube_pf_req_tx.send(k8s::PortForwardReq::Stop { id });
        }
        if let Some(id) = remove_id {
            self.forwards.retain(|f| f.id != id);
        }
    }

    fn settings_window(&mut self, ctx: &egui::Context) {
        if !self.settings_open {
            return;
        }
        if !self.contexts_loaded {
            let (list, current) = k8s::list_contexts_blocking();
            self.contexts = list;
            self.current_context = current;
            self.contexts_loaded = true;
        }
        let mut open = self.settings_open;
        let mut chosen = self.current_context.clone();
        let mut login_clicked = false;
        // 環境操作（描画中に収集→借用解消後に適用）
        let mut env_select: Option<usize> = None;
        let mut env_delete: Option<usize> = None;
        let mut env_add = false;
        let mut discover_clicked = false;
        // カスケード選択の操作（借用解消後に適用）
        let mut do_load_projects = false;
        let mut do_load_instances: Option<String> = None;
        let mut do_apply_pick = false;
        egui::Window::new("設定")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("Spanner 接続").color(MUTED).small());
                ui.label(&self.conn_info);

                // ── ADC でプロジェクト/インスタンス/DB を選択（1 回のログインのみ） ──
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("接続先を選択（ADC）")
                            .color(MUTED)
                            .small(),
                    );
                    let busy = self.pick_busy.load(std::sync::atomic::Ordering::Relaxed);
                    if ui
                        .add_enabled(!busy, egui::Button::new("プロジェクト一覧").small())
                        .on_hover_text("ADC でアクセスできるプロジェクトを一覧します")
                        .clicked()
                    {
                        do_load_projects = true;
                    }
                    if busy {
                        ui.spinner();
                    }
                });
                // ① プロジェクト
                egui::ComboBox::from_id_salt("pick_project")
                    .selected_text(if self.pick_project.is_empty() {
                        "プロジェクト…".to_string()
                    } else {
                        self.pick_project.clone()
                    })
                    .width(260.0)
                    .show_ui(ui, |ui| {
                        for p in self.pick_projects.clone() {
                            if ui
                                .selectable_value(&mut self.pick_project, p.clone(), &p)
                                .clicked()
                            {
                                // プロジェクト変更 → 下位をリセットして再取得。
                                self.pick_instance.clear();
                                self.pick_database.clear();
                                self.pick_instances.clear();
                                self.pick_databases.clear();
                                do_load_instances = Some(p.clone());
                            }
                        }
                    });
                // インスタンス／DB はトップバーで切り替える（ここでは選ばない）。
                ui.label(
                    egui::RichText::new(
                        "インスタンスとデータベースは、上のツールバーで切り替えます。",
                    )
                    .color(MUTED)
                    .small(),
                );
                if let Some(e) = &self.pick_error {
                    ui.colored_label(egui::Color32::from_rgb(248, 113, 113), e);
                }

                // 列挙権限が無く一覧に出ない場合の手動入力フォールバック。
                // （Spanner のリソース単位でのみ権限がある場合など）
                ui.collapsing("一覧に出ない場合は手動で入力", |ui| {
                    ui.label(
                        egui::RichText::new(
                            "プロジェクトの列挙権限が無くても、ID を直接指定すれば接続できます。",
                        )
                        .color(MUTED)
                        .small(),
                    );
                    egui::Grid::new("manual_conn").num_columns(2).show(ui, |ui| {
                        ui.label("プロジェクト");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pick_project_filter)
                                .hint_text("my-project-id")
                                .desired_width(260.0),
                        );
                        ui.end_row();
                        ui.label("インスタンス");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pick_instance_manual)
                                .hint_text("my-instance")
                                .desired_width(260.0),
                        );
                        ui.end_row();
                        ui.label("データベース");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pick_database_manual)
                                .hint_text("my-database")
                                .desired_width(260.0),
                        );
                        ui.end_row();
                    });
                    let manual_ready = !self.pick_project_filter.trim().is_empty()
                        && !self.pick_instance_manual.trim().is_empty()
                        && !self.pick_database_manual.trim().is_empty();
                    if ui
                        .add_enabled(
                            manual_ready,
                            egui::Button::new("この内容で接続"),
                        )
                        .clicked()
                    {
                        self.pick_project = self.pick_project_filter.trim().to_string();
                        self.pick_instance = self.pick_instance_manual.trim().to_string();
                        self.pick_database = self.pick_database_manual.trim().to_string();
                        do_apply_pick = true;
                    }
                });
                ui.separator();

                // 登録済み環境の一覧（選択・削除）
                for (i, p) in self.env_profiles.iter().enumerate() {
                    ui.horizontal(|ui| {
                        let active = self.active_env.as_deref() == Some(p.name.as_str());
                        if ui.radio(active, &p.name).clicked() {
                            env_select = Some(i);
                        }
                        ui.label(
                            egui::RichText::new(format!(
                                "{}/{}/{}",
                                p.project, p.instance, p.database
                            ))
                            .color(MUTED)
                            .small(),
                        );
                        if ui.small_button("🗑").on_hover_text("削除").clicked() {
                            env_delete = Some(i);
                        }
                    });
                }
                // 自動検出（gcloud で instance/database を列挙）
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.discover_project)
                            .hint_text("project（空=gcloud既定）")
                            .desired_width(160.0),
                    );
                    let running = self
                        .discover_running
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if ui
                        .add_enabled(!running, egui::Button::new("環境を自動検出"))
                        .on_hover_text(
                            "ログイン済みなら gcloud で instance/database を列挙して登録",
                        )
                        .clicked()
                    {
                        discover_clicked = true;
                    }
                    if running {
                        ui.spinner();
                    }
                });
                ui.label(
                    egui::RichText::new("または手動で登録:")
                        .color(MUTED)
                        .small(),
                );
                // 新規登録フォーム
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.env_form.name)
                            .hint_text("名前")
                            .desired_width(110.0),
                    );
                    ui.add(
                        egui::TextEdit::singleline(&mut self.env_form.project)
                            .hint_text("project")
                            .desired_width(130.0),
                    );
                });
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.env_form.instance)
                            .hint_text("instance")
                            .desired_width(130.0),
                    );
                    ui.add(
                        egui::TextEdit::singleline(&mut self.env_form.database)
                            .hint_text("database")
                            .desired_width(130.0),
                    );
                    if ui.button("環境を追加").clicked() {
                        env_add = true;
                    }
                });
                ui.separator();

                // GCP 認証（ADC ログイン）
                ui.label(egui::RichText::new("GCP 認証").color(MUTED).small());
                let running = self.auth_running.load(std::sync::atomic::Ordering::Relaxed);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!running, egui::Button::new("ADC ログイン (gcloud)"))
                        .on_hover_text("gcloud auth application-default login を実行")
                        .clicked()
                    {
                        login_clicked = true;
                    }
                    if running {
                        ui.spinner();
                    }
                });
                if let Some(s) = self.auth_status.lock().unwrap().as_ref() {
                    ui.label(egui::RichText::new(s).color(MUTED).small());
                }
                ui.separator();

                ui.label(
                    egui::RichText::new("kubectl コンテキスト")
                        .color(MUTED)
                        .small(),
                );
                if self.contexts.is_empty() {
                    ui.label(
                        egui::RichText::new("(kubectl 未検出 / コンテキストなし)").color(MUTED),
                    );
                } else {
                    egui::ComboBox::from_id_salt("kctx")
                        .selected_text(chosen.clone().unwrap_or_else(|| "(既定)".into()))
                        .show_ui(ui, |ui| {
                            for c in &self.contexts {
                                ui.selectable_value(&mut chosen, Some(c.clone()), c);
                            }
                        });
                }
                ui.separator();

                ui.label(
                    egui::RichText::new("ポーリング間隔（秒）")
                        .color(MUTED)
                        .small(),
                );
                let mut secs = self
                    .poll_interval
                    .load(std::sync::atomic::Ordering::Relaxed);
                if ui.add(egui::Slider::new(&mut secs, 1..=120)).changed() {
                    self.poll_interval
                        .store(secs, std::sync::atomic::Ordering::Relaxed);
                }
                ui.label(
                    egui::RichText::new(
                        "監視・k8s メトリクスの取得間隔。\nCloud Monitoring は最小約60秒。",
                    )
                    .color(MUTED)
                    .small(),
                );
            });
        self.settings_open = open;

        if login_clicked {
            self.gcp_login();
        }
        if discover_clicked {
            self.discover_envs();
        }
        // カスケード選択の取得。
        if do_load_projects {
            self.load_projects(ctx);
        }
        if let Some(project) = do_load_instances {
            self.load_instances(ctx, project);
        }
        if do_apply_pick {
            self.apply_picked_connection();
        }
        // 検出中は結果を取り込むため再描画を促す
        if self
            .discover_running
            .load(std::sync::atomic::Ordering::Relaxed)
            || self.pick_busy.load(std::sync::atomic::Ordering::Relaxed)
        {
            ctx.request_repaint_after(Duration::from_millis(300));
        }

        // 環境の追加
        if env_add {
            let f = &self.env_form;
            if !f.project.is_empty() && !f.instance.is_empty() && !f.database.is_empty() {
                let mut p = self.env_form.clone();
                if p.name.trim().is_empty() {
                    p.name = format!("{}/{}", p.instance, p.database);
                }
                self.env_profiles.push(p);
                self.env_form = EnvProfile::default();
                save_envs(&self.env_profiles, &self.active_env);
            } else {
                self.copy_note = Some("project / instance / database を入力してください".into());
            }
        }
        // 環境の削除
        if let Some(i) = env_delete {
            if i < self.env_profiles.len() {
                let removed = self.env_profiles.remove(i);
                if self.active_env.as_deref() == Some(removed.name.as_str()) {
                    self.active_env = None;
                }
                save_envs(&self.env_profiles, &self.active_env);
            }
        }
        // 環境の選択 → 接続先を切り替え、データ/スキーマを取り直す
        if let Some(i) = env_select {
            self.select_env_profile(i);
        }

        // コンテキスト切替 → 以降の kubectl 呼び出しに反映し、表示をリセット
        if chosen != self.current_context {
            self.current_context = chosen.clone();
            k8s::set_context(chosen);
            self.kube_graph = None;
            self.kube_events = None;
            self.kube_metrics = None;
            self.copy_note = Some("コンテキストを切り替えました".into());
        }
    }

    fn logs_window(&mut self, ctx: &egui::Context) {
        if !self.kube_log_open {
            return;
        }
        // 追従中はこまめに再描画してストリームを反映
        if self.kube_log_following {
            ctx.request_repaint_after(Duration::from_millis(300));
        }
        let mut open = self.kube_log_open;
        egui::Window::new(format!("ログ · {}", self.kube_log_title))
            .open(&mut open)
            .default_size([680.0, 440.0])
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if self.kube_log_following {
                        ui.spinner();
                        ui.label(
                            egui::RichText::new("追従中 (logs -f)")
                                .color(STORAGE_COLOR)
                                .small(),
                        );
                    } else {
                        ui.label(egui::RichText::new("停止").color(MUTED).small());
                    }
                    if ui.button("コピー").clicked() {
                        ui.ctx().copy_text(self.kube_log_buf.clone());
                    }
                    if ui.button("クリア").clicked() {
                        self.kube_log_buf.clear();
                    }
                });
                // 検索バー
                ui.horizontal(|ui| {
                    ui.label("🔍");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.log_search)
                            .hint_text("ログを検索")
                            .desired_width(220.0),
                    );
                    if ui.button("✕").on_hover_text("検索をクリア").clicked() {
                        self.log_search.clear();
                    }
                    ui.checkbox(&mut self.log_filter, "一致行のみ");
                    if !self.log_search.trim().is_empty() {
                        let n = self
                            .kube_log_buf
                            .lines()
                            .filter(|l| line_contains_ci(l, &self.log_search))
                            .count();
                        ui.label(
                            egui::RichText::new(format!("{n} 行一致"))
                                .color(if n == 0 { MUTED } else { ACCENT })
                                .small(),
                        );
                    }
                });
                ui.separator();

                let query = self.log_search.trim().to_string();
                // 「一致行のみ」表示時は対象行だけ抽出した文字列を作る
                let filtered;
                let shown: &str = if !query.is_empty() && self.log_filter {
                    filtered = self
                        .kube_log_buf
                        .lines()
                        .filter(|l| line_contains_ci(l, &query))
                        .collect::<Vec<_>>()
                        .join("\n");
                    &filtered
                } else {
                    &self.kube_log_buf
                };
                let mut text = shown;

                let mut highlighter = |ui: &egui::Ui, s: &str, wrap_width: f32| {
                    let job = highlight_job(s, &query, wrap_width);
                    ui.fonts(|f| f.layout_job(job))
                };
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .stick_to_bottom(self.kube_log_following && query.is_empty())
                    .show(ui, |ui| {
                        let mut te = egui::TextEdit::multiline(&mut text)
                            .code_editor()
                            .desired_width(f32::INFINITY);
                        if !query.is_empty() {
                            te = te.layouter(&mut highlighter);
                        }
                        ui.add_sized(ui.available_size(), te);
                    });
            });
        self.kube_log_open = open;
    }

    fn kube_diagram_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("kube_topo_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("クラスタ構成図").strong());
                if ui
                    .add_enabled(!self.kube_pending, egui::Button::new("更新"))
                    .clicked()
                {
                    self.run_kube_topo();
                }
                if ui.button("表示リセット").clicked() {
                    self.kube_pan = egui::vec2(40.0, 40.0);
                    self.kube_zoom = 1.0;
                }
                if self.kube_pending {
                    ui.spinner();
                    ui.label(egui::RichText::new("読み込み中…").color(MUTED));
                } else if let Some(g) = &self.kube_graph {
                    if g.error.is_none() {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} Pod / {} Service",
                                g.pods.len(),
                                g.services.len()
                            ))
                            .color(MUTED),
                        );
                    }
                }
                ui.separator();
                legend(ui, COMM_COLOR, "通信 (Service→Pod)");
                ui.label(
                    egui::RichText::new("Pod クリックで関連通信を強調")
                        .color(MUTED)
                        .small(),
                );
            });
            ui.add_space(6.0);
        });

        let Self {
            kube_graph,
            kube_selected,
            kube_pan,
            kube_zoom,
            ..
        } = self;
        let g = kube_graph.as_ref();
        egui::CentralPanel::default().show(ctx, |ui| {
            draw_topology(ui, g, kube_pan, kube_zoom, kube_selected);
        });
    }

    fn schema_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("schema_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            // 1行目: 操作ボタン
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("スキーマ図").strong());
                if ui
                    .add_enabled(!self.schema_pending, egui::Button::new("更新"))
                    .clicked()
                {
                    self.run_schema();
                }
                if ui.button("配置を保存").clicked() {
                    self.copy_note = Some(match save_layout(&self.node_positions) {
                        Ok(_) => "配置を保存しました".into(),
                        Err(e) => format!("保存に失敗: {e}"),
                    });
                }
                if ui.button("配置を復元").clicked() {
                    self.node_positions = load_layout();
                    self.copy_note = Some("配置を復元しました".into());
                }
                if ui.button("配置クリア").clicked() {
                    self.node_positions.clear();
                }
                if ui.button("表示リセット").clicked() {
                    self.diagram_pan = egui::vec2(40.0, 40.0);
                    self.diagram_zoom = 1.0;
                }
            });
            // 2行目: 状態・凡例・操作ヒント
            ui.horizontal(|ui| {
                if self.schema_pending {
                    ui.spinner();
                    ui.label(egui::RichText::new("読み込み中…").color(MUTED));
                } else if let Some(g) = &self.schema_graph {
                    if g.error.is_none() {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} テーブル / {} 依存",
                                g.nodes.len(),
                                g.edges.len()
                            ))
                            .color(MUTED),
                        );
                    }
                }
                ui.separator();
                legend(ui, ACCENT, "インターリーブ");
                legend(ui, CPU_COLOR, "外部キー");
                legend(ui, PK_COLOR, "PK");
                ui.separator();
                ui.label(
                    egui::RichText::new(
                        "ヘッダ: クリックで名前コピー+選択 / ドラッグで移動 ・ 行: クリックでコピー",
                    )
                    .color(MUTED)
                    .small(),
                );
                if let Some(note) = &self.copy_note {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(egui::RichText::new(note).color(ACCENT).small());
                    });
                }
            });
            ui.add_space(6.0);
        });

        let Self {
            schema_graph,
            node_positions,
            selected,
            diagram_pan,
            diagram_zoom,
            copy_note,
            ..
        } = self;
        let g = schema_graph.as_ref();
        egui::CentralPanel::default().show(ctx, |ui| {
            Self::draw_graph(
                ui,
                g,
                node_positions,
                selected,
                diagram_pan,
                diagram_zoom,
                copy_note,
            );
        });
    }

    /// グラフ（テーブル/依存、または k8s 構成）を図として描画する。
    /// ノードはドラッグで移動、クリックで選択ハイライト、行/ヘッダクリックでコピー。
    #[allow(clippy::too_many_arguments)]
    fn draw_graph(
        ui: &mut egui::Ui,
        graph: Option<&SchemaGraph>,
        node_positions: &mut HashMap<String, egui::Pos2>,
        selected: &mut Option<String>,
        diagram_pan: &mut egui::Vec2,
        diagram_zoom: &mut f32,
        copy_note: &mut Option<String>,
    ) {
        let Some(graph) = graph else {
            centered_hint(ui, "読み込み中…");
            return;
        };
        if let Some(e) = &graph.error {
            ui.colored_label(
                egui::Color32::from_rgb(248, 113, 113),
                format!("エラー: {e}"),
            );
            return;
        }
        if graph.nodes.is_empty() {
            centered_hint(ui, "テーブルがありません");
            return;
        }

        let rect = ui.available_rect_before_wrap();
        // 背景（パン/ズーム/選択解除）。ノードより先に登録して下層に置く。
        let bg = ui.interact(
            rect,
            ui.id().with("schema_bg"),
            egui::Sense::click_and_drag(),
        );
        if bg.dragged() {
            *diagram_pan += bg.drag_delta();
        }
        if bg.clicked() {
            *selected = None;
        }
        if bg.hovered() {
            let scroll = ui.input(|i| i.raw_scroll_delta.y);
            if scroll != 0.0 {
                let factor = (1.0 + scroll * 0.0015).clamp(0.85, 1.18);
                *diagram_zoom = (*diagram_zoom * factor).clamp(0.3, 3.0);
            }
        }

        let painter = ui.painter_at(rect);
        painter.rect_filled(rect, 0.0, BASE);

        // ノード幅を内容（最長テキスト）に合わせて測定し、はみ出しを防ぐ
        let text_w = |text: &str, size: f32, mono: bool| -> f32 {
            let font = if mono {
                egui::FontId::monospace(size)
            } else {
                egui::FontId::proportional(size)
            };
            ui.fonts(|f| {
                f.layout_no_wrap(text.to_owned(), font, egui::Color32::WHITE)
                    .size()
                    .x
            })
        };
        let mut widths: HashMap<String, f32> = HashMap::new();
        for n in &graph.nodes {
            let mut w = text_w(&n.name, 14.0, false);
            for c in &n.columns {
                let extra = if c.pk { 26.0 } else { 0.0 };
                w = w.max(text_w(&format!("{}  {}", c.name, c.ty), 11.5, true) + extra);
            }
            for idx in &n.indexes {
                w = w.max(text_w(idx, 11.0, true));
            }
            widths.insert(n.name.clone(), (w + 22.0).clamp(150.0, 480.0));
        }

        let base = layout_nodes(graph, &widths);
        let z = *diagram_zoom;
        // テキストのスケールだけは離散化する。連続的にフォントサイズを変えると
        // フレーム毎に新サイズのグリフがラスタライズされフォントアトラスの再
        // アップロードが続き、ズーム中に重くなるため。幾何は z のまま滑らかに。
        let zt = (z * 8.0).round() / 8.0;
        let origin = rect.min + *diagram_pan;
        let tf = |p: egui::Pos2| origin + (p.to_vec2() * z);

        // 選択スナップショット（フィールドを借用したまま後で書き換えないようclone）
        let sel = selected.clone();
        let related: Option<HashSet<&str>> = sel.as_deref().map(|s| {
            let mut set = HashSet::new();
            set.insert(s);
            for e in &graph.edges {
                if e.from == s {
                    set.insert(e.to.as_str());
                }
                if e.to == s {
                    set.insert(e.from.as_str());
                }
            }
            set
        });

        // 同じノードに集まるエッジの接続点・曲がり位置を分散して重なりを防ぐ
        let mut incoming: HashMap<&str, Vec<usize>> = HashMap::new(); // 親側（下端に入る）
        let mut outgoing: HashMap<&str, Vec<usize>> = HashMap::new(); // 子側（上端から出る）
        for (i, e) in graph.edges.iter().enumerate() {
            incoming.entry(e.to.as_str()).or_default().push(i);
            outgoing.entry(e.from.as_str()).or_default().push(i);
        }
        let slot = |list: &[usize], i: usize| -> (usize, usize) {
            (
                list.iter().position(|&x| x == i).unwrap_or(0),
                list.len().max(1),
            )
        };

        // エッジ（背面）
        for (i, e) in graph.edges.iter().enumerate() {
            let (Some(ba), Some(bb)) = (base.get(&e.from), base.get(&e.to)) else {
                continue;
            };
            let ra_min = node_positions.get(&e.from).copied().unwrap_or(ba.min);
            let rb_min = node_positions.get(&e.to).copied().unwrap_or(bb.min);
            let ra = egui::Rect::from_min_size(ra_min, ba.size());
            let rb = egui::Rect::from_min_size(rb_min, bb.size());

            // 子の上端・親の下端で接続 x を分散
            let (op, oc) = slot(&outgoing[e.from.as_str()], i);
            let (ip, ic) = slot(&incoming[e.to.as_str()], i);
            let child_x = ra.left() + ra.width() * (op as f32 + 1.0) / (oc as f32 + 1.0);
            let parent_x = rb.left() + rb.width() * (ip as f32 + 1.0) / (ic as f32 + 1.0);
            let from = tf(egui::pos2(child_x, ra.top()));
            let to = tf(egui::pos2(parent_x, rb.bottom()));

            // 曲がる Y（水平セグメント）を親への流入スロットでずらす
            let base_mid = (ra.top() + rb.bottom()) * 0.5;
            let stagger = (ip as f32 - (ic as f32 - 1.0) / 2.0) * 16.0;
            let bend_y = tf(egui::pos2(0.0, base_mid + stagger)).y;

            // 画面外のエッジはスキップ（曲がり位置 bend_y も含めた範囲で判定）
            let edge_box = egui::Rect::from_points(&[
                from,
                to,
                egui::pos2(from.x, bend_y),
                egui::pos2(to.x, bend_y),
            ]);
            if !rect.intersects(edge_box) {
                continue;
            }

            let base_color = match e.kind {
                EdgeKind::Interleave => ACCENT,
                EdgeKind::ForeignKey => CPU_COLOR,
            };
            let active = sel.as_deref().is_none_or(|s| e.from == s || e.to == s);
            let color = if active {
                base_color
            } else {
                base_color.gamma_multiply(0.22)
            };
            draw_arrow(&painter, from, to, bend_y, color, z);
            if active && !e.label.is_empty() {
                painter.text(
                    egui::pos2((from.x + to.x) * 0.5, bend_y),
                    egui::Align2::CENTER_BOTTOM,
                    &e.label,
                    egui::FontId::proportional((10.0 * zt).max(6.0)),
                    color,
                );
            }
        }

        // ノード（インタラクション + 描画）
        for node in &graph.nodes {
            let Some(br) = base.get(&node.name) else {
                continue;
            };
            let node_min = node_positions.get(&node.name).copied().unwrap_or(br.min);
            let wr = egui::Rect::from_min_size(node_min, br.size());
            let screen = egui::Rect::from_min_max(tf(wr.min), tf(wr.max));

            // ビューポートカリング: 画面外のノードは描画も操作判定もしない。
            // 拡大時は大半が画面外になるため、ここで負荷を大きく減らす。
            if !rect.intersects(screen) {
                continue;
            }

            let is_sel = sel.as_deref() == Some(node.name.as_str());
            let dimmed = related
                .as_ref()
                .is_some_and(|r| !r.contains(node.name.as_str()));
            let dim = |c: egui::Color32| if dimmed { c.gamma_multiply(0.35) } else { c };
            let fs = |s: f32| (s * zt).max(6.0);
            // ノード内のテキストは枠外へはみ出さないようクリップ
            let pc = painter.with_clip_rect(screen.intersect(rect));

            // 背景 + 枠（角丸なしのシャープな矩形。ER 図らしい見た目）
            let rounding = egui::Rounding::ZERO;
            painter.rect_filled(screen, rounding, dim(ELEVATED));
            let border = if is_sel {
                egui::Stroke::new(2.0, ACCENT)
            } else {
                egui::Stroke::new(1.0, dim(BORDER))
            };
            painter.rect_stroke(screen, rounding, border);

            // ヘッダ（ドラッグハンドル + 選択 + 右クリックメニュー）
            let header = egui::Rect::from_min_max(
                screen.min,
                egui::pos2(screen.max.x, screen.min.y + HEADER_H * z),
            );
            painter.rect_filled(header, rounding, dim(ACCENT.gamma_multiply(0.85)));
            pc.text(
                header.left_center() + egui::vec2(8.0 * z, 0.0),
                egui::Align2::LEFT_CENTER,
                &node.name,
                egui::FontId::proportional(fs(14.0)),
                dim(egui::Color32::from_rgb(12, 18, 30)),
            );
            let hid = ui.id().with(("schemahdr", node.name.as_str()));
            let hresp = ui.interact(header, hid, egui::Sense::click_and_drag());
            if hresp.dragged() {
                let cur = node_positions.entry(node.name.clone()).or_insert(br.min);
                *cur += hresp.drag_delta() / z;
            }
            if hresp.clicked() {
                // クリックで選択ハイライト + テーブル名をコピー
                *selected = if sel.as_deref() == Some(node.name.as_str()) {
                    None
                } else {
                    Some(node.name.clone())
                };
                ui.ctx().copy_text(node.name.clone());
                *copy_note = Some(copied(&node.name));
            }
            if hresp.hovered() {
                ui.ctx().set_cursor_icon(egui::CursorIcon::Grab);
            }
            let name = node.name.clone();
            let cols_joined = node
                .columns
                .iter()
                .map(|c| format!("{}  {}", c.name, c.ty))
                .collect::<Vec<_>>()
                .join("\n");
            let idx_joined = node.indexes.join("\n");
            hresp.context_menu(|ui| {
                if ui.button("テーブル名をコピー").clicked() {
                    ui.ctx().copy_text(name.clone());
                    *copy_note = Some(copied(&name));
                    ui.close_menu();
                }
                if ui.button("カラム一覧をコピー").clicked() {
                    ui.ctx().copy_text(cols_joined.clone());
                    *copy_note = Some(format!("コピー: {name} のカラム"));
                    ui.close_menu();
                }
                if !idx_joined.is_empty() && ui.button("インデックス一覧をコピー").clicked()
                {
                    ui.ctx().copy_text(idx_joined.clone());
                    *copy_note = Some(format!("コピー: {name} のインデックス"));
                    ui.close_menu();
                }
            });

            // 行（カラム / インデックス）— 1つずつクリックでコピー
            let mut y = screen.min.y + HEADER_H * z;
            let row_h = ROW_H * z;
            for (i, col) in node.columns.iter().enumerate() {
                let rr = egui::Rect::from_min_size(
                    egui::pos2(screen.min.x, y),
                    egui::vec2(screen.width(), row_h),
                );
                if !rect.intersects(rr) {
                    y += row_h;
                    continue;
                }
                let rid = ui.id().with(("col", node.name.as_str(), i));
                let label = format!("{}  {}", col.name, col.ty);
                let color = if col.pk { dim(PK_COLOR) } else { dim(TEXT) };
                if diagram_row(
                    ui,
                    &pc,
                    rr,
                    rid,
                    &label,
                    egui::FontId::monospace(fs(11.5)),
                    color,
                    z,
                ) {
                    ui.ctx().copy_text(label.clone());
                    *copy_note = Some(copied(&label));
                }
                if col.pk {
                    // 主キーバッジ（右寄せ）
                    pc.text(
                        egui::pos2(rr.max.x - 6.0 * z, rr.center().y),
                        egui::Align2::RIGHT_CENTER,
                        "PK",
                        egui::FontId::proportional(fs(9.0)),
                        dim(PK_COLOR),
                    );
                }
                y += row_h;
            }
            if !node.indexes.is_empty() {
                pc.line_segment(
                    [egui::pos2(screen.min.x, y), egui::pos2(screen.max.x, y)],
                    egui::Stroke::new(1.0, dim(BORDER)),
                );
                pc.text(
                    egui::pos2(screen.min.x + 8.0 * z, y + SECTION_H * z * 0.5),
                    egui::Align2::LEFT_CENTER,
                    "インデックス",
                    egui::FontId::proportional(fs(10.0)),
                    dim(MUTED),
                );
                y += SECTION_H * z;
                for (i, idx) in node.indexes.iter().enumerate() {
                    let rr = egui::Rect::from_min_size(
                        egui::pos2(screen.min.x, y),
                        egui::vec2(screen.width(), row_h),
                    );
                    if !rect.intersects(rr) {
                        y += row_h;
                        continue;
                    }
                    let rid = ui.id().with(("idx", node.name.as_str(), i));
                    if diagram_row(
                        ui,
                        &pc,
                        rr,
                        rid,
                        idx,
                        egui::FontId::monospace(fs(11.0)),
                        dim(ACCENT),
                        z,
                    ) {
                        ui.ctx().copy_text(idx.clone());
                        *copy_note = Some(copied(idx));
                    }
                    y += row_h;
                }
            }
        }
    }
}

/// 主キーバッジ/カラムの色（金）
const PK_COLOR: egui::Color32 = egui::Color32::from_rgb(250, 204, 21);

/// コピー通知用に値を短く切り詰める。
fn copied(value: &str) -> String {
    const MAX: usize = 24;
    let v: String = if value.chars().count() > MAX {
        format!("{}…", value.chars().take(MAX).collect::<String>())
    } else {
        value.to_string()
    };
    format!("コピー: {v}")
}

/// ノード内の 1 行（カラム/インデックス）。クリックでコピー、ホバーで強調。
#[allow(clippy::too_many_arguments)]
fn diagram_row(
    ui: &egui::Ui,
    painter: &egui::Painter,
    rect: egui::Rect,
    id: egui::Id,
    text: &str,
    font: egui::FontId,
    color: egui::Color32,
    z: f32,
) -> bool {
    let resp = ui.interact(rect, id, egui::Sense::click());
    if resp.hovered() {
        painter.rect_filled(rect, 0.0, egui::Color32::from_white_alpha(16));
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    painter.text(
        egui::pos2(rect.min.x + 8.0 * z, rect.center().y),
        egui::Align2::LEFT_CENTER,
        text,
        font,
        color,
    );
    resp.clicked()
}

/// アンダーライン式のタブ（モダンな見た目）。
fn tab(ui: &mut egui::Ui, selected: bool, label: &str) -> bool {
    let color = if selected { TEXT } else { MUTED };
    let resp = ui.add(
        egui::Label::new(egui::RichText::new(label).color(color).strong())
            .sense(egui::Sense::click()),
    );
    let y = resp.rect.bottom() + 5.0;
    let line = if selected {
        Some(ACCENT)
    } else if resp.hovered() {
        Some(MUTED.gamma_multiply(0.7))
    } else {
        None
    };
    if let Some(c) = line {
        ui.painter().line_segment(
            [
                egui::pos2(resp.rect.left(), y),
                egui::pos2(resp.rect.right(), y),
            ],
            egui::Stroke::new(2.0, c),
        );
    }
    if resp.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    resp.clicked()
}

/// 凡例の色サンプル + ラベル。
fn legend(ui: &mut egui::Ui, color: egui::Color32, text: &str) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(14.0, 3.0), egui::Sense::hover());
    ui.painter().rect_filled(rect, 1.0, color);
    ui.label(egui::RichText::new(text).color(MUTED).small());
}

// ── アプリのマーク（DB アイコン） ──

/// 楕円の輪郭点（アイコン描画用）。
fn ellipse_pts(c: egui::Pos2, rx: f32, ry: f32) -> Vec<egui::Pos2> {
    (0..24)
        .map(|i| {
            let a = i as f32 / 24.0 * std::f32::consts::TAU;
            egui::pos2(c.x + rx * a.cos(), c.y + ry * a.sin())
        })
        .collect()
}

/// アクティビティバーの1項目（クリック可能なアイコン）。
fn activity_item(
    ui: &mut egui::Ui,
    selected: bool,
    draw: fn(&egui::Painter, egui::Rect, egui::Color32),
    tip: &str,
) -> bool {
    let w = ui.available_width();
    let (rect, resp) = ui.allocate_exact_size(egui::vec2(w, 50.0), egui::Sense::click());
    let p = ui.painter();
    if selected {
        p.rect_filled(
            egui::Rect::from_min_size(rect.min, egui::vec2(2.5, rect.height())),
            0.0,
            ACCENT,
        );
    } else if resp.hovered() {
        p.rect_filled(rect, 0.0, egui::Color32::from_white_alpha(8));
    }
    let color = if selected {
        egui::Color32::from_rgb(232, 236, 242)
    } else if resp.hovered() {
        TEXT
    } else {
        MUTED
    };
    draw(
        p,
        egui::Rect::from_center_size(rect.center(), egui::vec2(26.0, 26.0)),
        color,
    );
    if resp.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    resp.on_hover_text(tip).clicked()
}

/// ステータスドット（Docker Desktop 風の色丸）。
fn status_dot(ui: &mut egui::Ui, color: egui::Color32) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
    ui.painter().circle_filled(rect.center(), 4.5, color);
}

/// コンテナの使用率テキスト。limit があれば「CPU 45% Mem 60%」、無ければ絶対値、
/// どちらも無ければ空文字（metrics-server 無し等）。
fn usage_text(c: &k8s::ContainerInfo) -> String {
    let cpu = if c.cpu_limit_milli > 0.0 {
        Some(format!(
            "CPU {:.0}%",
            c.cpu_milli / c.cpu_limit_milli * 100.0
        ))
    } else if c.cpu_milli > 0.0 {
        Some(format!("{:.0}m", c.cpu_milli))
    } else {
        None
    };
    let mem = if c.mem_limit_mib > 0.0 {
        Some(format!("Mem {:.0}%", c.mem_mib / c.mem_limit_mib * 100.0))
    } else if c.mem_mib > 0.0 {
        Some(format!("{:.0}Mi", c.mem_mib))
    } else {
        None
    };
    match (cpu, mem) {
        (Some(a), Some(b)) => format!("{a}  {b}"),
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => String::new(),
    }
}

fn phase_color(phase: &str) -> egui::Color32 {
    match phase {
        "Running" => egui::Color32::from_rgb(34, 197, 94), // 緑
        "Pending" => egui::Color32::from_rgb(251, 191, 36), // 黄
        "Succeeded" => egui::Color32::from_rgb(56, 189, 248), // 青
        "Failed" | "Unknown" => egui::Color32::from_rgb(248, 113, 113), // 赤
        _ => MUTED,
    }
}

fn container_color(c: &k8s::ContainerInfo) -> egui::Color32 {
    let s = &c.state;
    if s.contains("BackOff") || s.contains("Error") || s.contains("CrashLoop") {
        egui::Color32::from_rgb(248, 113, 113) // 赤
    } else if s == "Completed" {
        egui::Color32::from_rgb(56, 189, 248) // 青
    } else if s == "Running" && c.ready {
        egui::Color32::from_rgb(34, 197, 94) // 緑
    } else if s == "Running" {
        egui::Color32::from_rgb(251, 191, 36) // 黄（未Ready）
    } else {
        MUTED
    }
}

/// 使用率バー（k8s 監視用）。
fn usage_bar(ui: &mut egui::Ui, label: &str, pct: f64, color: egui::Color32) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new(label).color(MUTED).monospace());
        let (rect, _) = ui.allocate_exact_size(egui::vec2(200.0, 13.0), egui::Sense::hover());
        let p = ui.painter();
        p.rect_filled(rect, 3.0, INPUT_BG);
        let frac = (pct as f32 / 100.0).clamp(0.0, 1.0);
        p.rect_filled(
            egui::Rect::from_min_size(rect.min, egui::vec2(rect.width() * frac, rect.height())),
            3.0,
            color,
        );
        ui.label(egui::RichText::new(format!("{pct:.0}%")).color(MUTED));
    });
}

/// 設定（歯車風）。
fn draw_gear_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    let stroke = egui::Stroke::new(1.6, color);
    let c = r.center();
    let outer = r.width() * 0.42;
    let inner = r.width() * 0.18;
    p.circle_stroke(c, outer * 0.62, stroke);
    p.circle_filled(c, inner, color);
    // 歯（8 本の短い棒）
    for i in 0..8 {
        let a = i as f32 / 8.0 * std::f32::consts::TAU;
        let (s, co) = a.sin_cos();
        let p1 = egui::pos2(c.x + co * outer * 0.62, c.y + s * outer * 0.62);
        let p2 = egui::pos2(c.x + co * outer, c.y + s * outer);
        p.line_segment([p1, p2], stroke);
    }
}

/// Kubernetes のマーク（操舵輪風: 七角形 + スポーク）。
fn draw_k8s_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    let stroke = egui::Stroke::new(1.6, color);
    let c = r.center();
    let rad = r.width() * 0.44;
    let pts: Vec<egui::Pos2> = (0..7)
        .map(|i| {
            let a = -std::f32::consts::FRAC_PI_2 + i as f32 / 7.0 * std::f32::consts::TAU;
            egui::pos2(c.x + rad * a.cos(), c.y + rad * a.sin())
        })
        .collect();
    p.add(egui::Shape::closed_line(pts.clone(), stroke));
    for q in &pts {
        p.line_segment([c, *q], egui::Stroke::new(1.0, color.gamma_multiply(0.8)));
    }
    p.circle_stroke(c, rad * 0.30, stroke);
}

/// アプリのマーク: データベース（シリンダー）。
fn draw_db_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    let stroke = egui::Stroke::new(1.7, color);
    let cx = r.center().x;
    let rx = r.width() * 0.40;
    let ry = r.height() * 0.14;
    let top = r.top() + ry + 1.0;
    let bot = r.bottom() - ry - 1.0;
    p.line_segment([egui::pos2(cx - rx, top), egui::pos2(cx - rx, bot)], stroke);
    p.line_segment([egui::pos2(cx + rx, top), egui::pos2(cx + rx, bot)], stroke);
    p.add(egui::Shape::closed_line(
        ellipse_pts(egui::pos2(cx, top), rx, ry),
        stroke,
    ));
    // 中段・下段の弧（下半分）
    for cy in [(top + bot) * 0.5, bot] {
        let arc: Vec<_> = ellipse_pts(egui::pos2(cx, cy), rx, ry)
            .into_iter()
            .filter(|q| q.y >= cy - 0.01)
            .collect();
        p.add(egui::Shape::line(arc, stroke));
    }
}

// ── スキーマダイアグラム描画 ──

const NODE_W: f32 = 230.0;
const HEADER_H: f32 = 30.0;
const ROW_H: f32 = 18.0;
const SECTION_H: f32 = 20.0; // 「インデックス」区切り見出しの高さ
const H_GAP: f32 = 56.0;
const V_GAP: f32 = 70.0;

/// スキーマ図のノード配置をファイルに保存する。
/// 登録した Spanner 接続環境。
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct EnvProfile {
    name: String,
    project: String,
    instance: String,
    database: String,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct EnvStore {
    profiles: Vec<EnvProfile>,
    active: Option<String>,
}

const ENV_FILE: &str = "spanner_envs.json";

fn load_envs() -> EnvStore {
    std::fs::read_to_string(ENV_FILE)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_envs(profiles: &[EnvProfile], active: &Option<String>) {
    let store = EnvStore {
        profiles: profiles.to_vec(),
        active: active.clone(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&store) {
        let _ = std::fs::write(ENV_FILE, json);
    }
}

/// 単発の async（ADC 一覧 API など）を専用ランタイムでブロッキング実行する。
fn run_blocking<T>(fut: impl std::future::Future<Output = anyhow::Result<T>>) -> Result<T, String> {
    match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt.block_on(fut).map_err(|e| e.to_string()),
        Err(e) => Err(format!("ランタイム生成に失敗: {e}")),
    }
}

/// gcloud 実行ファイルのパスを解決する。PATH に無くても（Finder 起動など）
/// よくある配置先を探す。見つからなければ "gcloud"（PATH 任せ）。
fn gcloud_bin() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let cand = dir.join("gcloud");
            if cand.is_file() {
                return cand;
            }
        }
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates = [
        "/usr/local/bin/gcloud".to_string(),
        "/opt/homebrew/bin/gcloud".to_string(),
        "/opt/homebrew/share/google-cloud-sdk/bin/gcloud".to_string(),
        "/usr/local/share/google-cloud-sdk/bin/gcloud".to_string(),
        "/opt/homebrew/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin/gcloud".to_string(),
        format!("{home}/google-cloud-sdk/bin/gcloud"),
        format!("{home}/.google-cloud-sdk/bin/gcloud"),
    ];
    for p in candidates {
        let pb = std::path::PathBuf::from(p);
        if pb.is_file() {
            return pb;
        }
    }
    std::path::PathBuf::from("gcloud")
}

/// gcloud が見つかったか（絶対パスで解決できれば true）。
fn gcloud_found() -> bool {
    gcloud_bin().is_absolute()
}

/// gcloud が見つからないときの案内文。
const GCLOUD_MISSING: &str = "gcloud が見つかりません。Google Cloud SDK をインストールしてください（https://cloud.google.com/sdk/docs/install）。インストール後にアプリを再起動すると認識されます。";

/// gcloud で Spanner の instance/database を列挙して EnvProfile を作る（ブロッキング）。
/// project 未指定なら gcloud config の既定 project を使う。
fn gcloud_discover(mut project: String) -> Result<Vec<EnvProfile>, String> {
    // gcloud を実行して value 行を取り出す
    let run = |args: &[&str]| -> Result<Vec<String>, String> {
        let out = std::process::Command::new(gcloud_bin())
            .args(args)
            .output()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    GCLOUD_MISSING.to_string()
                } else {
                    format!("gcloud 実行失敗: {e}")
                }
            })?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            return Err(err
                .lines()
                .last()
                .unwrap_or("gcloud エラー")
                .trim()
                .to_string());
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| l.trim().rsplit('/').next().unwrap_or("").to_string())
            .filter(|s| !s.is_empty())
            .collect())
    };

    if project.is_empty() {
        project = run(&["config", "get-value", "project"])?
            .into_iter()
            .next()
            .unwrap_or_default();
        if project.is_empty() {
            return Err("project を指定するか gcloud config を設定してください".into());
        }
    }

    let instances = run(&[
        "spanner",
        "instances",
        "list",
        "--project",
        &project,
        "--format=value(name)",
    ])?;
    let mut profiles = Vec::new();
    for inst in &instances {
        let dbs = run(&[
            "spanner",
            "databases",
            "list",
            "--instance",
            inst,
            "--project",
            &project,
            "--format=value(name)",
        ])
        .unwrap_or_default();
        for db in dbs {
            profiles.push(EnvProfile {
                name: format!("{project}/{inst}/{db}"),
                project: project.clone(),
                instance: inst.clone(),
                database: db,
            });
        }
    }
    if profiles.is_empty() {
        return Err(format!(
            "{project} に Spanner database が見つかりませんでした"
        ));
    }
    Ok(profiles)
}

const LAYOUT_FILE: &str = "schema_layout.json";

fn save_layout(positions: &HashMap<String, egui::Pos2>) -> std::io::Result<()> {
    let map: HashMap<&String, [f32; 2]> = positions.iter().map(|(k, p)| (k, [p.x, p.y])).collect();
    let json = serde_json::to_string_pretty(&map).unwrap_or_else(|_| "{}".into());
    std::fs::write(LAYOUT_FILE, json)
}

fn load_layout() -> HashMap<String, egui::Pos2> {
    let Ok(text) = std::fs::read_to_string(LAYOUT_FILE) else {
        return HashMap::new();
    };
    let map: HashMap<String, [f32; 2]> = serde_json::from_str(&text).unwrap_or_default();
    map.into_iter()
        .map(|(k, v)| (k, egui::pos2(v[0], v[1])))
        .collect()
}

/// 依存の深さでレベル分けし、各ノードの矩形（ワールド座標）を返す。
fn layout_nodes(graph: &SchemaGraph, widths: &HashMap<String, f32>) -> HashMap<String, egui::Rect> {
    // レベル = 依存の深さ（不動点反復、循環は反復回数で打ち切り）
    let mut level: HashMap<&str, usize> =
        graph.nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    for _ in 0..graph.nodes.len().max(1) {
        let mut changed = false;
        for e in &graph.edges {
            let lt = *level.get(e.to.as_str()).unwrap_or(&0);
            if let Some(lf) = level.get_mut(e.from.as_str()) {
                if *lf < lt + 1 {
                    *lf = lt + 1;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    // レベルごとにノードをまとめる（元の順序を保つ）
    let max_level = level.values().copied().max().unwrap_or(0);
    let mut by_level: Vec<Vec<&TableNode>> = vec![Vec::new(); max_level + 1];
    for node in &graph.nodes {
        let l = *level.get(node.name.as_str()).unwrap_or(&0);
        by_level[l].push(node);
    }

    let node_h = |n: &TableNode| -> f32 {
        let mut h = HEADER_H + n.columns.len() as f32 * ROW_H + 6.0;
        if !n.indexes.is_empty() {
            h += SECTION_H + n.indexes.len() as f32 * ROW_H;
        }
        h
    };

    let node_w = |n: &TableNode| widths.get(&n.name).copied().unwrap_or(NODE_W);

    let mut out = HashMap::new();
    let mut y = 0.0;
    for row in &by_level {
        let row_h = row.iter().map(|n| node_h(n)).fold(0.0_f32, f32::max);
        let mut x = 0.0;
        for n in row {
            let w = node_w(n);
            let h = node_h(n);
            out.insert(
                n.name.clone(),
                egui::Rect::from_min_size(egui::pos2(x, y), egui::vec2(w, h)),
            );
            x += w + H_GAP;
        }
        y += row_h + V_GAP;
    }
    out
}

/// クラスタ構成図を入れ子ボックス（Cluster > Node > Pod > コンテナ）で描く。
/// 矢印は Service → 背後 Pod（通信経路）。パン/ズーム対応。
fn draw_topology(
    ui: &mut egui::Ui,
    topo: Option<&k8s::KubeTopology>,
    pan: &mut egui::Vec2,
    zoom: &mut f32,
    selected: &mut Option<String>,
) {
    let Some(topo) = topo else {
        centered_hint(ui, "読み込み中…");
        return;
    };
    if let Some(e) = &topo.error {
        ui.colored_label(
            egui::Color32::from_rgb(248, 113, 113),
            format!("エラー: {e}"),
        );
        return;
    }
    if topo.pods.is_empty() {
        centered_hint(ui, "Pod がありません");
        return;
    }

    let rect = ui.available_rect_before_wrap();
    let bg = ui.interact(rect, ui.id().with("topo_bg"), egui::Sense::click_and_drag());
    if bg.dragged() {
        *pan += bg.drag_delta();
    }
    if bg.clicked() {
        *selected = None;
    }
    if bg.hovered() {
        let scroll = ui.input(|i| i.raw_scroll_delta.y);
        if scroll != 0.0 {
            let f = (1.0 + scroll * 0.0015).clamp(0.85, 1.18);
            *zoom = (*zoom * f).clamp(0.3, 3.0);
        }
    }
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, BASE);

    // ── レイアウト定数（ワールド座標） ──
    let (cw, ch, cgap) = (210.0_f32, 24.0_f32, 6.0_f32); // コンテナ（名前+使用量を表示）
    let (pod_pad, pod_head, pod_gap) = (8.0_f32, 22.0_f32, 12.0_f32);
    let (node_pad, node_head, node_gap) = (12.0_f32, 26.0_f32, 18.0_f32);
    let (cl_pad, cl_head) = (16.0_f32, 34.0_f32);
    let (svc_w, svc_h, svc_gap) = (160.0_f32, 30.0_f32, 12.0_f32);
    let budget = 1700.0_f32;

    // Pod を所属ノードでまとめる
    let mut by_node: std::collections::BTreeMap<String, Vec<&k8s::PodInfo>> = Default::default();
    for p in &topo.pods {
        let key = if p.node.is_empty() {
            "(未スケジュール)".to_string()
        } else {
            p.node.clone()
        };
        by_node.entry(key).or_default().push(p);
    }

    let pod_size = |p: &k8s::PodInfo| -> egui::Vec2 {
        let n = p.containers.len();
        let inner = if n == 0 {
            0.0
        } else {
            n as f32 * ch + (n as f32 - 1.0) * cgap
        };
        egui::vec2(cw + pod_pad * 2.0, pod_head + pod_pad + inner + pod_pad)
    };

    // 各ノードのサイズ（Pod は横並び）
    struct NodeLayout<'a> {
        name: String,
        pods: Vec<(&'a k8s::PodInfo, egui::Vec2)>,
        size: egui::Vec2,
    }
    let mut nodes: Vec<NodeLayout> = Vec::new();
    for (nname, pods) in &by_node {
        let sizes: Vec<(&k8s::PodInfo, egui::Vec2)> =
            pods.iter().map(|p| (*p, pod_size(p))).collect();
        let tot: f32 = sizes.iter().map(|(_, s)| s.x).sum();
        let k = sizes.len().max(1) as f32;
        let w = node_pad * 2.0 + tot + pod_gap * (k - 1.0);
        let maxh = sizes.iter().map(|(_, s)| s.y).fold(0.0, f32::max);
        let h = node_head + node_pad * 2.0 + maxh;
        nodes.push(NodeLayout {
            name: nname.clone(),
            pods: sizes,
            size: egui::vec2(w, h),
        });
    }

    // Service 行を上部に折り返し配置
    let mut svc_rects: Vec<(usize, egui::Rect)> = Vec::new();
    let (mut cx, mut cy, mut max_x) = (cl_pad, cl_head, cl_pad);
    for i in 0..topo.services.len() {
        if cx > cl_pad && cx + svc_w > cl_pad + budget {
            cx = cl_pad;
            cy += svc_h + svc_gap;
        }
        svc_rects.push((
            i,
            egui::Rect::from_min_size(egui::pos2(cx, cy), egui::vec2(svc_w, svc_h)),
        ));
        cx += svc_w + svc_gap;
        max_x = max_x.max(cx - svc_gap);
    }
    let nodes_top = if topo.services.is_empty() {
        cl_head
    } else {
        cy + svc_h + node_gap
    };

    // ノードを折り返し配置し、各 Pod・コンテナの矩形を確定
    let mut node_rects: Vec<(String, egui::Rect)> = Vec::new();
    let mut pod_draw: Vec<(&k8s::PodInfo, egui::Rect)> = Vec::new();
    let mut pod_rect_by_key: HashMap<(String, String), egui::Rect> = HashMap::new();
    let (mut nx, mut ny, mut row_h) = (cl_pad, nodes_top, 0.0_f32);
    for nl in &nodes {
        let size = nl.size;
        if nx > cl_pad && nx + size.x > cl_pad + budget {
            nx = cl_pad;
            ny += row_h + node_gap;
            row_h = 0.0;
        }
        let nrect = egui::Rect::from_min_size(egui::pos2(nx, ny), size);
        node_rects.push((nl.name.clone(), nrect));
        // Pod を横並び
        let mut px = nx + node_pad;
        let py = ny + node_head + node_pad;
        for (p, ps) in &nl.pods {
            let prect = egui::Rect::from_min_size(egui::pos2(px, py), *ps);
            pod_draw.push((p, prect));
            pod_rect_by_key.insert((p.ns.clone(), p.name.clone()), prect);
            px += ps.x + pod_gap;
        }
        nx += size.x + node_gap;
        row_h = row_h.max(size.y);
        max_x = max_x.max(nx - node_gap);
    }
    let content_bottom = ny + row_h;
    let cluster = egui::Rect::from_min_size(
        egui::pos2(0.0, 0.0),
        egui::vec2(max_x + cl_pad, content_bottom + cl_pad),
    );

    // ── ワールド→スクリーン変換 ──
    let z = *zoom;
    let zt = (z * 8.0).round() / 8.0; // フォントサイズだけ離散化（アトラス再生成抑制）
    let origin = rect.min + *pan;
    let tf = |p: egui::Pos2| origin + (p.to_vec2() * z);
    let tr = |r: egui::Rect| egui::Rect::from_min_max(tf(r.min), tf(r.max));
    let fs = |s: f32| (s * zt).max(6.0);
    let round = |r: f32| egui::Rounding::same(r * z);

    // 選択 Pod（"ns/name"）に関係する通信だけ強調するための集合
    let sel = selected.clone();

    // Cluster
    let clr = tr(cluster);
    painter.rect_filled(clr, round(10.0), egui::Color32::from_rgb(24, 33, 48));
    painter.rect_stroke(clr, round(10.0), egui::Stroke::new(1.5, ACCENT));
    painter.text(
        clr.left_top() + egui::vec2(12.0 * z, (cl_head * 0.5) * z),
        egui::Align2::LEFT_CENTER,
        "Cluster",
        egui::FontId::proportional(fs(15.0)),
        TEXT,
    );

    // Nodes
    for (nname, nrect) in &node_rects {
        let r = tr(*nrect);
        let pc = painter.with_clip_rect(r.intersect(rect));
        painter.rect_filled(r, round(8.0), egui::Color32::from_rgb(28, 52, 70));
        painter.rect_stroke(
            r,
            round(8.0),
            egui::Stroke::new(1.0, ACCENT.gamma_multiply(0.7)),
        );
        pc.text(
            r.left_top() + egui::vec2(10.0 * z, (node_head * 0.5) * z),
            egui::Align2::LEFT_CENTER,
            format!("Node · {nname}"),
            egui::FontId::proportional(fs(13.0)),
            TEXT,
        );
    }

    // Pods + コンテナ（クリックで選択トグル）
    for (p, prect) in &pod_draw {
        let r = tr(*prect);
        if !rect.intersects(r) {
            continue;
        }
        let key = format!("{}/{}", p.ns, p.name);
        let is_sel = sel.as_deref() == Some(key.as_str());
        let pc = painter.with_clip_rect(r.intersect(rect));
        painter.rect_filled(r, round(6.0), egui::Color32::from_rgb(34, 68, 90));
        let border = if is_sel {
            egui::Stroke::new(2.0, COMM_COLOR)
        } else {
            egui::Stroke::new(1.0, ACCENT.gamma_multiply(0.5))
        };
        painter.rect_stroke(r, round(6.0), border);
        // ヘッダ（Pod 名）＋クリック判定
        let header = egui::Rect::from_min_max(r.min, egui::pos2(r.max.x, r.min.y + pod_head * z));
        pc.text(
            header.left_center() + egui::vec2(8.0 * z, 0.0),
            egui::Align2::LEFT_CENTER,
            &p.name,
            egui::FontId::proportional(fs(12.0)),
            phase_color(&p.phase),
        );
        // Pod 合計の使用率（limit 比。ヘッダ右）
        let cpu_lim: f64 = p.containers.iter().map(|c| c.cpu_limit_milli).sum();
        let mem_lim: f64 = p.containers.iter().map(|c| c.mem_limit_mib).sum();
        let total = match (cpu_lim > 0.0, mem_lim > 0.0) {
            (true, true) => format!(
                "Σ CPU {:.0}% Mem {:.0}%",
                p.cpu_milli / cpu_lim * 100.0,
                p.mem_mib / mem_lim * 100.0
            ),
            _ if p.cpu_milli > 0.0 || p.mem_mib > 0.0 => {
                format!("Σ {:.0}m {:.0}Mi", p.cpu_milli, p.mem_mib)
            }
            _ => String::new(),
        };
        if !total.is_empty() {
            pc.text(
                header.right_center() - egui::vec2(8.0 * z, 0.0),
                egui::Align2::RIGHT_CENTER,
                total,
                egui::FontId::monospace(fs(10.0)),
                MUTED,
            );
        }
        let pid = ui.id().with(("topo_pod", key.as_str()));
        if ui.interact(header, pid, egui::Sense::click()).clicked() {
            *selected = if is_sel { None } else { Some(key.clone()) };
        }
        // コンテナ
        let mut y = r.min.y + (pod_head + pod_pad) * z;
        for c in &p.containers {
            let crect = egui::Rect::from_min_size(
                egui::pos2(r.min.x + pod_pad * z, y),
                egui::vec2(cw * z, ch * z),
            );
            pc.rect_filled(crect, round(4.0), egui::Color32::from_rgb(226, 232, 240));
            pc.rect_stroke(crect, round(4.0), egui::Stroke::new(1.0, BORDER));
            let label = if c.init {
                format!("{} (init)", c.name)
            } else {
                c.name.clone()
            };
            pc.text(
                crect.left_center() + egui::vec2(6.0 * z, 0.0),
                egui::Align2::LEFT_CENTER,
                label,
                egui::FontId::monospace(fs(11.0)),
                egui::Color32::from_rgb(12, 18, 30),
            );
            // CPU / メモリ使用率（limit 比、右寄せ）。limit 未設定や metrics 無しは絶対値/—
            let usage = usage_text(c);
            if !usage.is_empty() {
                pc.text(
                    crect.right_center() - egui::vec2(6.0 * z, 0.0),
                    egui::Align2::RIGHT_CENTER,
                    usage,
                    egui::FontId::monospace(fs(10.0)),
                    egui::Color32::from_rgb(71, 85, 105),
                );
            }
            y += (ch + cgap) * z;
        }
    }

    // Services + 通信矢印
    for (i, srect) in &svc_rects {
        let svc = &topo.services[*i];
        let r = tr(*srect);
        // この Service が選択 Pod に関係するか
        let related = sel
            .as_deref()
            .is_none_or(|s| svc.pods.iter().any(|pn| format!("{}/{}", svc.ns, pn) == s));
        let svc_fill = if related {
            egui::Color32::from_rgb(20, 60, 50)
        } else {
            egui::Color32::from_rgb(20, 60, 50).gamma_multiply(0.4)
        };
        painter.rect_filled(r, round(6.0), svc_fill);
        painter.rect_stroke(r, round(6.0), egui::Stroke::new(1.0, COMM_COLOR));
        painter.with_clip_rect(r.intersect(rect)).text(
            r.center(),
            egui::Align2::CENTER_CENTER,
            format!("svc/{}", svc.name),
            egui::FontId::proportional(fs(12.0)),
            COMM_COLOR,
        );
        // 背後 Pod へ矢印
        let from = egui::pos2(r.center().x, r.bottom());
        for pn in &svc.pods {
            let Some(prect) = pod_rect_by_key.get(&(svc.ns.clone(), pn.clone())) else {
                continue;
            };
            let pr = tr(*prect);
            let to = egui::pos2(pr.center().x, pr.top());
            let active = sel.as_deref().is_none_or(|s| {
                s == format!("{}/{}", svc.ns, pn)
                    || svc.pods.iter().any(|x| format!("{}/{}", svc.ns, x) == s)
            });
            let color = if active {
                COMM_COLOR
            } else {
                COMM_COLOR.gamma_multiply(0.18)
            };
            let bend_y = (from.y + to.y) * 0.5;
            draw_arrow(&painter, from, to, bend_y, color, z);
        }
    }
}

/// 直交ルーティングの折れ線矢印を描く（from=子の上端 → to=親の下端）。
/// 縦→横→縦の L/Z 字に折れ、終点 `to` に矢じりを付ける。`bend_y` で水平セグメントの高さを指定。
fn draw_arrow(
    painter: &egui::Painter,
    from: egui::Pos2,
    to: egui::Pos2,
    bend_y: f32,
    color: egui::Color32,
    z: f32,
) {
    let stroke = egui::Stroke::new((1.6 * z).max(1.0), color);
    let p1 = egui::pos2(from.x, bend_y);
    let p2 = egui::pos2(to.x, bend_y);
    // 折れ線本体
    painter.add(egui::Shape::line(vec![from, p1, p2, to], stroke));

    // 矢じり（終点へ向かう向き = p2→to。通常は垂直方向）
    let dir = (to - p2).normalized();
    let dir = if dir.is_finite() {
        dir
    } else {
        egui::vec2(0.0, -1.0)
    };
    let n = egui::vec2(-dir.y, dir.x);
    let size = (9.0 * z).max(5.0);
    let left = to - dir * size + n * (size * 0.5);
    let right = to - dir * size - n * (size * 0.5);
    painter.add(egui::Shape::convex_polygon(
        vec![to, left, right],
        color,
        egui::Stroke::NONE,
    ));
}

/// クエリ結果のステータス（実行中スピナー / 行数・時間 / エラー）を表示する。
fn result_status(ui: &mut egui::Ui, pending: bool, result: Option<&QueryOutcome>) {
    if pending {
        ui.spinner();
        ui.label(egui::RichText::new("実行中…").color(MUTED));
    } else if let Some(r) = result {
        if let Some(e) = &r.error {
            ui.colored_label(
                egui::Color32::from_rgb(248, 113, 113),
                format!("エラー: {e}"),
            );
        } else {
            let mut msg = format!("{} 行 / {} ms", r.rows.len(), r.elapsed_ms);
            if r.truncated {
                msg.push_str(&format!(" (上限 {} 行で打ち切り)", r.rows.len()));
            }
            ui.label(egui::RichText::new(msg).color(MUTED));
        }
    }
}

/// 結果をスクロール可能なグリッドで表示する。
/// 強化版の結果グリッド。検索フィルタ・列ソート・行番号・NULL強調・行/列コピーに対応。
/// ヘッダクリックでソートが変わった場合は新しいソート状態を返す。
fn data_result_grid(
    ui: &mut egui::Ui,
    result: &QueryOutcome,
    search: &str,
    sort: Option<(usize, bool)>,
) -> Option<Option<(usize, bool)>> {
    // フィルタ
    let q = search.trim();
    let mut order: Vec<usize> = result
        .rows
        .iter()
        .enumerate()
        .filter(|(_, r)| q.is_empty() || r.iter().any(|c| line_contains_ci(c, q)))
        .map(|(i, _)| i)
        .collect();
    // ソート
    if let Some((col, asc)) = sort {
        order.sort_by(|&a, &b| {
            let va = result.rows[a].get(col).map(String::as_str).unwrap_or("");
            let vb = result.rows[b].get(col).map(String::as_str).unwrap_or("");
            let o = cmp_cell(va, vb);
            if asc {
                o
            } else {
                o.reverse()
            }
        });
    }

    if !q.is_empty() {
        ui.label(
            egui::RichText::new(format!("{} / {} 行", order.len(), result.rows.len()))
                .color(MUTED)
                .small(),
        );
    }

    let mut new_sort: Option<Option<(usize, bool)>> = None;
    let ncols = result.columns.len() + 1; // 行番号列
    egui::ScrollArea::both()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            egui::Grid::new("data_grid")
                .striped(true)
                .num_columns(ncols)
                .spacing([18.0, 6.0])
                .show(ui, |ui| {
                    // ヘッダ
                    ui.label(egui::RichText::new("#").color(MUTED).small());
                    for (ci, c) in result.columns.iter().enumerate() {
                        let arrow = match sort {
                            Some((sc, asc)) if sc == ci => {
                                if asc {
                                    " ▲"
                                } else {
                                    " ▼"
                                }
                            }
                            _ => "",
                        };
                        let resp = ui
                            .add(
                                egui::Label::new(
                                    egui::RichText::new(format!("{c}{arrow}"))
                                        .color(ACCENT)
                                        .strong(),
                                )
                                .sense(egui::Sense::click()),
                            )
                            .on_hover_text("クリックでソート / 右クリックで列コピー");
                        if resp.clicked() {
                            let asc = !matches!(sort, Some((sc, a)) if sc == ci && a);
                            new_sort = Some(Some((ci, asc)));
                        }
                        let col_idx = ci;
                        resp.context_menu(|ui| {
                            if ui.button("列名をコピー").clicked() {
                                ui.ctx().copy_text(result.columns[col_idx].clone());
                                ui.close_menu();
                            }
                            if ui.button("列の値をコピー").clicked() {
                                let vals: Vec<String> = order
                                    .iter()
                                    .map(|&r| {
                                        result.rows[r].get(col_idx).cloned().unwrap_or_default()
                                    })
                                    .collect();
                                ui.ctx().copy_text(vals.join("\n"));
                                ui.close_menu();
                            }
                        });
                    }
                    ui.end_row();

                    // 行
                    for (n, &ri) in order.iter().enumerate() {
                        let row = &result.rows[ri];
                        ui.label(
                            egui::RichText::new(format!("{}", n + 1))
                                .color(MUTED)
                                .small(),
                        );
                        for cell in row {
                            let is_null = cell == "NULL";
                            let rich = if is_null {
                                egui::RichText::new("NULL").color(MUTED).italics()
                            } else {
                                egui::RichText::new(cell)
                            };
                            let resp = ui
                                .add(egui::Label::new(rich).sense(egui::Sense::click()))
                                .on_hover_text("クリックでコピー / 右クリックで行コピー");
                            if resp.clicked() {
                                ui.ctx().copy_text(cell.clone());
                            }
                            resp.context_menu(|ui| {
                                if ui.button("セルをコピー").clicked() {
                                    ui.ctx().copy_text(cell.clone());
                                    ui.close_menu();
                                }
                                if ui.button("行をコピー (TSV)").clicked() {
                                    ui.ctx().copy_text(row.join("\t"));
                                    ui.close_menu();
                                }
                            });
                        }
                        ui.end_row();
                    }
                });
        });
    new_sort
}

/// 結果を CSV 文字列にする（カンマ/引用符/改行を含む値はクォート）。
/// 結果を CSV ファイルに保存する。保存先は ~/Downloads（無ければ HOME / カレント）。
fn save_csv(result: &QueryOutcome) -> std::io::Result<std::path::PathBuf> {
    let home = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let downloads = home.join("Downloads");
    let dir = if downloads.is_dir() { downloads } else { home };
    let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let path = dir.join(format!("spanner_export_{ts}.csv"));
    std::fs::write(&path, to_csv(result))?;
    // 保存先を Finder で表示（macOS。失敗しても無視）
    let _ = std::process::Command::new("open")
        .arg("-R")
        .arg(&path)
        .spawn();
    Ok(path)
}

/// マッピング用プレビューで読むバイト数の上限（全行は溜めない）。
const PREVIEW_BYTES: usize = 128 * 1024;
/// プレビュー表示の最大データ行数。
const PREVIEW_ROWS: usize = 50;

/// ファイルの先頭 `max_bytes` までを生バイトで読む（プレビュー用・文字コード未確定）。
fn read_file_prefix(path: &std::path::Path, max_bytes: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut buf = vec![0u8; max_bytes];
    let n = f.read(&mut buf)?;
    buf.truncate(n);
    Ok(buf)
}

/// GCS オブジェクト/フォルダのフルパスから末尾セグメント（表示名）を取り出す。
/// フォルダ（末尾 /）は "sub/"、オブジェクトは "file.csv" のように返す。
fn leaf_name(path: &str) -> String {
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    let leaf = trimmed.rsplit('/').next().unwrap_or(trimmed);
    if path.ends_with('/') {
        format!("{leaf}/")
    } else {
        leaf.to_string()
    }
}

/// 現在の一覧位置 `gs://bucket/prefix` から 1 つ上の階層を返す。ルートなら None。
fn parent_location(bucket: &str, listed_at: &str) -> Option<String> {
    let head = format!("gs://{bucket}/");
    let prefix = listed_at.strip_prefix(&head).unwrap_or("");
    if prefix.is_empty() {
        return None; // 既にバケット直下
    }
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    let parent = match trimmed.rfind('/') {
        Some(i) => &trimmed[..=i], // スラッシュ込みで残す
        None => "",                // ルートへ
    };
    Some(format!("gs://{bucket}/{parent}"))
}

/// macOS のネイティブダイアログでファイルを 1 つ選ぶ（osascript 利用）。
/// キャンセル時や失敗時は None。
fn pick_csv_file() -> Option<std::path::PathBuf> {
    let script = "POSIX path of (choose file with prompt \"インポートする CSV を選択\" \
                  of type {\"csv\", \"txt\", \"public.comma-separated-values-text\", \"public.plain-text\"})";
    let out = std::process::Command::new("osascript")
        .args(["-e", script])
        .output()
        .ok()?;
    if !out.status.success() {
        return None; // キャンセル含む
    }
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if path.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(path))
    }
}

fn to_csv(result: &QueryOutcome) -> String {
    fn esc(s: &str) -> String {
        if s.contains(',') || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    }
    let mut out = String::new();
    out.push_str(
        &result
            .columns
            .iter()
            .map(|c| esc(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');
    for row in &result.rows {
        out.push_str(&row.iter().map(|c| esc(c)).collect::<Vec<_>>().join(","));
        out.push('\n');
    }
    out
}

/// スキーマ情報から CREATE TABLE 風の DDL を組み立てる（近似。NOT NULL 等は省略）。
fn build_ddl(node: &TableNode) -> String {
    let mut s = format!("CREATE TABLE `{}` (\n", node.name);
    let cols: Vec<String> = node
        .columns
        .iter()
        .map(|c| format!("  `{}` {}", c.name, c.ty))
        .collect();
    s.push_str(&cols.join(",\n"));
    s.push_str("\n)");
    let pk: Vec<String> = node
        .columns
        .iter()
        .filter(|c| c.pk)
        .map(|c| format!("`{}`", c.name))
        .collect();
    if !pk.is_empty() {
        s.push_str(&format!(" PRIMARY KEY ({})", pk.join(", ")));
    }
    s.push_str(";\n");
    if !node.indexes.is_empty() {
        s.push_str("\n-- インデックス:\n");
        for idx in &node.indexes {
            s.push_str(&format!("--   {idx}\n"));
        }
    }
    s.push_str("\n-- ※ INFORMATION_SCHEMA からの近似 DDL です\n");
    s
}

/// 中央寄せの控えめなヒント表示。
fn centered_hint(ui: &mut egui::Ui, text: &str) {
    ui.add_space(20.0);
    ui.vertical_centered(|ui| {
        ui.label(egui::RichText::new(text).color(MUTED));
    });
}

/// モダンなダークテーマを適用する（配色・角丸・余白・フォントサイズ）。
// ── 表面（背景レイヤー） ──
const BASE: egui::Color32 = egui::Color32::from_rgb(13, 15, 19); // 最暗（アクティビティバー等）
const PANEL: egui::Color32 = egui::Color32::from_rgb(22, 24, 30); // コンテンツ
const ELEVATED: egui::Color32 = egui::Color32::from_rgb(29, 32, 39); // ウィンドウ/カード
const HOVER: egui::Color32 = egui::Color32::from_rgb(38, 42, 51);
const BORDER: egui::Color32 = egui::Color32::from_rgb(42, 47, 57);
const INPUT_BG: egui::Color32 = egui::Color32::from_rgb(16, 18, 23);
const ROW_ALT: egui::Color32 = egui::Color32::from_rgb(27, 30, 37); // 縞模様

fn setup_style(ctx: &egui::Context) {
    use egui::FontFamily::{Monospace, Proportional};
    use egui::{FontId, Rounding, Stroke, TextStyle};

    let mut v = egui::Visuals::dark();
    v.override_text_color = Some(TEXT);
    v.panel_fill = PANEL;
    v.window_fill = ELEVATED;
    v.window_stroke = Stroke::new(1.0, BORDER);
    v.faint_bg_color = ROW_ALT;
    v.extreme_bg_color = INPUT_BG;
    v.code_bg_color = INPUT_BG;
    v.hyperlink_color = ACCENT;
    v.selection.bg_fill = egui::Color32::from_rgba_unmultiplied(56, 189, 248, 48);
    v.selection.stroke = Stroke::new(1.0, ACCENT);

    // 角丸・影
    v.window_rounding = Rounding::same(10.0);
    v.menu_rounding = Rounding::same(8.0);
    v.window_shadow = egui::epaint::Shadow {
        offset: egui::vec2(0.0, 6.0),
        blur: 24.0,
        spread: 0.0,
        color: egui::Color32::from_black_alpha(110),
    };
    v.popup_shadow = egui::epaint::Shadow {
        offset: egui::vec2(0.0, 3.0),
        blur: 14.0,
        spread: 0.0,
        color: egui::Color32::from_black_alpha(90),
    };

    // ウィジェット状態（フラットで控えめなボーダー、ホバーで浮く）
    let round = Rounding::same(6.0);
    let w = &mut v.widgets;
    w.noninteractive.rounding = round;
    w.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    w.noninteractive.fg_stroke = Stroke::new(1.0, TEXT);

    w.inactive.rounding = round;
    w.inactive.weak_bg_fill = egui::Color32::from_rgb(34, 38, 46);
    w.inactive.bg_fill = egui::Color32::from_rgb(34, 38, 46);
    w.inactive.bg_stroke = Stroke::new(1.0, BORDER);
    w.inactive.fg_stroke = Stroke::new(1.0, egui::Color32::from_rgb(205, 211, 222));

    w.hovered.rounding = round;
    w.hovered.weak_bg_fill = HOVER;
    w.hovered.bg_fill = HOVER;
    w.hovered.bg_stroke = Stroke::new(1.0, ACCENT.gamma_multiply(0.7));
    w.hovered.fg_stroke = Stroke::new(1.0, TEXT);
    w.hovered.expansion = 1.0;

    w.active.rounding = round;
    w.active.weak_bg_fill = ACCENT.gamma_multiply(0.35);
    w.active.bg_fill = ACCENT.gamma_multiply(0.35);
    w.active.bg_stroke = Stroke::new(1.0, ACCENT);
    w.active.fg_stroke = Stroke::new(1.0, egui::Color32::WHITE);
    w.active.expansion = 1.0;

    w.open.rounding = round;
    w.open.weak_bg_fill = HOVER;
    w.open.bg_stroke = Stroke::new(1.0, BORDER);

    ctx.set_visuals(v);

    ctx.style_mut(|s| {
        // フォントを少し小さく＋余白も合わせて詰める。
        s.spacing.item_spacing = egui::vec2(7.0, 6.0);
        s.spacing.button_padding = egui::vec2(10.0, 6.0);
        s.spacing.interact_size.y = 25.0;
        s.spacing.window_margin = egui::Margin::same(11.0);
        s.spacing.menu_margin = egui::Margin::same(8.0);
        s.spacing.scroll.bar_width = 10.0;
        s.spacing.scroll.floating = false;
        s.text_styles = [
            (TextStyle::Heading, FontId::new(18.0, Proportional)),
            (TextStyle::Body, FontId::new(13.0, Proportional)),
            (TextStyle::Button, FontId::new(13.0, Proportional)),
            (TextStyle::Monospace, FontId::new(12.5, Monospace)),
            (TextStyle::Small, FontId::new(11.0, Proportional)),
        ]
        .into_iter()
        .collect();
    });
}

/// 日本語対応のシステムフォントを読み込み、既定フォントのフォールバックに追加する。
/// 見つからなければ何もしない（英数字は既定フォントで表示される）。
fn install_japanese_font(ctx: &egui::Context) {
    // 単一フェイスの .ttf を優先し、無ければヒラギノ(.ttc)へフォールバック
    const CANDIDATES: [&str; 4] = [
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
        "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
    ];
    let Some(bytes) = CANDIDATES.iter().find_map(|p| std::fs::read(p).ok()) else {
        return;
    };

    let mut fonts = egui::FontDefinitions::default();
    fonts
        .font_data
        .insert("jp".to_owned(), egui::FontData::from_owned(bytes));
    // 既定（英数）フォントの後ろに足すことで、未収録の和文だけ JP フォントが埋める
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .push("jp".to_owned());
    }
    ctx.set_fonts(fonts);
}

/// ステータスバー用の小さなカラーチップ（ラベル + 値）。
fn chip(ui: &mut egui::Ui, label: &str, value: &str, color: egui::Color32) {
    egui::Frame::none()
        .fill(ELEVATED)
        .rounding(egui::Rounding::same(7.0))
        .inner_margin(egui::Margin::symmetric(10.0, 4.0))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).color(MUTED).small());
            ui.label(egui::RichText::new(value).color(color).strong());
        });
}

/// 容量表示。1 ノード = 1000 PU。ちょうどノード単位ならノード数も併記。
fn capacity_label(pu: f64) -> String {
    let pu = pu.round() as i64;
    if pu > 0 && pu % 1000 == 0 {
        format!("{pu} PU (= {} ノード)", pu / 1000)
    } else {
        format!("{pu} PU")
    }
}

fn human_bytes(b: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", UNITS[i])
}

/// 取込の進捗割合（処理バイト ÷ 全体）。全体が不明/0 なら None（不確定表示）。
fn progress_fraction(bytes_done: u64, bytes_total: Option<u64>) -> Option<f32> {
    bytes_total
        .filter(|t| *t > 0)
        .map(|t| (bytes_done as f32 / t as f32).clamp(0.0, 1.0))
}

/// 取込の速度（rows/s）と残り時間（ETA）の表示文字列。情報が足りなければ空。
fn import_rate_eta(started: Option<std::time::Instant>, p: &ImportProg) -> String {
    match started {
        Some(t) => rate_eta(t.elapsed().as_secs_f64(), p.written, p.frac),
        None => String::new(),
    }
}

/// 速度・ETA 文字列の純粋ロジック（テスト用に時間を引数化）。
fn rate_eta(elapsed: f64, written: usize, frac: Option<f32>) -> String {
    if elapsed < 0.5 || written == 0 {
        return String::new();
    }
    let rps = written as f64 / elapsed;
    let speed = if rps >= 1000.0 {
        format!("{:.0}k 行/s", rps / 1000.0)
    } else {
        format!("{rps:.0} 行/s")
    };
    // ETA は進捗割合から。frac が分かるときだけ。
    let eta = match frac {
        Some(f) if f > 0.01 && f < 0.999 => {
            let remain = elapsed * (1.0 - f as f64) / f as f64;
            format!("  ·  残り ~{}", fmt_duration(remain))
        }
        _ => String::new(),
    };
    format!("  ·  {speed}{eta}")
}

/// 秒数を人に読みやすい残り時間表記にする。
fn fmt_duration(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    if s >= 3600 {
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    } else if s >= 60 {
        format!("{}m{}s", s / 60, s % 60)
    } else {
        format!("{s}s")
    }
}

/// 未割当の主キー列名を返す（空なら OK）。
fn unmapped_pks(table_columns: &[query::Column], mapping: &[Option<usize>]) -> Vec<String> {
    table_columns
        .iter()
        .zip(mapping.iter())
        .filter(|(c, m)| c.pk && m.is_none())
        .map(|(c, _)| c.name.clone())
        .collect()
}

/// フォルダ一括用: オブジェクト群から *.csv のみ抽出し (gs:// URI, 表示名) を返す。
fn csv_object_uris(bucket: &str, objects: &[String]) -> Vec<(String, String)> {
    objects
        .iter()
        .filter(|o| o.to_lowercase().ends_with(".csv"))
        .map(|o| (format!("gs://{bucket}/{o}"), leaf_name(o)))
        .collect()
}

/// ジョブの状態を日本語ラベルにする。
fn job_status_label(s: JobStatus) -> &'static str {
    match s {
        JobStatus::Queued => "待機",
        JobStatus::Running => "実行中",
        JobStatus::Done => "完了",
        JobStatus::Failed => "失敗",
        JobStatus::Cancelled => "中断",
    }
}

/// 方式ラベル（挿入のみ / 上書き挿入）。
fn mode_label(mode: query::ImportMode) -> &'static str {
    match mode {
        query::ImportMode::Insert => "挿入のみ",
        query::ImportMode::InsertOrUpdate => "上書き挿入",
    }
}

/// ジョブの取得元の表示文字列。
fn job_source(req: &query::ImportRequest) -> String {
    match &req.source {
        query::ImportSource::File(p) => p.display().to_string(),
        query::ImportSource::Gcs(u) => u.clone(),
    }
}

/// 証跡レポート（Markdown）を組み立てる。
fn report_markdown(jobs: &[ImportJob], ts: &chrono::DateTime<chrono::Local>) -> String {
    let mut s = String::new();
    s.push_str("# インポート証跡レポート\n\n");
    s.push_str(&format!("生成日時: {}\n\n", ts.format("%Y-%m-%d %H:%M:%S")));
    s.push_str(
        "| テーブル | 方式 | 状態 | 書込行数 | 総行数 | スキップ | 再開スキップ | 所要(ms) | リジェクト | エラー | ソース |\n",
    );
    s.push_str("|---|---|---|--:|--:|--:|--:|--:|---|---|---|\n");
    let mut total_written = 0usize;
    for j in jobs {
        let o = j.outcome.as_ref();
        let written = o.map(|o| o.written).unwrap_or(0);
        total_written += written;
        let mode = if o.map(|o| o.dry_run).unwrap_or(false) {
            "検証(ドライラン)".to_string()
        } else {
            mode_label(j.req.mode).to_string()
        };
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            j.req.table,
            mode,
            job_status_label(j.status),
            fmt_count(written),
            fmt_count(o.map(|o| o.total).unwrap_or(0)),
            fmt_count(o.map(|o| o.skipped).unwrap_or(0)),
            fmt_count(o.map(|o| o.resumed).unwrap_or(0)),
            o.map(|o| o.elapsed_ms).unwrap_or(0),
            o.and_then(|o| o.reject_path.clone()).unwrap_or_else(|| "-".into()),
            o.and_then(|o| o.error.clone()).unwrap_or_else(|| "-".into()),
            job_source(&j.req),
        ));
    }
    s.push_str(&format!(
        "\n合計: {} ジョブ / 書込 {} 行\n",
        jobs.len(),
        fmt_count(total_written)
    ));
    s.push_str(
        "\n※ 書込行数は BatchWrite で適用に成功した行数です。Spanner は行ごとの\n\
         「新規挿入/更新」内訳を返さないため、本レポートには内訳は含みません。\n",
    );
    s
}

/// 証跡レポート（CSV）を組み立てる。
fn report_csv(jobs: &[ImportJob]) -> String {
    let esc = |s: &str| {
        if s.contains(',') || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s.to_string()
        }
    };
    let mut out = String::from(
        "table,mode,status,written,total,skipped,resumed,elapsed_ms,reject_path,error,source\n",
    );
    for j in jobs {
        let o = j.outcome.as_ref();
        let mode = if o.map(|o| o.dry_run).unwrap_or(false) {
            "dry_run"
        } else {
            match j.req.mode {
                query::ImportMode::Insert => "insert",
                query::ImportMode::InsertOrUpdate => "insert_or_update",
            }
        };
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{}\n",
            esc(&j.req.table),
            mode,
            job_status_label(j.status),
            o.map(|o| o.written).unwrap_or(0),
            o.map(|o| o.total).unwrap_or(0),
            o.map(|o| o.skipped).unwrap_or(0),
            o.map(|o| o.resumed).unwrap_or(0),
            o.map(|o| o.elapsed_ms).unwrap_or(0),
            esc(&o.and_then(|o| o.reject_path.clone()).unwrap_or_default()),
            esc(&o.and_then(|o| o.error.clone()).unwrap_or_default()),
            esc(&job_source(&j.req)),
        ));
    }
    out
}

/// egui のスクリーンショット（ColorImage）を PNG として保存する。
fn save_screenshot_png(
    path: &std::path::Path,
    img: &egui::ColorImage,
) -> Result<(), image::ImageError> {
    let [w, h] = img.size;
    let mut rgba = Vec::with_capacity(w * h * 4);
    for px in &img.pixels {
        rgba.extend_from_slice(&[px.r(), px.g(), px.b(), px.a()]);
    }
    image::save_buffer(
        path,
        &rgba,
        w as u32,
        h as u32,
        image::ExtendedColorType::Rgba8,
    )
}

/// インポート完了の結果メッセージを組み立てる。
fn format_import_result(out: &query::ImportOutcome) -> String {
    let partial = out.error.is_some() && out.written > 0;
    let mut notes = String::new();
    if out.resumed > 0 {
        notes.push_str(&format!("・前回完了分 {} 行をスキップ", out.resumed));
    }
    if out.skipped > 0 {
        notes.push_str(&format!("・不正行 {} 行をスキップ", out.skipped));
    }
    if let Some(p) = &out.reject_path {
        notes.push_str(&format!("・リジェクト: {p}"));
    }
    if out.dry_run {
        return match &out.error {
            Some(e) => format!("検証で停止: {e}{notes}"),
            None => format!(
                "検証完了: {} 行中 {} 行が書き込み可能（不正 {} 行）{notes}",
                out.total, out.written, out.skipped
            ),
        };
    }
    if out.cancelled {
        return format!(
            "中断（{} 行まで取込）。再キューで続きから再開します。{notes}",
            out.written
        );
    }
    match &out.error {
        Some(e) if partial => format!(
            "{} 件まで書き込み後にエラー: {e}（再キューで続きから再開）{notes}",
            out.written
        ),
        Some(e) => format!("失敗: {e}{notes}"),
        None => format!(
            "{} / {} 行を取り込みました（{} ms）{notes}",
            out.written, out.total, out.elapsed_ms
        ),
    }
}

/// ドロップダウンの選択テキスト（空ならプレースホルダ）。
fn combo_text(val: &str, placeholder: &str) -> String {
    if val.is_empty() {
        format!("{placeholder}…")
    } else {
        val.to_string()
    }
}

/// 整数を 3 桁区切りで表示する（例: 1234567 → "1,234,567"）。
fn fmt_count(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GCS パスの末尾セグメント取り出し（フォルダ/ファイル）。
    #[test]
    fn leaf_name_folder_and_file() {
        assert_eq!(leaf_name("dir/sub/file.csv"), "file.csv");
        assert_eq!(leaf_name("dir/sub/"), "sub/");
        assert_eq!(leaf_name("file.csv"), "file.csv");
        assert_eq!(leaf_name("top/"), "top/");
    }

    /// 一覧位置から 1 つ上の階層（ルートは None）。
    #[test]
    fn parent_location_walks_up() {
        assert_eq!(
            parent_location("b", "gs://b/dir/sub/").as_deref(),
            Some("gs://b/dir/")
        );
        assert_eq!(
            parent_location("b", "gs://b/dir/").as_deref(),
            Some("gs://b/")
        );
        // バケット直下からはこれ以上上がれない。
        assert_eq!(parent_location("b", "gs://b/"), None);
    }

    /// 行数の 3 桁区切り表示。
    #[test]
    fn fmt_count_groups_thousands() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(7), "7");
        assert_eq!(fmt_count(42), "42");
        assert_eq!(fmt_count(1000), "1,000");
        assert_eq!(fmt_count(12_345), "12,345");
        assert_eq!(fmt_count(1_234_567), "1,234,567");
    }

    /// 残り時間の表記（h/m/s）。
    #[test]
    fn fmt_duration_units() {
        assert_eq!(fmt_duration(5.0), "5s");
        assert_eq!(fmt_duration(65.0), "1m5s");
        assert_eq!(fmt_duration(3661.0), "1h1m");
        assert_eq!(fmt_duration(-3.0), "0s");
    }

    /// 進捗割合: 全体不明/0 は None、それ以外は 0..1 にクランプ。
    #[test]
    fn progress_fraction_cases() {
        assert_eq!(progress_fraction(50, None), None);
        assert_eq!(progress_fraction(50, Some(0)), None);
        assert_eq!(progress_fraction(0, Some(100)), Some(0.0));
        assert_eq!(progress_fraction(50, Some(100)), Some(0.5));
        assert_eq!(progress_fraction(100, Some(100)), Some(1.0));
        // 読み出しが全体を超えても 1.0 にクランプ。
        assert_eq!(progress_fraction(150, Some(100)), Some(1.0));
    }

    /// 速度・ETA: 経過が短い/0 行は空、それ以外は rows/s と残り時間。
    #[test]
    fn rate_eta_cases() {
        assert_eq!(rate_eta(0.2, 100, Some(0.5)), ""); // 経過が短すぎ
        assert_eq!(rate_eta(2.0, 0, Some(0.5)), ""); // まだ 0 行
        // 100 行/2s = 50 行/s、50% なら残りは経過と同じ ~2s。
        assert_eq!(rate_eta(2.0, 100, Some(0.5)), "  ·  50 行/s  ·  残り ~2s");
        // 1000 行/s 以上は k 表記、frac 不明なら ETA 無し。
        assert_eq!(rate_eta(1.0, 5000, None), "  ·  5k 行/s");
    }

    /// 未割当 PK の検出。
    #[test]
    fn unmapped_pks_detects() {
        let cols = vec![
            query::Column { name: "Id".into(), ty: "INT64".into(), pk: true },
            query::Column { name: "Name".into(), ty: "STRING(MAX)".into(), pk: false },
        ];
        // PK 未割当。
        assert_eq!(
            unmapped_pks(&cols, &[None, Some(0)]),
            vec!["Id".to_string()]
        );
        // 全割当なら空。
        assert!(unmapped_pks(&cols, &[Some(1), Some(0)]).is_empty());
    }

    /// フォルダ一括: *.csv だけ抽出して URI と表示名を作る。
    #[test]
    fn csv_object_uris_filters() {
        let objs = vec![
            "dir/a.csv".to_string(),
            "dir/readme.txt".to_string(),
            "dir/B.CSV".to_string(),
        ];
        let got = csv_object_uris("bkt", &objs);
        assert_eq!(
            got,
            vec![
                ("gs://bkt/dir/a.csv".to_string(), "a.csv".to_string()),
                ("gs://bkt/dir/B.CSV".to_string(), "B.CSV".to_string()),
            ]
        );
    }

    /// 証跡レポート CSV: ヘッダ＋各ジョブの行（件数・方式・状態）。
    #[test]
    fn report_csv_rows() {
        fn job(table: &str, mode: query::ImportMode, written: usize, total: usize) -> ImportJob {
            ImportJob {
                req: query::ImportRequest {
                    table: table.into(),
                    columns: vec![],
                    source: query::ImportSource::File("/tmp/x.csv".into()),
                    has_header: true,
                    mode,
                    empty_as_null: true,
                    fresh: false,
                    encoding: query::Encoding::Utf8,
                    delimiter: b',',
                    skip_bad_rows: false,
                    dry_run: false,
                    null_token: None,
                    cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
                source_name: "x.csv".into(),
                sent: true,
                status: JobStatus::Done,
                started: None,
                progress: None,
                result: None,
                outcome: Some(query::ImportOutcome {
                    written,
                    total,
                    elapsed_ms: 10,
                    ..Default::default()
                }),
            }
        }
        let jobs = vec![
            job("A", query::ImportMode::Insert, 100, 100),
            job("B", query::ImportMode::InsertOrUpdate, 50, 60),
        ];
        let csv = report_csv(&jobs);
        let lines: Vec<&str> = csv.lines().collect();
        assert!(lines[0].starts_with("table,mode,status,written,total"));
        assert_eq!(lines[1], "A,insert,完了,100,100,0,0,10,,,/tmp/x.csv");
        assert_eq!(lines[2], "B,insert_or_update,完了,50,60,0,0,10,,,/tmp/x.csv");
    }

    /// 結果メッセージ: 成功/ドライラン/中断/部分適用/失敗 と注記。
    #[test]
    fn format_import_result_branches() {
        let base = query::ImportOutcome {
            table: "T".into(),
            total: 100,
            elapsed_ms: 50,
            ..Default::default()
        };
        // 成功（注記なし）。
        let ok = query::ImportOutcome { written: 100, ..base.clone() };
        assert_eq!(format_import_result(&ok), "100 / 100 行を取り込みました（50 ms）");
        // 再開＋スキップ＋リジェクトの注記付き。
        let noted = query::ImportOutcome {
            written: 90,
            resumed: 30,
            skipped: 5,
            reject_path: Some("/tmp/r.csv".into()),
            ..base.clone()
        };
        let m = format_import_result(&noted);
        assert!(m.contains("前回完了分 30 行をスキップ"));
        assert!(m.contains("不正行 5 行をスキップ"));
        assert!(m.contains("/tmp/r.csv"));
        // ドライラン。
        let dry = query::ImportOutcome { written: 80, skipped: 20, dry_run: true, ..base.clone() };
        assert!(format_import_result(&dry).starts_with("検証完了: 100 行中 80 行が書き込み可能"));
        // 中断。
        let cancelled = query::ImportOutcome { written: 40, cancelled: true, ..base.clone() };
        assert!(format_import_result(&cancelled).starts_with("中断（40 行まで取込）"));
        // 部分適用（書込後にエラー）。
        let partial = query::ImportOutcome {
            written: 60,
            error: Some("boom".into()),
            ..base.clone()
        };
        assert!(format_import_result(&partial).contains("60 件まで書き込み後にエラー: boom"));
        // 1 行も書けずに失敗。
        let failed = query::ImportOutcome { written: 0, error: Some("nope".into()), ..base };
        assert!(format_import_result(&failed).starts_with("失敗: nope"));
    }
}
