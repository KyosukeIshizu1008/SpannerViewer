use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use eframe::egui;
use egui_plot::{Legend, Line, Plot, PlotPoints};
use tokio::sync::mpsc::UnboundedSender;

use crate::csvview;
use crate::k8s;
use crate::monitoring::Sample;
use crate::query::{self, EdgeKind, QueryOutcome, SchemaGraph, TableNode, Target};

// ── カラーパレット（モダンダーク） ──
// VS Code Dark+ 配色
const ACCENT: egui::Color32 = egui::Color32::from_rgb(0, 122, 204); // VS Code blue #007acc（クローム用）
// 図/グラフの配色（見やすい明るめの色に戻す）
const DIAGRAM_ACCENT: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // 明るい青（図のヘッダ/辺）
const CPU_COLOR: egui::Color32 = egui::Color32::from_rgb(251, 146, 60); // amber/orange
const STORAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const TEXT: egui::Color32 = egui::Color32::from_rgb(204, 204, 204); // #cccccc
const MUTED: egui::Color32 = egui::Color32::from_rgb(133, 133, 133); // #858585

/// 背景ワーカーへの送信失敗時に表示するメッセージ。
const WORKER_GONE: &str = "バックグラウンド処理が停止しています。アプリを再起動してください。";

#[derive(PartialEq, Eq, Clone, Copy)]
enum Section {
    Spanner,
    Kube,
    Csv,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum View {
    Monitor,
    Data,
    Schema,
    Import,
    Verify,
    Plan,
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
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum JobStatus {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

/// インポートキューの 1 ジョブ。別テーブルは並列・同一テーブルは直列で実行する。
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
    /// 一括投入の対象フォルダ（この一覧応答だけを取り込む。無関係な
    /// ブラウズ一覧を誤って一括投入しないよう照合に使う）。
    folder: String,
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
        self.mapping = auto_mapping(&self.table_columns, &self.csv_headers, self.has_header, ncols);
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

/// GCS ダイアログの用途（取得成功後の引き継ぎ先）。
#[derive(Clone, Copy, PartialEq, Eq)]
enum GcsPurpose {
    Import,
    Verify,
}

/// GCS から CSV を取り込むための入力ダイアログ。
/// URI を入力 → 背景で取得 → 成功したら ImportDialog（マッピング画面）へ引き継ぐ。
struct GcsDialog {
    /// 取得成功後の引き継ぎ先（インポート / 照合）。
    purpose: GcsPurpose,
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

/// CSV↔DB 照合タブのマッピング状態。テーブルと CSV を選ぶと作られる。
/// 実データは溜めず、実行時に `source` からストリーミングして突合する。
struct VerifyState {
    /// 照合対象テーブル。
    table: String,
    /// テーブルのカラム（名前・型・PK）。比較列とキーの算出に使う。
    table_columns: Vec<query::Column>,
    /// 取得元（ファイル/GCS）。実行時にここからストリーミングする。
    source: query::ImportSource,
    /// 表示用のソース名。
    source_name: String,
    /// プレビュー用の生バイト（文字コード/区切り変更時に再パース）。
    preview_bytes: Vec<u8>,
    /// マッピング表示用のプレビュー行（先頭の数行のみ）。
    records: Vec<Vec<String>>,
    /// CSV 側の列見出し。
    csv_headers: Vec<String>,
    encoding: query::Encoding,
    delimiter: u8,
    has_header: bool,
    /// 空欄を NULL とみなして比較するか。
    empty_as_null: bool,
    /// 数値の表記ゆれを無視して突合するか（"005"="5", "5.0"="5"）。
    numeric_match: bool,
    /// NULL とみなす文字列（空なら無効）。
    null_token: String,
    /// テーブル各カラムに割り当てる CSV 列インデックス（None=比較から除外）。
    mapping: Vec<Option<usize>>,
    note: Option<String>,
    config_msg: Option<String>,
}

impl VerifyState {
    /// has_header に応じて CSV 見出しと自動マッピングを作り直す（ImportDialog と同じ規則）。
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
        self.mapping = auto_mapping(&self.table_columns, &self.csv_headers, self.has_header, ncols);
    }

    /// 文字コード/区切りを変えたとき、プレビューを生バイトから再パースする。
    fn reparse_preview(&mut self) {
        self.records =
            query::parse_preview(&self.preview_bytes, self.encoding, self.delimiter, PREVIEW_ROWS + 1);
        self.recompute();
    }
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
    pub verify_req_tx: UnboundedSender<query::VerifyRequest>,
    pub verify_res_rx: Receiver<query::VerifyProgress>,
    pub schema_rx: Receiver<SchemaGraph>,
    pub plan_rx: Receiver<query::PlanOutcome>,
    pub kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    pub kube_topo_req_tx: UnboundedSender<Option<String>>,
    pub kube_topo_rx: Receiver<k8s::ArchGraph>,
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
    // 実行計画
    plan_rx: Receiver<query::PlanOutcome>,
    plan_result: Option<query::PlanOutcome>,
    plan_pending: bool,
    // CSV インポート
    import_req_tx: UnboundedSender<query::ImportRequest>,
    import_res_rx: Receiver<query::ImportProgress>,
    import_dialog: Option<ImportDialog>,
    import_pending: bool,
    /// インポートキュー（別テーブルは並列・同一テーブルは直列で処理）。
    import_jobs: Vec<ImportJob>,
    /// 次に発番するジョブ id（進捗/完了の紐付けに使う。1 から増やす）。
    import_next_id: u64,
    /// 証跡レポートの保存先（スクショ受信待ち。受信したら PNG を書いて Finder で開く）。
    pending_report_dir: Option<std::path::PathBuf>,
    /// スクショ応答を待つ残りフレーム数。0 で諦めて pending を解放する
    /// （応答が来ないまま後の無関係なスクショを誤って保存しないため）。
    pending_report_wait: u32,
    /// GCS フォルダ一括投入の保留（List 応答待ち）。
    pending_bulk: Option<BulkSpec>,
    /// インポートタブで選択中の取り込み先テーブル名。
    import_table_pick: String,
    // GCS インポート（CSV 取得 → ImportDialog へ）
    gcs_req_tx: UnboundedSender<query::GcsRequest>,
    gcs_res_rx: Receiver<query::GcsResponse>,
    gcs_dialog: Option<GcsDialog>,
    gcs_pending: bool,

    // CSV↔DB 照合（突合）
    verify_req_tx: UnboundedSender<query::VerifyRequest>,
    verify_res_rx: Receiver<query::VerifyProgress>,
    /// マッピング状態（テーブル + CSV を選ぶと作られる）。
    verify: Option<VerifyState>,
    /// 照合タブで選択中のテーブル名。
    verify_table_pick: String,
    /// 実行中か（結果待ち）。
    verify_running: bool,
    /// 実行中の進捗（フェーズ・DB件数・CSV件数）。
    verify_progress: Option<(&'static str, usize, usize)>,
    /// 直近の照合結果。
    verify_result: Option<query::VerifyOutcome>,
    /// 結果一覧の種別フィルタ（None=すべて）。
    verify_filter: Option<query::VerifyKind>,
    /// 中断フラグ（実行中ジョブと共有）。
    verify_cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
    // SQL 補完（テーブル/カラム名 + キーワード）
    sql_sug_open: bool,               // 補完ポップアップ表示中か
    sql_sug_index: usize,             // 選択中の候補
    sql_suggestions: Vec<String>,     // 現在の候補（前フレーム算出。Tab確定にも使う）
    sql_word_range: (usize, usize),   // 補完対象の単語のバイト範囲 (start, end)
    sql_set_cursor: Option<usize>,    // 次フレームで設定するカーソル位置（バイト）

    // スキーマ図のパン/ズーム・編集状態
    diagram_pan: egui::Vec2,
    diagram_zoom: f32,
    node_positions: HashMap<String, egui::Pos2>,
    selected: Option<String>,
    copy_note: Option<String>,
    /// 図のテーブル名クリックで開く CREATE 文ウィンドウ（テーブル名, DDL）。
    ddl_view: Option<(String, String)>,

    // Kubernetes
    kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    kube_metrics: Option<k8s::KubeMetrics>,
    kube_req_tx: UnboundedSender<Option<String>>,
    kube_graph_rx: Receiver<k8s::ArchGraph>,
    kube_graph: Option<k8s::ArchGraph>,
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
    auth_checking: std::sync::Arc<std::sync::atomic::AtomicBool>, // 確認スレッド実行中
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
    // 「この親に対して一覧を取得済み」を表すセンチネル。空一覧でも取得済みなら
    // 再取得ループに入らないようにする（instance/DB が 0 件のプロジェクト対策）。
    instances_loaded_for: Option<String>,
    databases_loaded_for: Option<(String, String)>,
    // プロジェクトが大量にある組織向けの絞り込み入力（兼: 一覧に出ない ID の手動指定）。
    pick_project_filter: String,
    // 列挙権限が無く一覧に出ないとき用の手動入力（instance / database）。
    pick_instance_manual: String,
    pick_database_manual: String,

    // CSV ビューア（複数タブ・巨大ファイル: mmap + 行仮想化）
    csv_tabs: Vec<CsvTab>,
    csv_active: usize,
    csv_gcs_open: bool,   // GCS URI 入力ウィンドウの表示
    csv_gcs_uri: String,

    section: Section,
    view: View,
}

/// project/instance/database カスケード選択の背景取得結果。
/// Instances/Databases は「どの親に対する結果か」をタグ付けし、取得中に選択が
/// 切り替わった場合に古い結果を誤って別の親に紐付けないようにする。
enum PickMsg {
    Projects(Result<Vec<String>, String>),
    Instances {
        project: String,
        result: Result<Vec<String>, String>,
    },
    Databases {
        project: String,
        instance: String,
        result: Result<Vec<String>, String>,
    },
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

        // 前回の未完インポートジョブを「中断」状態で復元（再キューで続きから再開可）。
        let (restored_jobs, restored_next_id) = restore_import_jobs();

        Self {
            sample_rx: ch.sample_rx,
            samples: VecDeque::new(),
            last_error: None,
            max_points: 480,
            req_tx: ch.req_tx,
            res_rx: ch.res_rx,
            schema_rx: ch.schema_rx,
            plan_rx: ch.plan_rx,
            plan_result: None,
            plan_pending: false,
            import_req_tx: ch.import_req_tx,
            import_res_rx: ch.import_res_rx,
            import_dialog: None,
            import_pending: false,
            import_jobs: restored_jobs,
            import_next_id: restored_next_id,
            pending_report_dir: None,
            pending_report_wait: 0,
            pending_bulk: None,
            import_table_pick: String::new(),
            gcs_req_tx: ch.gcs_req_tx,
            gcs_res_rx: ch.gcs_res_rx,
            gcs_dialog: None,
            gcs_pending: false,
            verify_req_tx: ch.verify_req_tx,
            verify_res_rx: ch.verify_res_rx,
            verify: None,
            verify_table_pick: String::new(),
            verify_running: false,
            verify_progress: None,
            verify_result: None,
            verify_filter: None,
            verify_cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sql: "SELECT * FROM LoadTest LIMIT 100".to_string(),
            data_result: None,
            data_pending: false,
            schema_graph: None,
            schema_pending: false,
            data_sort: None,
            data_search: String::new(),
            data_history: Vec::new(),
            tree_expanded: HashSet::new(),
            sql_sug_open: false,
            sql_sug_index: 0,
            sql_suggestions: Vec::new(),
            sql_word_range: (0, 0),
            sql_set_cursor: None,
            diagram_pan: egui::vec2(40.0, 40.0),
            diagram_zoom: 1.0,
            node_positions: load_layout(),
            selected: None,
            copy_note: None,
            ddl_view: None,
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
            auth_checking: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
            instances_loaded_for: None,
            databases_loaded_for: None,
            csv_tabs: Vec::new(),
            csv_active: 0,
            csv_gcs_open: false,
            csv_gcs_uri: String::new(),

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
            // Finder 起動だと PATH が最小で gcloud 自身が python 等を見つけられない。
            // よくある bin を PATH に補ってから実行する（ターミナル起動相当にする）。
            let out = std::process::Command::new(gcloud_bin())
                .args(["auth", "application-default", "login"])
                .env("PATH", k8s::augmented_path())
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
                // ログイン成功で「後で」の抑止を解除。後でトークン失効等で
                // 再び未ログインになったら、また案内できるようにする。
                self.login_dismissed = false;
            } else if !self.login_dismissed {
                self.login_dialog = true;
            }
        }
        self.login_window(ctx);
    }

    /// ADC（ログイン状態）を背景で確認する。結果は auth_ok に入る。
    fn start_adc_check(&self, ctx: &egui::Context) {
        use std::sync::atomic::Ordering;
        // 多重起動を防ぐ（実行中なら何もしない）。
        if self
            .auth_checking
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        *self.auth_ok.lock().unwrap() = None; // 確認中
        let slot = self.auth_ok.clone();
        let checking = self.auth_checking.clone();
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let ok = run_blocking(query::check_adc()).is_ok();
            *slot.lock().unwrap() = Some(ok);
            checking.store(false, Ordering::Release);
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
            let result = run_blocking(query::list_instances(&project));
            PickMsg::Instances { project, result }
        });
    }

    fn load_databases(&self, ctx: &egui::Context, project: String, instance: String) {
        self.spawn_pick(ctx, move || {
            let result = run_blocking(query::list_databases(&project, &instance));
            PickMsg::Databases {
                project,
                instance,
                result,
            }
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
            PickMsg::Instances { project, result } => match result {
                Ok(v) => {
                    self.pick_error = None;
                    // 取得中に別プロジェクトへ切り替わっていたら破棄（次フレームで再取得される）。
                    if project == self.pick_project {
                        self.pick_instances = v;
                        self.instances_loaded_for = Some(project);
                    }
                }
                Err(e) => self.pick_error = Some(e),
            },
            PickMsg::Databases {
                project,
                instance,
                result,
            } => match result {
                Ok(v) => {
                    self.pick_error = None;
                    if project == self.pick_project && instance == self.pick_instance {
                        self.pick_databases = v;
                        self.databases_loaded_for = Some((project, instance));
                    }
                }
                Err(e) => self.pick_error = Some(e),
            },
            PickMsg::Projects(Err(e)) => {
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
        while let Ok(p) = self.plan_rx.try_recv() {
            self.plan_pending = false;
            self.plan_result = Some(p);
        }
        while let Ok(ev) = self.import_res_rx.try_recv() {
            // ジョブは並列実行されうるので、進捗/完了は id で正しいジョブ行に紐付ける。
            match ev {
                query::ImportProgress::Progress {
                    id,
                    written,
                    bytes_done,
                    bytes_total,
                } => {
                    let frac = progress_fraction(bytes_done, bytes_total);
                    if let Some(j) = self.import_jobs.iter_mut().find(|j| j.req.id == id) {
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
                    if let Some(j) = self.import_jobs.iter_mut().find(|j| j.req.id == out.id) {
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
                    // 空いた枠で次の待機ジョブを動かす。
                    self.pump_import_queue();
                    // 完了/失敗/中断で状態が変わったので保存（Done は保存対象外になる）。
                    save_import_jobs(&self.import_jobs);
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
                            match d.purpose {
                                GcsPurpose::Import => self.build_import_dialog(
                                    d.target,
                                    query::ImportSource::Gcs(out.uri.clone()),
                                    out.uri,
                                    bytes,
                                ),
                                GcsPurpose::Verify => self.build_verify_state(
                                    d.target,
                                    query::ImportSource::Gcs(out.uri.clone()),
                                    out.uri,
                                    bytes,
                                ),
                            }
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
                    // この一覧が一括投入の対象フォルダのものか照合する（無関係な
                    // ブラウズ一覧を誤って一括投入しないため）。
                    let listed_loc = format!("gs://{}/{}", out.bucket, out.prefix);
                    let is_bulk = self
                        .pending_bulk
                        .as_ref()
                        .is_some_and(|b| b.folder == listed_loc);
                    if let Some(bulk) = if is_bulk { self.pending_bulk.take() } else { None } {
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
        while let Ok(ev) = self.verify_res_rx.try_recv() {
            match ev {
                query::VerifyProgress::Progress {
                    phase,
                    db_rows,
                    csv_rows,
                } => {
                    if self.verify_running {
                        self.verify_progress = Some((phase, db_rows, csv_rows));
                    }
                }
                query::VerifyProgress::Done(out) => {
                    self.verify_running = false;
                    self.verify_progress = None;
                    self.verify_filter = None;
                    self.copy_note = Some(if let Some(e) = &out.error {
                        format!("照合エラー: {e}")
                    } else {
                        format!(
                            "照合完了: 一致 {} / 値差異 {} / CSVのみ {} / DBのみ {}",
                            out.matched, out.value_mismatch, out.csv_only, out.db_only
                        )
                    });
                    self.verify_result = Some(out);
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
        let req = k8s::LogReq::Follow {
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

    /// ログ追従を停止する（kubectl logs -f プロセスを終わらせ、リークを防ぐ）。
    fn stop_logs(&mut self) {
        if self.kube_log_following {
            let _ = self.kube_log_req_tx.send(k8s::LogReq::Stop);
            self.kube_log_following = false;
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

    /// 現在の SQL の実行計画を背景で取得する（PLAN モード・実行はしない）。
    fn run_plan(&mut self) {
        let sql = self.sql.trim().to_string();
        if sql.is_empty() {
            return;
        }
        if self.req_tx.send((Target::Plan, sql)).is_ok() {
            self.plan_pending = true;
            self.plan_result = None;
        } else {
            self.plan_pending = false;
            self.plan_result = Some(query::PlanOutcome {
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
    /// 未描画領域がクリアカラー（黒）で出ないよう、エディタ背景色で塗る。
    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        let [r, g, b, _] = BASE.to_normalized_gamma_f32();
        [r, g, b, 1.0]
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx_owned = ui.ctx().clone();
        let ctx = &ctx_owned;
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
        egui::Panel::left("activity")
            .exact_size(44.0)
            .resizable(false)
            .frame(
                egui::Frame::NONE
                    .fill(ACTIVITY_BG)
                    .stroke(egui::Stroke::new(1.0, BORDER)),
            )
            .show(ui, |ui| {
                self.activity_bar(ui);
            });

        // VS Code 風の下部ステータスバー（青）。
        egui::Panel::bottom("statusbar")
            .exact_size(24.0)
            .resizable(false)
            .frame(
                egui::Frame::NONE
                    .fill(STATUS_BG)
                    .inner_margin(egui::Margin::symmetric(10, 3)),
            )
            .show(ui, |ui| {
                let white = egui::Color32::WHITE;
                ui.horizontal(|ui| {
                    let sec = match self.section {
                        Section::Spanner => "Spanner",
                        Section::Kube => "Kubernetes",
                        Section::Csv => "CSV ビューア",
                    };
                    ui.label(egui::RichText::new(sec).color(white).small());
                    ui.label(egui::RichText::new("·").color(white).small());
                    let info = match self.section {
                        Section::Csv => self
                            .csv_tabs
                            .get(self.csv_active)
                            .map(|t| t.title.clone())
                            .unwrap_or_else(|| "ファイル未選択".into()),
                        _ => self.conn_info.clone(),
                    };
                    ui.label(egui::RichText::new(info).color(white).small());
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if let Some(note) = &self.copy_note {
                            ui.label(egui::RichText::new(note).color(white).small());
                        }
                    });
                });
            });

        // ビュー切替タブ（セクションごとに内容が変わる）
        // 接続切替の操作はクロージャ内で借用中なので、解放後に適用する。
        // トップのカスケード選択（借用解消後に適用）。
        let mut tb_load_instances: Option<String> = None;
        let mut tb_load_databases: Option<(String, String)> = None;
        let mut tb_apply = false;
        egui::Panel::top("tabs").show(ui, |ui| {
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
                        ui.add_space(gap);
                        if tab(ui, self.view == View::Verify, "照合") {
                            self.view = View::Verify;
                        }
                        ui.add_space(gap);
                        if tab(ui, self.view == View::Plan, "実行計画") {
                            self.view = View::Plan;
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
                                    let db_key =
                                        (self.pick_project.clone(), self.pick_instance.clone());
                                    if self.pick_databases.is_empty()
                                        && !busy
                                        && !self.pick_project.is_empty()
                                        && !self.pick_instance.is_empty()
                                        && self.databases_loaded_for.as_ref() != Some(&db_key)
                                    {
                                        tb_load_databases = Some(db_key);
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
                                    if self.pick_instances.is_empty()
                                        && !busy
                                        && !proj.is_empty()
                                        && self.instances_loaded_for.as_deref() != Some(proj.as_str())
                                    {
                                        tb_load_instances = Some(proj.clone());
                                        ui.label(
                                            egui::RichText::new("取得中…").color(MUTED).small(),
                                        );
                                    }
                                    for inst in self.pick_instances.clone() {
                                        if ui
                                            .selectable_label(self.pick_instance == inst, &inst)
                                            .clicked()
                                        {
                                            cascade::select_instance(
                                                &mut self.pick_instance,
                                                &mut self.pick_database,
                                                &mut self.pick_databases,
                                                &inst,
                                            );
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
                            ui.label(egui::RichText::new(proj_text).color(MUTED))
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
                            let resp = ui.label(egui::RichText::new(label).color(color));
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
                    Section::Csv => {
                        ui.label(egui::RichText::new("CSV ビューア").strong());
                        ui.label(
                            egui::RichText::new("巨大ファイル対応（mmap + 行仮想化）")
                                .color(MUTED)
                                .small(),
                        );
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if let Some(t) = self.csv_tabs.get(self.csv_active) {
                                ui.label(
                                    egui::RichText::new(format!("{} 個のタブ", self.csv_tabs.len()))
                                        .color(MUTED)
                                        .small(),
                                );
                                ui.label(egui::RichText::new(&t.title).color(MUTED).small());
                            }
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
                View::Schema => self.schema_view(ui),
                View::Monitor => self.monitor_view(ui),
                View::Data => self.data_view(ui),
                View::Import => self.import_view(ui),
                View::Verify => self.verify_view(ui),
                View::Plan => self.plan_view(ui),
            },
            Section::Kube => match self.kube_view {
                KubeView::Monitor => self.kube_monitor_view(ui),
                KubeView::Resources => self.kube_resource_view(ui),
                KubeView::Diagram => self.kube_diagram_view(ui),
                KubeView::Events => self.kube_events_view(ui),
            },
            Section::Csv => self.csv_view(ui),
        }

        self.settings_window(ctx);
        self.ddl_window(ctx);
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

/// CSV ビューアの1ファイル分の状態（VS Code のタブ相当）。読み込み/絞り込みは
/// タブごとに独立した背景タスクで行う。
/// GCS ストリーミングプレビューの蓄積バッファ（生バイト行を順次ためる）。
#[derive(Default)]
struct PreviewBuf {
    lines: Vec<Vec<u8>>, // CRLF 除去済みの生バイト行
    bytes_read: u64,
    complete: bool, // 末尾まで読み切った
    capped: bool,   // 上限に達して打ち切った
    error: Option<String>,
}

struct CsvTab {
    path: std::path::PathBuf,
    title: String,
    index: Option<std::sync::Arc<csvview::CsvIndex>>,
    result: std::sync::Arc<std::sync::Mutex<Option<Result<csvview::CsvIndex, String>>>>,
    progress: std::sync::Arc<std::sync::atomic::AtomicU64>,
    total_bytes: u64,
    loading: bool,
    err: Option<String>,
    // GCS ストリーミングプレビュー（ローカルに落とさず逐次表示）。Some ならプレビュー。
    preview: Option<std::sync::Arc<std::sync::Mutex<PreviewBuf>>>,
    stream_cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    first_row: u64,
    col_off: f32,
    delim: u8,
    /// 索引/プレビューを作ったときの区切り文字。delim を変えたら作り直す判定に使う。
    built_delim: u8,
    /// GCS から開いた場合の元 URI（区切り変更時の再ストリーミング用）。
    gcs_uri: Option<String>,
    encoding: query::Encoding,
    has_header: bool,
    goto: String,
    filter: String,
    filter_col: Option<usize>,
    matches: Option<std::sync::Arc<Vec<u64>>>,
    filtering: bool,
    filter_progress: std::sync::Arc<std::sync::atomic::AtomicU64>,
    filter_cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    filter_result: std::sync::Arc<std::sync::Mutex<Option<Vec<u64>>>>,
    /// 各列の表示幅（内容に合わせて算出。空なら次の描画で再計算）。
    col_widths: Vec<f32>,
    /// クリックで選択中のセル（データ行インデックス, 列）。コピー対象のハイライト用。
    selected: Option<(u64, usize)>,
    /// 直近にコピーしたセル値（ツールバーに表示。確認用）。
    copied_note: Option<String>,
    /// 列診断の結果（列ズレ/桁落ち/科学表記の判定）。ボタンで生成。
    diag: Option<String>,
}

impl CsvTab {
    fn new(path: std::path::PathBuf) -> Self {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use std::sync::{Arc, Mutex};
        let title = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "CSV".into());
        let total_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        Self {
            path,
            title,
            index: None,
            result: Arc::new(Mutex::new(None)),
            progress: Arc::new(AtomicU64::new(0)),
            total_bytes,
            loading: false,
            err: None,
            preview: None,
            stream_cancel: Arc::new(AtomicBool::new(false)),
            first_row: 0,
            col_off: 0.0,
            delim: b',',
            built_delim: b',',
            gcs_uri: None,
            encoding: query::Encoding::Utf8,
            has_header: true,
            goto: String::new(),
            filter: String::new(),
            filter_col: None,
            matches: None,
            filtering: false,
            filter_progress: Arc::new(AtomicU64::new(0)),
            filter_cancel: Arc::new(AtomicBool::new(false)),
            filter_result: Arc::new(Mutex::new(None)),
            col_widths: Vec::new(),
            selected: None,
            copied_note: None,
            diag: None,
        }
    }

    /// 読み込んだ CSV を解析し、「列ズレ（読み方/区切りの問題）」か
    /// 「データ自体の桁落ち/科学表記」かを判定する診断レポートを作る。
    /// 先頭から最大 sample 行を走査する（巨大ファイルでも軽い）。
    fn diagnose(&self) -> String {
        let total = self.total_lines();
        let header_off: u64 = if self.has_header && total > 0 { 1 } else { 0 };
        let data_rows = total.saturating_sub(header_off);
        if data_rows == 0 {
            return "データ行がありません。".into();
        }
        let sample = data_rows.min(50_000);
        let rows = self.visible_rows(0, sample);
        if rows.is_empty() {
            return "行を読めませんでした。".into();
        }
        let ncols = self.ncols().max(1);
        let headers = self.header_cells();

        // 列数ヒストグラム（バラバラ = 列ズレ）。
        let mut field_hist: std::collections::BTreeMap<usize, usize> = std::collections::BTreeMap::new();
        for r in &rows {
            *field_hist.entry(r.len()).or_default() += 1;
        }

        // 各列の解析: 長さレンジ・科学表記・小数・非数値の出現数。
        let mut min_len = vec![usize::MAX; ncols];
        let mut max_len = vec![0usize; ncols];
        let mut sci = vec![0usize; ncols];
        let mut dec = vec![0usize; ncols];
        let mut nonnum = vec![0usize; ncols];
        let mut nonempty = vec![0usize; ncols];
        for r in &rows {
            for c in 0..ncols {
                let Some(v) = r.get(c) else { continue };
                let v = v.trim();
                if v.is_empty() {
                    continue;
                }
                nonempty[c] += 1;
                let len = v.chars().count();
                min_len[c] = min_len[c].min(len);
                max_len[c] = max_len[c].max(len);
                let digits = v.bytes().filter(|b| b.is_ascii_digit()).count();
                let has_e = v.bytes().any(|b| b == b'e' || b == b'E');
                if has_e && digits > 0 && v.bytes().all(|b| {
                    b.is_ascii_digit() || matches!(b, b'e' | b'E' | b'+' | b'-' | b'.')
                }) {
                    sci[c] += 1;
                }
                if v.contains('.')
                    && v.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.'))
                {
                    dec[c] += 1;
                }
                if !v.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-')) {
                    nonnum[c] += 1;
                }
            }
        }

        let mut s = String::new();
        s.push_str(&format!("診断（先頭 {} 行を解析）\n", fmt_count(rows.len())));
        s.push_str(&format!("区切り='{}' / 文字コード={}\n",
            match self.delim { b'\t' => "Tab".into(), d => (d as char).to_string() },
            match self.encoding { query::Encoding::ShiftJis => "Shift-JIS", _ => "UTF-8" }));

        // 列ズレ判定。
        if field_hist.len() > 1 {
            s.push_str("⚠ 列数がバラバラです（=列ズレ・読み方/区切りの問題の可能性大）:\n");
            for (cols, cnt) in &field_hist {
                s.push_str(&format!("   {cols}列 … {} 行\n", fmt_count(*cnt)));
            }
            s.push_str("→ 区切り文字が正しいか確認。引用符で囲まれていない区切り/改行が\n   データ内にあると列がズレます。\n");
        } else if let Some((cols, _)) = field_hist.iter().next() {
            s.push_str(&format!("列数は一定（{cols}列）= 列ズレなし。\n"));
        }

        // 各列のフラグ。
        s.push_str("\n列ごとの値:\n");
        for c in 0..ncols {
            let name = headers.get(c).cloned().unwrap_or_else(|| format!("列{}", c + 1));
            if nonempty[c] == 0 {
                s.push_str(&format!("  [{c}] {name}: （空）\n"));
                continue;
            }
            let (mn, mx) = (min_len[c], max_len[c]);
            let mut flags = Vec::new();
            if mn != mx {
                flags.push(format!("桁数 {mn}〜{mx}（不揃い）"));
            } else {
                flags.push(format!("桁数 {mn}"));
            }
            if sci[c] > 0 {
                flags.push(format!("⚠科学表記 {}件", fmt_count(sci[c])));
            }
            if dec[c] > 0 {
                flags.push(format!("小数 {}件", fmt_count(dec[c])));
            }
            if nonnum[c] > 0 && nonnum[c] < nonempty[c] {
                flags.push("数値と非数値が混在".to_string());
            }
            s.push_str(&format!("  [{c}] {name}: {}\n", flags.join(" / ")));
        }
        s.push_str("\n判定の目安:\n");
        s.push_str("・科学表記あり / 桁数が大きく不揃い → 表計算でIDが桁落ち（データの問題・要再エクスポート）\n");
        s.push_str("・列数バラバラ → 区切り/引用符の問題（区切りを直すと解決することが多い）\n");
        s.push_str("・列数一定で桁数も妥当 → 読み込みは正常\n");
        s
    }

    /// 各列の表示幅を内容（ヘッダ + 先頭付近のデータ行）から算出する。
    /// 100GB でも先頭サンプルだけ見るので軽い。半角=1/全角=2 で文字幅を概算し、
    /// 列ごとに [MIN, MAX] でクランプする。
    fn compute_col_widths(&self) -> Vec<f32> {
        const CHAR_PX: f32 = 7.1; // 12px プロポーショナルの 1 単位ぶんの目安
        const PAD: f32 = 16.0;
        const MIN_W: f32 = 54.0;
        const MAX_W: f32 = 460.0;
        let ncols = self.ncols();
        if ncols == 0 {
            return Vec::new();
        }
        let units = |s: &str| -> usize {
            s.chars().map(|c| if c.is_ascii() { 1 } else { 2 }).sum()
        };
        let mut max_units = vec![0usize; ncols];
        // ヘッダはやや太字なので +1 単位みておく。
        for (i, h) in self.header_cells().iter().enumerate().take(ncols) {
            max_units[i] = max_units[i].max(units(h) + 1);
        }
        // 先頭付近のデータ行をサンプル（フィルタ中は一致行の先頭）。
        for row in self.visible_rows(0, 200) {
            for (i, cell) in row.iter().enumerate().take(ncols) {
                max_units[i] = max_units[i].max(units(cell));
            }
        }
        max_units
            .into_iter()
            .map(|u| (u as f32 * CHAR_PX + PAD).clamp(MIN_W, MAX_W))
            .collect()
    }

    /// 列幅キャッシュを無効化する（フィルタ変更・再読込時）。
    fn invalidate_col_widths(&mut self) {
        self.col_widths.clear();
    }

    /// 指定列を「表示中の最長セル」に合わせた幅にする（上限なし＝全部見える）。
    /// 境界のダブルクリックで使う。
    fn fit_col_width(&self, ci: usize) -> f32 {
        const CHAR_PX: f32 = 7.1;
        const PAD: f32 = 18.0;
        let units = |s: &str| -> usize {
            s.chars().map(|c| if c.is_ascii() { 1 } else { 2 }).sum()
        };
        let mut max_u = self.header_cells().get(ci).map(|h| units(h) + 1).unwrap_or(2);
        for row in self.visible_rows(0, 500) {
            if let Some(c) = row.get(ci) {
                max_u = max_u.max(units(c));
            }
        }
        (max_u as f32 * CHAR_PX + PAD).max(40.0)
    }

    /// 背景で索引を作る（巨大ファイルでも UI を止めない）。
    fn start_load(&mut self, ctx: &egui::Context) {
        use std::sync::atomic::Ordering;
        self.loading = true;
        self.built_delim = self.delim;
        self.progress.store(0, Ordering::Relaxed);
        *self.result.lock().unwrap() = None;
        let prog = self.progress.clone();
        let slot = self.result.clone();
        let path = self.path.clone();
        let delim = self.delim;
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let r =
                csvview::CsvIndex::build_with_delim(&path, prog, delim).map_err(|e| e.to_string());
            *slot.lock().unwrap() = Some(r);
            ctx.request_repaint();
        });
    }

    /// GCS をストリーミングして逐次プレビュー表示する（ローカルに保存しない）。
    /// 即時に先頭から表示し、上限（行数/バイト数）まで読み進める。
    fn start_gcs_preview(&mut self, uri: String, ctx: &egui::Context) {
        use std::sync::atomic::Ordering;
        let buf = std::sync::Arc::new(std::sync::Mutex::new(PreviewBuf::default()));
        self.preview = Some(buf.clone());
        self.gcs_uri = Some(uri.clone());
        self.built_delim = self.delim;
        self.loading = false; // 即表示（ローディング画面にしない）
        self.progress.store(0, Ordering::Relaxed);
        self.stream_cancel.store(false, Ordering::Relaxed);
        let prog = self.progress.clone();
        let stop = self.stream_cancel.clone();
        let delim = self.delim;
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            const ROW_CAP: usize = 2_000_000; // メモリ保護
            const BYTE_CAP: u64 = 256 * 1024 * 1024;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt");
            rt.block_on(async move {
                let mut resp = match crate::query::gcs_get_stream(&uri).await {
                    Ok(r) => r,
                    Err(e) => {
                        let mut b = buf.lock().unwrap();
                        b.error = Some(format!("GCS 取得失敗: {e}"));
                        b.complete = true;
                        ctx.request_repaint();
                        return;
                    }
                };
                let mut carry: Vec<u8> = Vec::new();
                let mut total: u64 = 0;
                let mut first = true;
                let mut reached_eof = true;
                loop {
                    let chunk = match resp.chunk().await {
                        Ok(Some(c)) => c,
                        Ok(None) => break,
                        Err(e) => {
                            buf.lock().unwrap().error = Some(format!("読み込み中断: {e}"));
                            reached_eof = false;
                            break;
                        }
                    };
                    total += chunk.len() as u64;
                    prog.store(total, Ordering::Relaxed);
                    let mut bytes: &[u8] = chunk.as_ref();
                    if first {
                        if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
                            bytes = &bytes[3..];
                        }
                        first = false;
                    }
                    carry.extend_from_slice(bytes);
                    // carry から完成レコードを切り出す（RFC4180 引用符対応＝インポート
                    // /照合と同じ数え方。引用符内の改行は 1 レコード内に束ねる）。
                    let mut newlines: Vec<Vec<u8>> = Vec::new();
                    let mut s = 0usize;
                    while let Some((ce, next, blank)) =
                        crate::csvview::next_complete_record(&carry, s, delim)
                    {
                        if !blank {
                            newlines.push(carry[s..ce].to_vec());
                        }
                        s = next;
                    }
                    carry.drain(0..s);
                    let mut capped = false;
                    if !newlines.is_empty() {
                        let mut b = buf.lock().unwrap();
                        b.lines.extend(newlines);
                        b.bytes_read = total;
                        if b.lines.len() >= ROW_CAP || total >= BYTE_CAP {
                            b.capped = true;
                            capped = true;
                        }
                    }
                    ctx.request_repaint();
                    if capped || stop.load(Ordering::Relaxed) {
                        reached_eof = false;
                        break;
                    }
                }
                let mut b = buf.lock().unwrap();
                if reached_eof {
                    // 改行で終わらない最後のレコード（引用符対応・空行は除外）。
                    if let Some(line) = crate::csvview::final_record(&carry, delim) {
                        b.lines.push(line);
                    }
                }
                b.complete = reached_eof;
                ctx.request_repaint();
            });
        });
    }

    /// 検索/絞り込みを背景スレッドで実行（全件スキャン・進捗・キャンセル可）。
    /// 索引（mmap）はファイルを走査、プレビューは読み込み済みバッファを走査。
    fn start_filter(&mut self, ctx: &egui::Context) {
        use std::sync::atomic::Ordering;
        let needle = self.filter.trim().to_string();
        let col = self.filter_col;
        let delim = self.delim;
        let has_header = self.has_header;
        let enc = self.encoding;
        let cancel = self.filter_cancel.clone();
        let prog = self.filter_progress.clone();
        let slot = self.filter_result.clone();
        let ctx = ctx.clone();
        const CAP: usize = 5_000_000; // 一致件数の上限（メモリ保護）

        if let Some(idx) = self.index.clone() {
            self.filtering = true;
            self.filter_cancel.store(false, Ordering::Relaxed);
            self.filter_progress.store(0, Ordering::Relaxed);
            *self.filter_result.lock().unwrap() = None;
            std::thread::spawn(move || {
                let v = idx.scan_filter(&needle, col, delim, has_header, enc, &cancel, &prog, CAP);
                *slot.lock().unwrap() = Some(v);
                ctx.request_repaint();
            });
        } else if let Some(pv) = self.preview.clone() {
            self.filtering = true;
            self.filter_cancel.store(false, Ordering::Relaxed);
            *self.filter_result.lock().unwrap() = None;
            // 読み込み済みの行スナップショットを走査（ストリームを止めない）。
            let lines = pv.lock().unwrap().lines.clone();
            std::thread::spawn(move || {
                let needle_l = needle.to_lowercase();
                let mut out: Vec<u64> = Vec::new();
                for (i, line) in lines.iter().enumerate() {
                    if has_header && i == 0 {
                        continue;
                    }
                    let hit = match col {
                        None => enc.decode(line).to_lowercase().contains(&needle_l),
                        Some(c) => csvview::split_fields(line, delim)
                            .get(c)
                            .map(|f| enc.decode(f).to_lowercase().contains(&needle_l))
                            .unwrap_or(false),
                    };
                    if hit {
                        out.push(i as u64);
                        if out.len() >= CAP {
                            break;
                        }
                    }
                    if i % (1 << 16) == 0 && cancel.load(Ordering::Relaxed) {
                        break;
                    }
                }
                *slot.lock().unwrap() = Some(out);
                ctx.request_repaint();
            });
        }
    }

    /// 背景タスクの結果を取り込む（毎フレーム全タブで呼ぶ）。
    fn drain(&mut self) {
        if self.loading {
            // 先に取り出して MutexGuard を落としてから self を変更する（借用衝突回避）。
            let r = self.result.lock().unwrap().take();
            if let Some(r) = r {
                self.loading = false;
                match r {
                    Ok(idx) => {
                        self.index = Some(std::sync::Arc::new(idx));
                        self.invalidate_col_widths();
                    }
                    Err(e) => self.err = Some(e),
                }
            }
        }
        if self.filtering {
            let v = self.filter_result.lock().unwrap().take();
            if let Some(v) = v {
                self.filtering = false;
                self.first_row = 0;
                self.matches = Some(std::sync::Arc::new(v));
                self.invalidate_col_widths();
            }
        }
    }

    /// 表示するヘッダのセル（ヘッダ有: 先頭行をデコード / 無: 列1,列2…）。
    /// データ表示の準備ができているか（索引 or プレビュー）。
    fn ready(&self) -> bool {
        self.index.is_some() || self.preview.is_some()
    }

    /// ファイル行の総数（ヘッダ込み）。プレビューはこれまで読み込めた行数。
    fn total_lines(&self) -> u64 {
        if let Some(pv) = &self.preview {
            pv.lock().unwrap().lines.len() as u64
        } else if let Some(idx) = &self.index {
            idx.total_rows
        } else {
            0
        }
    }

    /// 表示上の「データ行数」。先頭行ヘッダのチェックが入っていればヘッダを除く。
    /// インポート/照合タブの件数（データ行のみ）と一致する。
    fn data_rows(&self) -> u64 {
        let total = self.total_lines();
        if self.has_header {
            total.saturating_sub(1)
        } else {
            total
        }
    }

    /// ファイル行 i の生バイト列（所有）。索引/プレビューの両方を吸収する。
    fn line_at(&self, i: u64) -> Option<Vec<u8>> {
        if let Some(pv) = &self.preview {
            pv.lock().unwrap().lines.get(i as usize).cloned()
        } else if let Some(idx) = &self.index {
            idx.row_bytes(i).map(|b| b.to_vec())
        } else {
            None
        }
    }

    /// 列数（先頭行のフィールド数）。
    fn ncols(&self) -> usize {
        self.line_at(0)
            .map(|l| csvview::split_fields(&l, self.delim).len())
            .unwrap_or(0)
            .max(1)
    }

    fn header_cells(&self) -> Vec<String> {
        if !self.ready() {
            return Vec::new();
        }
        if self.has_header {
            self.line_at(0)
                .map(|l| {
                    csvview::split_fields(&l, self.delim)
                        .iter()
                        .map(|f| self.encoding.decode(f))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            (1..=self.ncols()).map(|i| format!("列{i}")).collect()
        }
    }

    /// 表示対象の `first` 行目から `count` 行分の、画面に出すセル列を返す。
    /// ヘッダ除外・絞り込み（matches）・エンコーディングを反映する（テスト可能）。
    fn visible_rows(&self, first: u64, count: u64) -> Vec<Vec<String>> {
        if !self.ready() {
            return Vec::new();
        }
        let total = self.total_lines();
        let header_off: u64 = if self.has_header && total > 0 { 1 } else { 0 };
        let data_rows: u64 = match &self.matches {
            Some(m) => m.len() as u64,
            None => total.saturating_sub(header_off),
        };
        let mut out = Vec::new();
        for di in first..first.saturating_add(count) {
            if di >= data_rows {
                break;
            }
            let file_row = match &self.matches {
                Some(m) => m[di as usize],
                None => header_off + di,
            };
            if let Some(l) = self.line_at(file_row) {
                out.push(
                    csvview::split_fields(&l, self.delim)
                        .iter()
                        .map(|f| self.encoding.decode(f))
                        .collect(),
                );
            }
        }
        out
    }

    /// アクティブタブの中身（操作行＋グリッド）を描画する。
    fn show(&mut self, ui: &mut egui::Ui) {
        use std::sync::atomic::Ordering;
        let salt = self.title.clone();
        let mut do_goto = false;
        ui.add_space(6.0);
        let mut layout_changed = false;
        ui.horizontal(|ui| {
            layout_changed |= ui.checkbox(&mut self.has_header, "先頭行をヘッダ").changed();
            egui::ComboBox::from_id_salt(("csv_delim", &salt))
                .selected_text(match self.delim {
                    b'\t' => "タブ",
                    b';' => "セミコロン",
                    b'|' => "パイプ",
                    _ => "カンマ",
                })
                .show_ui(ui, |ui| {
                    layout_changed |= ui.selectable_value(&mut self.delim, b',', "カンマ").changed();
                    layout_changed |= ui.selectable_value(&mut self.delim, b'\t', "タブ").changed();
                    layout_changed |= ui.selectable_value(&mut self.delim, b';', "セミコロン").changed();
                    layout_changed |= ui.selectable_value(&mut self.delim, b'|', "パイプ").changed();
                });
            egui::ComboBox::from_id_salt(("csv_enc", &salt))
                .selected_text(match self.encoding {
                    query::Encoding::ShiftJis => "Shift-JIS",
                    _ => "UTF-8",
                })
                .show_ui(ui, |ui| {
                    layout_changed |= ui
                        .selectable_value(&mut self.encoding, query::Encoding::Utf8, "UTF-8")
                        .changed();
                    layout_changed |= ui
                        .selectable_value(&mut self.encoding, query::Encoding::ShiftJis, "Shift-JIS")
                        .changed();
                });
            if self.ready() {
                ui.separator();
                ui.label("行へ移動:");
                let r = ui.add(
                    egui::TextEdit::singleline(&mut self.goto)
                        .desired_width(90.0)
                        .hint_text("行番号"),
                );
                if (ui.button("移動").clicked())
                    || (r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                {
                    do_goto = true;
                }
                ui.separator();
                if ui
                    .button("🔍 診断")
                    .on_hover_text(
                        "読み込んだCSVを解析し、列ズレ（読み方/区切りの問題）か\n\
                         データ自体の桁落ち/科学表記かを判定します。",
                    )
                    .clicked()
                {
                    self.diag = Some(self.diagnose());
                }
                if let Some(c) = &self.copied_note {
                    ui.label(egui::RichText::new(format!("📋 {c}")).color(ACCENT))
                        .on_hover_text("クリップボードにコピー済み");
                } else {
                    ui.label(
                        egui::RichText::new("セルをクリックでコピー").color(MUTED).small(),
                    );
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if self.ready() {
                    let total = self.total_lines();
                    let rows = self.data_rows();
                    // バイト数: 索引はファイル全体、プレビューは読み込み済み。
                    let bytes = if let Some(idx) = &self.index {
                        Some(idx.bytes as f64)
                    } else {
                        self.preview
                            .as_ref()
                            .map(|pv| pv.lock().unwrap().bytes_read as f64)
                    };
                    let suffix = match bytes {
                        Some(b) => format!(" · {}", human_bytes(b)),
                        None => String::new(),
                    };
                    let label = if self.has_header {
                        format!("{} 行（ヘッダ除く）{}", fmt_count(rows as usize), suffix)
                    } else {
                        format!("{} 行{}", fmt_count(rows as usize), suffix)
                    };
                    ui.label(egui::RichText::new(label).color(MUTED)).on_hover_text(format!(
                        "データ行数（インポート/照合の件数と一致）。ファイル総レコード数: {}\n\
                         列の境界をドラッグで幅変更・ダブルクリックで内容にフィット",
                        fmt_count(total as usize)
                    ));
                }
            });
        });
        if do_goto {
            if let Ok(n) = self.goto.trim().replace(',', "").parse::<u64>() {
                self.first_row = n.saturating_sub(1);
            }
        }
        if layout_changed {
            // ヘッダ有無/区切り/文字コードが変わると列の内容も変わるので幅を再算出。
            self.invalidate_col_widths();
        }
        // 区切り文字が変わるとレコード境界（引用符の開始判定）が変わるので、
        // 索引/プレビューを作り直してレコード数・行内容を一致させる。
        if self.delim != self.built_delim {
            let ctx = ui.ctx().clone();
            if let Some(uri) = self.gcs_uri.clone() {
                self.stream_cancel.store(true, Ordering::Relaxed);
                self.start_gcs_preview(uri, &ctx);
            } else {
                self.index = None;
                self.start_load(&ctx);
            }
            self.first_row = 0;
            self.matches = None;
        }

        // ── 検索 / 絞り込み行 ──
        if self.ready() {
            let header_names: Vec<String> = self.header_cells();
            let mut do_filter = false;
            let mut do_clear = false;
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label("絞り込み:");
                let r = ui.add(
                    egui::TextEdit::singleline(&mut self.filter)
                        .desired_width(220.0)
                        .hint_text("含む文字列（大小無視）"),
                );
                let col_label = match self.filter_col {
                    None => "全列".to_string(),
                    Some(c) => header_names.get(c).cloned().unwrap_or_else(|| format!("列{c}")),
                };
                egui::ComboBox::from_id_salt(("csv_filter_col", &salt))
                    .selected_text(col_label)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.filter_col, None, "全列");
                        for (c, name) in header_names.iter().enumerate() {
                            ui.selectable_value(&mut self.filter_col, Some(c), name);
                        }
                    });
                if ui.button("実行").clicked()
                    || (r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                {
                    do_filter = true;
                }
                if self.matches.is_some() && ui.button("解除").clicked() {
                    do_clear = true;
                }
                if self.filtering {
                    ui.spinner();
                    if ui.button("キャンセル").clicked() {
                        self.filter_cancel.store(true, Ordering::Relaxed);
                    }
                }
                if let Some(m) = &self.matches {
                    ui.label(
                        egui::RichText::new(format!("{} 件一致", fmt_count(m.len()))).color(ACCENT),
                    );
                }
            });
            if do_filter && !self.filter.trim().is_empty() {
                self.start_filter(ui.ctx());
            }
            if do_clear {
                self.matches = None;
                self.first_row = 0;
                self.invalidate_col_widths();
            }
            if self.filtering {
                let done = self.filter_progress.load(Ordering::Relaxed);
                let frac = if self.total_bytes > 0 {
                    (done as f32 / self.total_bytes as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                ui.add(
                    egui::ProgressBar::new(frac)
                        .text("スキャン中…")
                        .desired_width(f32::INFINITY),
                );
                ui.ctx().request_repaint();
            }
        }

        // ローカルの索引作成中（プレビューは即表示するのでここを通らない）。
        if self.loading {
            let done = self.progress.load(Ordering::Relaxed);
            let frac = if self.total_bytes > 0 {
                (done as f32 / self.total_bytes as f32).clamp(0.0, 1.0)
            } else {
                0.0
            };
            ui.add_space(6.0);
            ui.add(
                egui::ProgressBar::new(frac)
                    .text(format!(
                        "索引作成中… {} / {}",
                        human_bytes(done as f64),
                        human_bytes(self.total_bytes as f64)
                    ))
                    .desired_width(f32::INFINITY),
            );
            ui.ctx().request_repaint();
            return;
        }
        // プレビュー（GCS ストリーミング）のエラー/取得状況。
        if let Some(pv) = &self.preview {
            let b = pv.lock().unwrap();
            if let Some(e) = &b.error {
                ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
            }
            let n = b.lines.len();
            let complete = b.complete;
            let capped = b.capped;
            let read = b.bytes_read;
            drop(b);
            let status = if let Some(_e) = &self.err {
                String::new()
            } else if complete {
                format!("{} 行（完了）", fmt_count(n))
            } else if capped {
                format!("{} 行（上限まで・以降は省略）", fmt_count(n))
            } else {
                ui.ctx().request_repaint();
                format!("≥ {} 行 取得中… {}", fmt_count(n), human_bytes(read as f64))
            };
            ui.label(egui::RichText::new(status).color(MUTED).small());
        }
        if let Some(e) = &self.err {
            ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
            return;
        }
        if !self.ready() {
            return;
        }
        // 診断レポート（出ていれば畳めるパネルで表示）。
        if self.diag.is_some() {
            let mut close = false;
            egui::Frame::group(ui.style()).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("CSV 診断").strong());
                    if ui.button("コピー").clicked() {
                        if let Some(d) = &self.diag {
                            ui.ctx().copy_text(d.clone());
                        }
                    }
                    if ui.button("閉じる").clicked() {
                        close = true;
                    }
                });
                if let Some(d) = &self.diag {
                    ui.add(
                        egui::Label::new(egui::RichText::new(d).monospace().small()).wrap(),
                    );
                }
            });
            if close {
                self.diag = None;
            }
        }
        ui.add_space(4.0);
        self.grid_ui(ui);
    }

    /// 行インデックスで仮想化したカスタムグリッド（f32 の限界に縛られず数十億行でも可）。
    fn grid_ui(&mut self, ui: &mut egui::Ui) {
        let salt = self.title.clone();
        let has_header = self.has_header;
        let total = self.total_lines();
        let ncols = self.ncols();
        let header_off: u64 = if has_header && total > 0 { 1 } else { 0 };
        let matches = self.matches.clone();
        let data_rows: u64 = match &matches {
            Some(m) => m.len() as u64,
            None => total - header_off,
        };

        let row_h = 22.0_f32;
        let sb = 12.0_f32;
        let full = ui.available_rect_before_wrap();
        let grid = egui::Rect::from_min_max(full.min, egui::pos2(full.max.x - sb, full.max.y - sb));
        let header_h = row_h;
        let body_top = grid.top() + header_h;
        let body_h = (grid.bottom() - body_top).max(0.0);
        let visible = (body_h / row_h).floor().max(0.0) as u64;
        let max_first = data_rows.saturating_sub(visible);

        // 列幅を内容に合わせて算出（必要時のみ。ヘッダ + 先頭サンプル）。
        if self.col_widths.len() != ncols {
            self.col_widths = self.compute_col_widths();
        }

        // ── 列幅の手動調整: 境界ドラッグでリサイズ / ダブルクリックで内容にフィット ──
        {
            let coloff = self.col_off;
            let mut bounds = Vec::with_capacity(ncols);
            let mut cx = 0.0_f32;
            for w in &self.col_widths {
                cx += *w;
                bounds.push(cx); // 列 ci の右境界（コンテンツ座標）
            }
            for ci in 0..ncols {
                let bx = bounds.get(ci).copied().unwrap_or(0.0);
                let x = grid.left() + bx - coloff;
                if x < grid.left() - 1.0 || x > grid.right() + 1.0 {
                    continue;
                }
                let handle = egui::Rect::from_min_max(
                    egui::pos2(x - 3.0, grid.top()),
                    egui::pos2(x + 3.0, grid.bottom()),
                );
                let id = ui.id().with(("csv_colsep", &salt, ci));
                let r = ui.interact(handle, id, egui::Sense::click_and_drag());
                if r.hovered() || r.dragged() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::ResizeHorizontal);
                }
                if r.dragged() {
                    let dx = r.drag_delta().x;
                    if let Some(w) = self.col_widths.get_mut(ci) {
                        *w = (*w + dx).clamp(40.0, 4000.0);
                    }
                } else if r.double_clicked() {
                    let fit = self.fit_col_width(ci);
                    if let Some(w) = self.col_widths.get_mut(ci) {
                        *w = fit;
                    }
                }
            }
        }

        // 調整後の幅から各列の左端オフセット（累積）と総幅を作る。
        let widths = self.col_widths.clone();
        let mut col_x = Vec::with_capacity(ncols + 1);
        let mut acc = 0.0_f32;
        for w in &widths {
            col_x.push(acc);
            acc += *w;
        }
        col_x.push(acc);
        let content_w = acc;
        let max_off = (content_w - grid.width()).max(0.0);

        let resp = ui.interact(grid, ui.id().with(("csv_grid", &salt)), egui::Sense::hover());
        if resp.hovered() {
            let (dy, dx) = ui.input(|i| (i.smooth_scroll_delta.y, i.smooth_scroll_delta.x));
            if dy != 0.0 {
                let d = (dy / row_h) as f64;
                self.first_row = (self.first_row as f64 - d).max(0.0) as u64;
            }
            if dx != 0.0 {
                self.col_off = (self.col_off - dx).clamp(0.0, max_off);
            }
        }
        let vtrack = egui::Rect::from_min_max(
            egui::pos2(grid.right(), body_top),
            egui::pos2(full.right(), grid.bottom()),
        );
        let vresp = ui.interact(vtrack, ui.id().with(("csv_vsb", &salt)), egui::Sense::click_and_drag());
        if (vresp.dragged() || vresp.clicked()) && max_first > 0 {
            if let Some(p) = vresp.interact_pointer_pos() {
                let frac = ((p.y - vtrack.top()) / vtrack.height().max(1.0)).clamp(0.0, 1.0);
                self.first_row = (frac as f64 * max_first as f64).round() as u64;
            }
        }
        let htrack = egui::Rect::from_min_max(
            egui::pos2(grid.left(), grid.bottom()),
            egui::pos2(grid.right(), full.bottom()),
        );
        let hresp = ui.interact(htrack, ui.id().with(("csv_hsb", &salt)), egui::Sense::click_and_drag());
        if (hresp.dragged() || hresp.clicked()) && max_off > 0.0 {
            if let Some(p) = hresp.interact_pointer_pos() {
                let frac = ((p.x - htrack.left()) / htrack.width().max(1.0)).clamp(0.0, 1.0);
                self.col_off = frac * max_off;
            }
        }
        if self.first_row > max_first {
            self.first_row = max_first;
        }
        if self.col_off > max_off {
            self.col_off = max_off;
        }

        let first = self.first_row;
        let coloff = self.col_off;
        let header_cells = self.header_cells();
        let rows = self.visible_rows(first, visible);

        // セルクリックで値をクリップボードにコピー（カスタム描画なので手動ヒットテスト）。
        let body_rect = egui::Rect::from_min_max(
            egui::pos2(grid.left(), body_top),
            egui::pos2(grid.right(), grid.bottom()),
        );
        let cell_click =
            ui.interact(body_rect, ui.id().with(("csv_cellclick", &salt)), egui::Sense::click());
        if cell_click.clicked() {
            if let Some(p) = cell_click.interact_pointer_pos() {
                let vr = ((p.y - body_top) / row_h).floor() as i64;
                let content_x = p.x - grid.left() + coloff;
                let mut ci = None;
                for c in 0..ncols {
                    if content_x >= col_x[c] && content_x < col_x[c + 1] {
                        ci = Some(c);
                        break;
                    }
                }
                if vr >= 0 && (vr as usize) < rows.len() {
                    if let Some(ci) = ci {
                        if let Some(v) = rows[vr as usize].get(ci) {
                            ui.ctx().copy_text(v.clone());
                            self.selected = Some((first + vr as u64, ci));
                            // 確認用に表示（長い値は頭だけ）。桁数の目視確認にも使える。
                            let short: String = v.chars().take(120).collect();
                            self.copied_note = Some(format!("{}（{}文字）", short, v.chars().count()));
                        }
                    }
                }
            }
        }
        let selected = self.selected;

        let painter = ui.painter_at(full);
        painter.rect_filled(full, 0.0, BASE);

        // 列幅に応じてセル文字列を省略する（… 付き）。
        let clip = |s: &str, w: f32| -> String {
            let cell_chars = ((w - 12.0) / 7.5).max(2.0) as usize;
            if s.chars().count() > cell_chars {
                let mut t: String = s.chars().take(cell_chars.saturating_sub(1)).collect();
                t.push('…');
                t
            } else {
                s.to_string()
            }
        };
        let draw_cells = |cells: &[String], y: f32, strong: bool, fg: egui::Color32| {
            for (ci, &cx) in col_x.iter().take(ncols).enumerate() {
                let cw = widths.get(ci).copied().unwrap_or(120.0);
                let x = grid.left() + cx - coloff;
                if x + cw < grid.left() || x > grid.right() {
                    continue;
                }
                if let Some(v) = cells.get(ci) {
                    let font = if strong {
                        egui::FontId::proportional(12.5)
                    } else {
                        egui::FontId::proportional(12.0)
                    };
                    painter.text(
                        egui::pos2(x + 6.0, y + row_h * 0.5),
                        egui::Align2::LEFT_CENTER,
                        clip(v, cw),
                        font,
                        fg,
                    );
                }
                painter.line_segment(
                    [egui::pos2(x, body_top), egui::pos2(x, grid.bottom())],
                    egui::Stroke::new(1.0, egui::Color32::from_gray(45)),
                );
            }
        };

        painter.rect_filled(
            egui::Rect::from_min_max(grid.min, egui::pos2(grid.right(), body_top)),
            0.0,
            ELEVATED,
        );
        draw_cells(&header_cells, grid.top(), true, egui::Color32::from_gray(230));
        painter.line_segment(
            [egui::pos2(grid.left(), body_top), egui::pos2(grid.right(), body_top)],
            egui::Stroke::new(1.0, ACCENT),
        );

        for (vr, cells) in rows.iter().enumerate() {
            let y = body_top + vr as f32 * row_h;
            if !vr.is_multiple_of(2) {
                painter.rect_filled(
                    egui::Rect::from_min_max(
                        egui::pos2(grid.left(), y),
                        egui::pos2(grid.right(), y + row_h),
                    ),
                    0.0,
                    egui::Color32::from_gray(24),
                );
            }
            draw_cells(cells, y, false, egui::Color32::from_gray(210));
            // 選択中セルを枠でハイライト。
            if let Some((srow, scol)) = selected {
                if srow == first + vr as u64 && scol < ncols {
                    let cx = grid.left() + col_x[scol] - coloff;
                    let cw = widths.get(scol).copied().unwrap_or(120.0);
                    let left = cx.max(grid.left());
                    let right = (cx + cw).min(grid.right());
                    if right > left {
                        painter.rect_stroke(
                            egui::Rect::from_min_max(egui::pos2(left, y), egui::pos2(right, y + row_h)),
                            0.0,
                            egui::Stroke::new(2.0, ACCENT),
                            egui::StrokeKind::Inside,
                        );
                    }
                }
            }
        }

        let vbar_bg = egui::Color32::from_gray(30);
        painter.rect_filled(vtrack, 0.0, vbar_bg);
        if max_first > 0 && vtrack.height() > 0.0 {
            // つまみの最小高は 24px だがトラックがそれより短いと min>max で clamp が
            // パニックするため、トラック高で頭打ちにする。
            let min_th = 24.0_f32.min(vtrack.height());
            let th =
                (visible as f32 / data_rows as f32 * vtrack.height()).clamp(min_th, vtrack.height());
            let ty = vtrack.top() + (first as f32 / max_first as f32) * (vtrack.height() - th);
            painter.rect_filled(
                egui::Rect::from_min_size(egui::pos2(vtrack.left() + 2.0, ty), egui::vec2(sb - 4.0, th)),
                egui::CornerRadius::same(3),
                egui::Color32::from_gray(90),
            );
        }
        painter.rect_filled(htrack, 0.0, vbar_bg);
        if max_off > 0.0 && htrack.width() > 0.0 {
            // 縦と同じく、トラックが 24px 未満でも min>max で clamp が panic しないようにする。
            let min_tw = 24.0_f32.min(htrack.width());
            let tw = (grid.width() / content_w * htrack.width()).clamp(min_tw, htrack.width());
            let tx = htrack.left() + (coloff / max_off) * (htrack.width() - tw);
            painter.rect_filled(
                egui::Rect::from_min_size(egui::pos2(tx, htrack.top() + 2.0), egui::vec2(tw, sb - 4.0)),
                egui::CornerRadius::same(3),
                egui::Color32::from_gray(90),
            );
        }
    }
}

impl MonitorApp {
    /// CSV ファイルを選んで新しいタブで開く（複数同時に開ける）。
    fn open_csv(&mut self, ctx: &egui::Context) {
        let Some(path) = pick_csv_file() else {
            return;
        };
        // 同じファイルが既に開いていればそれをアクティブに。
        if let Some(i) = self.csv_tabs.iter().position(|t| t.path == path) {
            self.csv_active = i;
            return;
        }
        let mut tab = CsvTab::new(path);
        tab.start_load(ctx);
        self.csv_tabs.push(tab);
        self.csv_active = self.csv_tabs.len() - 1;
    }

    /// GCS の CSV を新しいタブで開く（一時ファイルへ取得 → 索引化）。
    fn open_gcs_csv(&mut self, uri: String, ctx: &egui::Context) {
        use std::hash::{Hash, Hasher};
        let uri = uri.trim().to_string();
        if uri.is_empty() {
            return;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        uri.hash(&mut h);
        let dest =
            std::env::temp_dir().join(format!("spanner_viewer_gcs_{:016x}.csv", h.finish()));
        let mut tab = CsvTab::new(dest);
        tab.title = uri
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or(uri.as_str())
            .to_string();
        tab.start_gcs_preview(uri, ctx);
        self.csv_tabs.push(tab);
        self.csv_active = self.csv_tabs.len() - 1;
    }

    fn csv_view(&mut self, ui: &mut egui::Ui) {
        for t in &mut self.csv_tabs {
            t.drain();
        }

        // ── タブバー（VS Code 風） ──
        let mut do_open = false;
        let mut activate: Option<usize> = None;
        let mut close: Option<usize> = None;
        ui.add_space(4.0);
        let mut do_gcs = false;
        ui.horizontal(|ui| {
            if ui.button("＋ CSV を開く").clicked() {
                do_open = true;
            }
            if ui.button("GCS を開く…").clicked() {
                do_gcs = true;
            }
        });
        // ── タブ帯（VS Code 風） ──
        if !self.csv_tabs.is_empty() {
            ui.add_space(4.0);
            egui::Frame::NONE.fill(PANEL).show(ui, |ui| {
                ui.spacing_mut().item_spacing = egui::vec2(0.0, 0.0);
                egui::ScrollArea::horizontal()
                    .auto_shrink([false, true])
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.spacing_mut().item_spacing = egui::vec2(0.0, 0.0);
                            for (i, t) in self.csv_tabs.iter().enumerate() {
                                let (act, cl) =
                                    draw_vscode_tab(ui, &t.title, i == self.csv_active, t.loading, i);
                                if act {
                                    activate = Some(i);
                                }
                                if cl {
                                    close = Some(i);
                                }
                            }
                        });
                    });
            });
            // 帯下のボーダー（エディタ領域との境界）。
            let sep = ui.min_rect();
            ui.painter().hline(
                sep.x_range(),
                ui.cursor().top(),
                egui::Stroke::new(1.0, BORDER),
            );
        }
        if do_open {
            self.open_csv(ui.ctx());
        }
        if do_gcs {
            self.csv_gcs_open = true;
        }
        // GCS URI 入力ウィンドウ。
        if self.csv_gcs_open {
            let ctx = ui.ctx().clone();
            let mut open = true;
            let mut go = false;
            egui::Window::new("GCS の CSV を開く")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(&ctx, |ui| {
                    ui.label("gs://バケット/パス.csv を入力（認証は ADC）");
                    let r = ui.add(
                        egui::TextEdit::singleline(&mut self.csv_gcs_uri)
                            .desired_width(380.0)
                            .hint_text("gs://my-bucket/data.csv"),
                    );
                    ui.horizontal(|ui| {
                        if ui.button("開く").clicked()
                            || (r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)))
                        {
                            go = true;
                        }
                        if ui.button("キャンセル").clicked() {
                            open = false;
                        }
                    });
                    ui.label(
                        egui::RichText::new("一時ファイルへ取得してから表示します（巨大可）")
                            .color(MUTED)
                            .small(),
                    );
                });
            if go {
                let uri = self.csv_gcs_uri.clone();
                self.open_gcs_csv(uri, &ctx);
                self.csv_gcs_open = false;
            } else {
                self.csv_gcs_open = open;
            }
        }
        if let Some(i) = activate {
            self.csv_active = i;
        }
        if let Some(i) = close {
            if i < self.csv_tabs.len() {
                self.csv_tabs.remove(i);
                // アクティブより左を閉じたら、表示中タブがずれないよう index を詰める。
                if i < self.csv_active {
                    self.csv_active -= 1;
                }
            }
            self.csv_active = self.csv_active.min(self.csv_tabs.len().saturating_sub(1));
        }

        if self.csv_tabs.is_empty() {
            centered_hint(
                ui,
                "「＋ CSV を開く」で開いてください（複数タブ・巨大ファイル対応）",
            );
            return;
        }
        let active = self.csv_active.min(self.csv_tabs.len() - 1);
        self.csv_active = active;
        ui.separator();
        self.csv_tabs[active].show(ui);
    }

    /// 左アクティビティバー: セクション切替（Spanner / Kubernetes）。
    fn activity_bar(&mut self, ui: &mut egui::Ui) {
        ui.add_space(12.0);
        if activity_item(
            ui,
            self.section == Section::Spanner,
            draw_db_icon,
            "Spanner",
        ) {
            // Kube から離れるのでログ追従を止める（プロセスを残さない）。
            self.stop_logs();
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
        if activity_item(
            ui,
            self.section == Section::Csv,
            draw_csv_icon,
            "CSV ビューア",
        ) {
            self.stop_logs();
            self.section = Section::Csv;
        }
        // 設定（歯車）はバー下部に
        ui.with_layout(egui::Layout::bottom_up(egui::Align::Center), |ui| {
            ui.add_space(10.0);
            if activity_item(ui, self.settings_open, draw_gear_icon, "設定") {
                self.settings_open = !self.settings_open;
            }
        });
    }

    fn monitor_view(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("status").show(ui, |ui| {
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

        egui::CentralPanel::default_margins().show(ui, |ui| {
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
                        Line::new("CPU %", cpu)
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
                        Line::new("Storage %", storage_pct)
                            .color(STORAGE_COLOR)
                            .width(1.8)
                            .fill(0.0),
                    );
                });
        });
    }

    fn data_view(&mut self, ui: &mut egui::Ui) {
        // 実行したい SQL（ツリー/履歴クリックで設定し、借用解消後に実行）
        let mut load_run: Option<String> = None;
        let mut ddl_copy: Option<String> = None;
        // CSV インポート対象テーブル（借用解消後にダイアログを開く）
        let mut import_open: Option<TableNode> = None;
        let mut gcs_open: Option<TableNode> = None;

        // 左: オブジェクトツリー
        egui::Panel::left("db_objects")
            .default_size(240.0)
            .size_range(160.0..=420.0)
            .show(ui, |ui| {
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    // VS Code のセクション見出し風（小さめ・補助色）。
                    ui.label(
                        egui::RichText::new("データベース")
                            .small()
                            .strong()
                            .color(MUTED),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add_enabled(!self.schema_pending, egui::Button::new("⟳").small())
                            .on_hover_text("スキーマを再取得")
                            .clicked()
                        {
                            self.schema_graph = None;
                            self.run_schema();
                        }
                    });
                });
                ui.add_space(2.0);
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        // 行を密着させる（VS Code のツリーのように隙間なし）。
                        ui.spacing_mut().item_spacing.y = 0.0;
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
                        for node in &g.nodes {
                            let expanded = self.tree_expanded.contains(&node.name);
                            let chev = if expanded { "▾" } else { "▸" };
                            let resp = explorer_row(
                                ui,
                                8.0,
                                &format!("{chev}  {}", node.name),
                                TEXT,
                                false,
                            );
                            if resp.clicked() {
                                if expanded {
                                    self.tree_expanded.remove(&node.name);
                                } else {
                                    self.tree_expanded.insert(node.name.clone());
                                }
                            }
                            resp.context_menu(|ui| {
                                if ui.button("SELECT * を実行").clicked() {
                                    load_run =
                                        Some(format!("SELECT * FROM `{}` LIMIT 100", node.name));
                                    ui.close();
                                }
                                if ui.button("CSV をインポート…").clicked() {
                                    import_open = Some(node.clone());
                                    ui.close();
                                }
                                if ui.button("GCS から CSV をインポート…").clicked() {
                                    gcs_open = Some(node.clone());
                                    ui.close();
                                }
                                if ui.button("テーブル名をコピー").clicked() {
                                    ui.ctx().copy_text(node.name.clone());
                                    ui.close();
                                }
                                if ui.button("DDL をコピー").clicked() {
                                    ddl_copy = Some(build_ddl(node));
                                    ui.close();
                                }
                            });
                            if expanded {
                                for c in &node.columns {
                                    let key = if c.pk { "🔑" } else { "·" };
                                    let color = if c.pk { PK_COLOR } else { TEXT };
                                    let r = explorer_row(
                                        ui,
                                        30.0,
                                        &format!("{key} {}  {}", c.name, c.ty),
                                        color,
                                        true,
                                    );
                                    if r.clicked() {
                                        ui.ctx().copy_text(c.name.clone());
                                    }
                                }
                                for idx in &node.indexes {
                                    explorer_row(
                                        ui,
                                        30.0,
                                        &format!("🔎 {idx}"),
                                        DIAGRAM_ACCENT,
                                        true,
                                    );
                                }
                            }
                        }
                    });
            });

        // 補完候補のソース（テーブル名・カラム名）をスキーマから用意する。
        let (sql_tables, sql_columns): (Vec<String>, Vec<String>) = {
            let mut tabs = Vec::new();
            let mut cols = Vec::new();
            if let Some(g) = self.schema_graph.as_ref().filter(|g| g.error.is_none()) {
                for n in &g.nodes {
                    tabs.push(n.name.clone());
                    for c in &n.columns {
                        cols.push(c.name.clone());
                    }
                }
            }
            cols.sort();
            cols.dedup();
            (tabs, cols)
        };

        // 上: SQL エディタ + 実行 / 選択実行 / 履歴
        egui::Panel::top("query_bar").show(ui, |ui| {
            ui.add_space(6.0);

            // 補完ポップアップ表示中のキー操作を TextEdit より前に消費する。
            let mut accepted = false;
            if self.sql_sug_open && !self.sql_suggestions.is_empty() {
                let n = self.sql_suggestions.len();
                let mut do_accept: Option<String> = None;
                ui.input_mut(|i| {
                    if i.consume_key(egui::Modifiers::NONE, egui::Key::ArrowDown) {
                        self.sql_sug_index = (self.sql_sug_index + 1) % n;
                    }
                    if i.consume_key(egui::Modifiers::NONE, egui::Key::ArrowUp) {
                        self.sql_sug_index = (self.sql_sug_index + n - 1) % n;
                    }
                    if i.consume_key(egui::Modifiers::NONE, egui::Key::Tab) {
                        do_accept = self.sql_suggestions.get(self.sql_sug_index).cloned();
                    }
                    if i.consume_key(egui::Modifiers::NONE, egui::Key::Escape) {
                        self.sql_sug_open = false;
                    }
                });
                if let Some(cand) = do_accept {
                    apply_sql_completion(&mut self.sql, self.sql_word_range, &cand, &mut self.sql_set_cursor);
                    self.sql_sug_open = false;
                    accepted = true;
                }
            }

            let output = egui::TextEdit::multiline(&mut self.sql)
                .desired_rows(3)
                .desired_width(f32::INFINITY)
                .code_editor()
                .show(ui);

            // 確定後のカーソル位置を反映する（テキスト変更の次フレームで適用）。
            if let Some(cpos) = self.sql_set_cursor.take() {
                let id = output.response.response.id;
                let mut state = output.state.clone();
                state.cursor.set_char_range(Some(egui::text_selection::CCursorRange::one(
                    egui::text::CCursor::new(cpos),
                )));
                state.store(ui.ctx(), id);
                output.response.response.request_focus();
            }

            // 現在の単語から補完候補を作る（フォーカス時のみ）。
            let has_focus = output.response.response.has_focus();
            if has_focus && !accepted {
                if let Some(cr) = &output.cursor_range {
                    let (s, e) = current_word_range(&self.sql, cr.primary.index.0);
                    self.sql_word_range = (s, e);
                    let word = &self.sql[s..e];
                    self.sql_suggestions = sql_completions(word, &sql_tables, &sql_columns, 8);
                    self.sql_sug_open = !self.sql_suggestions.is_empty();
                    if self.sql_sug_index >= self.sql_suggestions.len() {
                        self.sql_sug_index = 0;
                    }
                }
            } else if !has_focus {
                self.sql_sug_open = false;
            }

            // 補完ポップアップを描画（クリックで確定）。
            if self.sql_sug_open && !self.sql_suggestions.is_empty() {
                let anchor = output
                    .cursor_range
                    .as_ref()
                    .map(|cr| {
                        let r = output.galley.pos_from_cursor(cr.primary);
                        output.galley_pos + r.left_bottom().to_vec2()
                    })
                    .unwrap_or_else(|| output.response.response.rect.left_bottom());
                let sugs = self.sql_suggestions.clone();
                let sel = self.sql_sug_index;
                let mut click: Option<String> = None;
                let mut hover: Option<usize> = None;
                egui::Area::new(ui.id().with("sql_ac_popup"))
                    .order(egui::Order::Foreground)
                    .fixed_pos(anchor + egui::vec2(0.0, 3.0))
                    .show(ui.ctx(), |ui| {
                        egui::Frame::popup(ui.style()).show(ui, |ui| {
                            ui.set_max_width(300.0);
                            for (i, s) in sugs.iter().enumerate() {
                                let r = ui.selectable_label(i == sel, s);
                                if r.clicked() {
                                    click = Some(s.clone());
                                }
                                if r.hovered() {
                                    hover = Some(i);
                                }
                            }
                        });
                    });
                if let Some(i) = hover {
                    self.sql_sug_index = i;
                }
                if let Some(cand) = click {
                    apply_sql_completion(&mut self.sql, self.sql_word_range, &cand, &mut self.sql_set_cursor);
                    self.sql_sug_open = false;
                }
            }

            // 選択範囲（あれば）を取り出す
            let selected: Option<String> = output.cursor_range.and_then(|cr| {
                let s = cr.slice_str(&self.sql);
                if !s.is_empty() {
                    Some(s.to_string())
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
        egui::CentralPanel::default_margins().show(ui, |ui| {
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
                if ui.button("×").on_hover_text("検索クリア").clicked() {
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
            ui.ctx().copy_text(ddl);
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
            purpose: GcsPurpose::Import,
            target: node,
            uri: "gs://".to_string(),
            status: None,
            bucket: String::new(),
            folders: Vec::new(),
            objects: Vec::new(),
            listed_at: None,
        });
    }

    // ── CSV↔DB 照合 ──

    /// ローカル CSV を選び、照合のマッピング状態を作る。
    fn open_verify_local(&mut self, node: TableNode) {
        let Some(path) = pick_csv_file() else {
            return;
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
        self.build_verify_state(node, query::ImportSource::File(path), file_name, bytes);
    }

    /// 照合用に GCS URI 入力ダイアログを開く（取得成功で照合状態へ）。
    fn open_verify_gcs(&mut self, node: TableNode) {
        self.gcs_dialog = Some(GcsDialog {
            purpose: GcsPurpose::Verify,
            target: node,
            uri: "gs://".to_string(),
            status: None,
            bucket: String::new(),
            folders: Vec::new(),
            objects: Vec::new(),
            listed_at: None,
        });
    }

    /// プレビュー（生バイト）から照合状態を組み立てる。
    fn build_verify_state(
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
        let mut st = VerifyState {
            table: node.name,
            table_columns: node.columns,
            source,
            source_name: display_name,
            preview_bytes,
            records,
            csv_headers: Vec::new(),
            encoding,
            delimiter,
            has_header: true,
            empty_as_null: true,
            numeric_match: true,
            null_token: String::new(),
            mapping: Vec::new(),
            note: Some("プレビューは先頭のみ。実行時に全行をストリーミングして突合します。".into()),
            config_msg: None,
        };
        st.recompute();
        self.verify = Some(st);
        self.verify_result = None;
        self.verify_progress = None;
        self.section = Section::Spanner;
        self.view = View::Verify;
    }

    /// 照合状態から VerifyRequest を組み立てる（検証込み）。
    fn verify_request(&self) -> Result<query::VerifyRequest, String> {
        let Some(d) = &self.verify else {
            return Err("照合の設定がありません".into());
        };
        let columns: Vec<query::VerifyColumn> = d
            .table_columns
            .iter()
            .zip(d.mapping.iter())
            .filter_map(|(col, m)| {
                m.map(|src| query::VerifyColumn {
                    name: col.name.clone(),
                    pk: col.pk,
                    src_index: src,
                })
            })
            .collect();
        if columns.is_empty() {
            return Err("比較する列がありません（マッピングしてください）".into());
        }
        let unmapped_pk = unmapped_pks(&d.table_columns, &d.mapping);
        if !unmapped_pk.is_empty() {
            return Err(format!(
                "主キー列が未割当です: {}（突合キーに必要です）",
                unmapped_pk.join(", ")
            ));
        }
        let null_token = (!d.null_token.is_empty()).then(|| d.null_token.clone());
        Ok(query::VerifyRequest {
            table: d.table.clone(),
            columns,
            source: d.source.clone(),
            has_header: d.has_header,
            encoding: d.encoding,
            delimiter: d.delimiter,
            empty_as_null: d.empty_as_null,
            null_token,
            numeric_match: d.numeric_match,
            cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// 照合を背景へ投入して開始する。
    fn start_verify(&mut self) {
        match self.verify_request() {
            Ok(req) => {
                self.verify_cancel = req.cancel.clone();
                if self.verify_req_tx.send(req).is_ok() {
                    self.verify_running = true;
                    self.verify_result = None;
                    self.verify_filter = None;
                    self.verify_progress = Some(("開始", 0, 0));
                } else {
                    self.copy_note = Some(WORKER_GONE.into());
                }
            }
            Err(e) => {
                if let Some(d) = &mut self.verify {
                    d.config_msg = Some(e);
                }
            }
        }
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
            id: 0,
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
        self.pending_bulk = Some(BulkSpec {
            template: req,
            folder: folder.clone(),
        });
        self.import_dialog = None;
        // 同フォルダを一覧する（応答で各 CSV を enqueue）。
        let _ = self.gcs_req_tx.send(query::GcsRequest::List(folder));
    }

    /// リクエストを 1 ジョブとしてキューに積み、キューを進める。
    fn push_job(&mut self, mut req: query::ImportRequest, source_name: String) {
        // 一意な id を発番（進捗/完了の紐付けに使う）。
        req.id = self.import_next_id;
        self.import_next_id += 1;
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
        save_import_jobs(&self.import_jobs);
    }

    /// 同時に走らせるインポートジョブ数の上限（リソース保護）。
    const MAX_PARALLEL_IMPORTS: usize = 3;

    /// キューを進める: 別テーブルなら並列、同一テーブルは直列で待機ジョブを送る。
    /// 同時実行は MAX_PARALLEL_IMPORTS まで。
    fn pump_import_queue(&mut self) {
        loop {
            // 現在実行中のジョブ数と、実行中テーブルの集合を集める。
            let running_tables: std::collections::HashSet<String> = self
                .import_jobs
                .iter()
                .filter(|j| j.status == JobStatus::Running)
                .map(|j| j.req.table.clone())
                .collect();
            if running_tables.len() >= Self::MAX_PARALLEL_IMPORTS {
                break;
            }
            // 次に送れる待機ジョブ: 同一テーブルが実行中でないもの（同一テーブルは直列化）。
            let max = Self::MAX_PARALLEL_IMPORTS;
            let next = self.import_jobs.iter_mut().find(|j| {
                j.status == JobStatus::Queued
                    && !j.sent
                    && can_dispatch(&running_tables, &j.req.table, max)
            });
            let Some(job) = next else {
                break;
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
            } else {
                job.status = JobStatus::Failed;
                job.result = Some(WORKER_GONE.into());
            }
        }
        self.import_pending = self
            .import_jobs
            .iter()
            .any(|j| j.status == JobStatus::Running || j.status == JobStatus::Queued);
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
    fn import_view(&mut self, ui: &mut egui::Ui) {
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

        egui::CentralPanel::default_margins().show(ui, |ui| {
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

            // ── ジョブ一覧（別テーブルは並列・同一テーブルは直列） ──
            if !self.import_jobs.is_empty() {
                ui.add_space(16.0);
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("インポートジョブ").strong())
                        .on_hover_text(
                            "別テーブルへのジョブは並列実行（最大3）。\
                             同一テーブルへのジョブは順番に直列実行します。",
                        );
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
                            egui::Frame::NONE
                                .fill(ELEVATED)
                                .corner_radius(egui::CornerRadius::same(6))
                                .inner_margin(egui::Margin::same(8))
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
                                                    if ui.small_button("取消").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                                JobStatus::Failed | JobStatus::Cancelled => {
                                                    if ui
                                                        .small_button("⟳ 再キュー")
                                                        .on_hover_text("続きから再開します")
                                                        .clicked()
                                                    {
                                                        requeue_idx = Some(i);
                                                    }
                                                    if ui.small_button("×").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                                JobStatus::Done => {
                                                    if ui.small_button("×").clicked() {
                                                        remove_idx = Some(i);
                                                    }
                                                }
                                            },
                                        );
                                    });
                                    // 進捗バー（実行中）。
                                    if let Some(p) = &job.progress {
                                        // 速度・ETA（経過時間と進捗から算出）。
                                        let rate = import_rate_eta(job.started, p);
                                        // 総件数の推定（バイト進捗から: written × 全体/読込済）。
                                        let count = match import_total_estimate(p) {
                                            Some(y) => format!(
                                                "{} / 約{} 件",
                                                fmt_count(p.written),
                                                fmt_count(y)
                                            ),
                                            None => format!("{} 件", fmt_count(p.written)),
                                        };
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
                                                    "{:.0}%  ·  {count}{bytes}{rate}",
                                                    f * 100.0
                                                );
                                                paint_import_bar(ui, Some(f), &text);
                                            }
                                            None => {
                                                paint_import_bar(
                                                    ui,
                                                    None,
                                                    &format!("取込中…  {count}{rate}"),
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
                save_import_jobs(&self.import_jobs);
            }
        }
        if clear_done {
            self.import_jobs.retain(|j| j.is_active());
            save_import_jobs(&self.import_jobs);
        }
        if do_report {
            self.export_import_report(ui.ctx());
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
        let _ = std::fs::write(dir.join("report.md"), md);
        // スクショは次フレームで Event::Screenshot として届く。届いたら同フォルダへ保存。
        self.pending_report_dir = Some(dir.clone());
        self.pending_report_wait = 30; // 約30フレーム待って来なければ諦める
        ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(egui::UserData::default()));
        self.copy_note = Some(format!("レポートを出力中…: {}", dir.display()));
    }

    /// スクリーンショット応答が来ていれば PNG として保存し、Finder で開く。
    fn drain_screenshot(&mut self, ctx: &egui::Context) {
        // 依頼中でなければスクショイベントには触れない（無関係なスクショを拾わない）。
        if self.pending_report_dir.is_none() {
            return;
        }
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
        } else {
            // まだ届かない。一定フレーム待っても来なければ諦めて pending を解放する。
            self.pending_report_wait = self.pending_report_wait.saturating_sub(1);
            if self.pending_report_wait == 0 {
                if let Some(dir) = self.pending_report_dir.take() {
                    let _ = std::process::Command::new("open").arg(&dir).spawn();
                    self.copy_note = Some(format!(
                        "レポートは保存（スクショ取得に失敗）: {}",
                        dir.display()
                    ));
                }
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
                                .hint_text("追加トークン 例: NULL, \\N（空なら無効）")
                                .desired_width(200.0),
                        )
                        .on_hover_text(
                            "この文字列のセルを NULL として書き込みます（完全一致・大文字小文字を区別）。\
                             ＜null＞ と (null) は既定で NULL になります（このトークンは追加分）。",
                        );
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
                        egui::RichText::new(
                            "テーブル列 ← CSV 列（ヘッダ無しは列の並び順で対応。\
                             右の「先頭行の値」で対応を確認してください）",
                        )
                        .color(MUTED)
                        .small(),
                    );
                    ui.add_space(2.0);

                    // 先頭データ行（ヘッダ有なら 2 行目）。各列に入る実値の確認用。
                    let sample_row: Vec<String> = d.data_rows().first().cloned().unwrap_or_default();
                    egui::ScrollArea::vertical()
                        .max_height(260.0)
                        .auto_shrink([false, true])
                        .show(ui, |ui| {
                            egui::Grid::new("import_map_grid")
                                .num_columns(3)
                                .spacing([12.0, 6.0])
                                .striped(true)
                                .show(ui, |ui| {
                                    ui.label(egui::RichText::new("テーブル列").color(MUTED).small());
                                    ui.label(egui::RichText::new("CSV 列").color(MUTED).small());
                                    ui.label(
                                        egui::RichText::new("先頭行の値").color(MUTED).small(),
                                    );
                                    ui.end_row();
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
                                            .width(200.0)
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
                                        // 先頭行の実値（対応確認用）。
                                        let sample = match d.mapping[ci] {
                                            Some(src) => match sample_row.get(src) {
                                                Some(s) if s.is_empty() => "(空欄→NULL)".to_string(),
                                                Some(s) => {
                                                    let t: String = s.chars().take(24).collect();
                                                    if s.chars().count() > 24 {
                                                        format!("{t}…")
                                                    } else {
                                                        t
                                                    }
                                                }
                                                None => "(該当列なし)".to_string(),
                                            },
                                            None => "—".to_string(),
                                        };
                                        ui.label(
                                            egui::RichText::new(sample)
                                                .color(MUTED)
                                                .monospace()
                                                .small(),
                                        );
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

    fn kube_monitor_view(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("kube_status").show(ui, |ui| {
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
        egui::CentralPanel::default_margins().show(ui, |ui| {
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

    fn kube_events_view(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("kube_ev_bar").show(ui, |ui| {
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

        egui::CentralPanel::default_margins().show(ui, |ui| {
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
    fn kube_resource_view(&mut self, ui: &mut egui::Ui) {
        // 上部コントロール（種別・namespace・検索・更新）
        egui::Panel::top("kube_res_bar").show(ui, |ui| {
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

        egui::CentralPanel::default_margins().show(ui, |ui| {
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
                                        ui.close();
                                    }
                                    if ui.button("describe").clicked() {
                                        action =
                                            Some(RowAction::Describe(ns_opt.clone(), name.clone()));
                                        ui.close();
                                    }
                                    if ui.button("YAML を編集").clicked() {
                                        action =
                                            Some(RowAction::EditYaml(ns_opt.clone(), name.clone()));
                                        ui.close();
                                    }
                                    if kind == "pods" && ui.button("ログを追従").clicked() {
                                        action = Some(RowAction::Logs(
                                            row.namespace.clone(),
                                            name.clone(),
                                        ));
                                        ui.close();
                                    }
                                    if kind == "pods" && ui.button("コマンド実行 (exec)").clicked()
                                    {
                                        action = Some(RowAction::Exec(
                                            row.namespace.clone(),
                                            name.clone(),
                                        ));
                                        ui.close();
                                    }
                                    if matches!(kind.as_str(), "pods" | "services")
                                        && ui.button("port-forward").clicked()
                                    {
                                        let prefix = if kind == "services" { "svc" } else { "pod" };
                                        action = Some(RowAction::PortForward(
                                            row.namespace.clone(),
                                            format!("{prefix}/{name}"),
                                        ));
                                        ui.close();
                                    }
                                    if is_restartable(&kind)
                                        && ui.button("再起動 (rollout restart)").clicked()
                                    {
                                        action =
                                            Some(RowAction::Restart(ns_opt.clone(), name.clone()));
                                        ui.close();
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
                                                    ui.close();
                                                }
                                            }
                                        });
                                    }
                                    ui.separator();
                                    if ui.button(egui::RichText::new("削除").color(red)).clicked()
                                    {
                                        action =
                                            Some(RowAction::Delete(ns_opt.clone(), name.clone()));
                                        ui.close();
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
                        if let Some(p) =
                            project_list_ui(ui, &self.pick_projects, &self.pick_project)
                        {
                            // プロジェクト変更 → 下位をリセットして再取得。
                            cascade::select_project(
                                &mut self.pick_project,
                                &mut self.pick_instance,
                                &mut self.pick_database,
                                &mut self.pick_instances,
                                &mut self.pick_databases,
                                &p,
                            );
                            do_load_instances = Some(p);
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
                    let manual_ready = cascade::ready(
                        self.pick_project_filter.trim(),
                        self.pick_instance_manual.trim(),
                        self.pick_database_manual.trim(),
                    );
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
                        // 手動で project/instance を変えたので、トップバーの一覧は
                        // 古いプロジェクトのものになっている。クリアして取り直させる。
                        self.pick_instances.clear();
                        self.pick_databases.clear();
                        self.instances_loaded_for = None;
                        self.databases_loaded_for = None;
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
                    if ui.button("×").on_hover_text("検索をクリア").clicked() {
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

                let mut highlighter =
                    |ui: &egui::Ui, buf: &dyn egui::TextBuffer, wrap_width: f32| {
                        let job = highlight_job(buf.as_str(), &query, wrap_width);
                        ui.fonts_mut(|f| f.layout_job(job))
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
        // ウィンドウを閉じたら追従も止める（kubectl logs -f を残さない）。
        if !open {
            self.stop_logs();
        }
        self.kube_log_open = open;
    }

    fn kube_diagram_view(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("kube_topo_bar").show(ui, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("アーキテクチャ図").strong());
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
                                "{} ノード / {} 接続",
                                g.nodes.len(),
                                g.edges.len()
                            ))
                            .color(MUTED),
                        );
                    }
                }
                ui.separator();
                ui.label(
                    egui::RichText::new("Ingress → Service → Workload → Pod の通信フロー")
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
        egui::CentralPanel::default_margins().show(ui, |ui| {
            draw_topology(ui, g, kube_pan, kube_zoom, kube_selected);
        });
    }

    fn verify_view(&mut self, ui: &mut egui::Ui) {
        let mut open_local: Option<TableNode> = None;
        let mut open_gcs: Option<TableNode> = None;
        let mut do_run = false;
        let mut do_cancel = false;
        let mut back = false;
        let tables: Vec<TableNode> = self
            .schema_graph
            .as_ref()
            .filter(|g| g.error.is_none())
            .map(|g| g.nodes.clone())
            .unwrap_or_default();
        let running = self.verify_running;
        let progress = self.verify_progress;

        egui::CentralPanel::default_margins().show(ui, |ui| {
            ui.add_space(10.0);
            ui.heading("CSV ↔ DB 照合");
            ui.label(
                egui::RichText::new(
                    "主キーで突合し、各カラムの値まで比較します（一致 / 値差異 / CSVのみ / DBのみ）。",
                )
                .color(MUTED),
            );
            ui.add_space(12.0);

            if self.verify.is_none() {
                // ── ランディング（テーブル + ソース選択） ──
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("① テーブル").strong());
                    let sel_text = if self.verify_table_pick.is_empty() {
                        "選択…".to_string()
                    } else {
                        self.verify_table_pick.clone()
                    };
                    egui::ComboBox::from_id_salt("verify_table_pick")
                        .selected_text(sel_text)
                        .width(280.0)
                        .show_ui(ui, |ui| {
                            if tables.is_empty() {
                                ui.label("テーブルがありません（スキーマ未取得）");
                            }
                            for t in &tables {
                                ui.selectable_value(
                                    &mut self.verify_table_pick,
                                    t.name.clone(),
                                    &t.name,
                                );
                            }
                        });
                });
                let sel = tables
                    .iter()
                    .find(|t| t.name == self.verify_table_pick)
                    .cloned();
                ui.add_space(10.0);
                ui.label(egui::RichText::new("② 突合する CSV を選ぶ").strong());
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
                        "先にテーブルを選択してください。",
                    );
                }
            } else if let Some(d) = &mut self.verify {
                // ── マッピング + オプション + 実行 ──
                ui.horizontal(|ui| {
                    if ui.button("← 別の CSV / テーブルを選ぶ").clicked() {
                        back = true;
                    }
                    ui.label(
                        egui::RichText::new(format!("{}  ↔  {}", d.table, d.source_name)).strong(),
                    );
                });
                ui.add_space(8.0);

                // オプション行（変更時はプレビューを再パース）。
                let mut reparse = false;
                ui.horizontal(|ui| {
                    if ui.checkbox(&mut d.has_header, "先頭行はヘッダ").changed() {
                        reparse = true;
                    }
                    ui.separator();
                    ui.label("文字コード:");
                    egui::ComboBox::from_id_salt("verify_enc")
                        .selected_text(match d.encoding {
                            query::Encoding::Utf8 => "UTF-8",
                            query::Encoding::ShiftJis => "Shift-JIS",
                        })
                        .show_ui(ui, |ui| {
                            reparse |= ui
                                .selectable_value(&mut d.encoding, query::Encoding::Utf8, "UTF-8")
                                .changed();
                            reparse |= ui
                                .selectable_value(
                                    &mut d.encoding,
                                    query::Encoding::ShiftJis,
                                    "Shift-JIS",
                                )
                                .changed();
                        });
                    ui.label("区切り:");
                    egui::ComboBox::from_id_salt("verify_delim")
                        .selected_text(match d.delimiter {
                            b'\t' => "Tab",
                            b';' => ";",
                            _ => ",",
                        })
                        .show_ui(ui, |ui| {
                            reparse |=
                                ui.selectable_value(&mut d.delimiter, b',', ",").changed();
                            reparse |=
                                ui.selectable_value(&mut d.delimiter, b'\t', "Tab").changed();
                            reparse |=
                                ui.selectable_value(&mut d.delimiter, b';', ";").changed();
                        });
                    ui.separator();
                    ui.checkbox(&mut d.empty_as_null, "空欄=NULL")
                        .on_hover_text("空欄を NULL とみなして比較します（DB の NULL と一致）。");
                    ui.checkbox(&mut d.numeric_match, "数値の表記ゆれを無視")
                        .on_hover_text(
                            "005=5・5.0=5・+5=5・前後空白 を同じ値として突合します。\n\
                             STRING 列に数値 ID を入れていて、CSV 側がゼロ詰め/小数化\n\
                             しているときに有効。桁落ち（巨大 INT64 を表計算で丸めた等）は\n\
                             復元できません。",
                        );
                });
                if reparse {
                    d.reparse_preview();
                }
                ui.add_space(8.0);

                // マッピング表。テーブル列と mapping を同時に触らないよう列情報は複製する。
                ui.label(egui::RichText::new("列のマッピング（🔑 = 突合キー）").strong());
                ui.add_space(4.0);
                let headers = d.csv_headers.clone();
                let cols: Vec<(String, String, bool)> = d
                    .table_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.ty.clone(), c.pk))
                    .collect();
                egui::ScrollArea::vertical()
                    .max_height(260.0)
                    .auto_shrink([false, false])
                    .id_salt("verify_map_scroll")
                    .show(ui, |ui| {
                        egui::Grid::new("verify_map")
                            .striped(true)
                            .num_columns(3)
                            .spacing([12.0, 4.0])
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("テーブル列").color(MUTED));
                                ui.label(egui::RichText::new("型").color(MUTED));
                                ui.label(egui::RichText::new("CSV 列").color(MUTED));
                                ui.end_row();
                                for (i, (name, ty, pk)) in cols.iter().enumerate() {
                                    ui.horizontal(|ui| {
                                        if *pk {
                                            ui.label(egui::RichText::new("🔑").color(PK_COLOR));
                                        }
                                        ui.label(name);
                                    });
                                    ui.label(egui::RichText::new(ty).color(MUTED).small());
                                    let cur = d.mapping.get(i).copied().flatten();
                                    let sel_text = match cur {
                                        Some(idx) => headers
                                            .get(idx)
                                            .cloned()
                                            .unwrap_or_else(|| format!("列{}", idx + 1)),
                                        None => "（比較しない）".to_string(),
                                    };
                                    egui::ComboBox::from_id_salt(("verify_map_combo", i))
                                        .selected_text(sel_text)
                                        .width(240.0)
                                        .show_ui(ui, |ui| {
                                            let mut v = d.mapping.get(i).copied().flatten();
                                            if ui
                                                .selectable_label(v.is_none(), "（比較しない）")
                                                .clicked()
                                            {
                                                v = None;
                                            }
                                            for (h, hn) in headers.iter().enumerate() {
                                                if ui
                                                    .selectable_label(
                                                        v == Some(h),
                                                        format!("{} (列{})", hn, h + 1),
                                                    )
                                                    .clicked()
                                                {
                                                    v = Some(h);
                                                }
                                            }
                                            if let Some(slot) = d.mapping.get_mut(i) {
                                                *slot = v;
                                            }
                                        });
                                    ui.end_row();
                                }
                            });
                    });
                ui.add_space(10.0);

                // 実行 / 中断 + 進捗。
                ui.horizontal(|ui| {
                    if running {
                        ui.spinner();
                        let (phase, db, csv) = progress.unwrap_or(("実行中", 0, 0));
                        ui.label(format!(
                            "{phase}…  DB {} 行 / CSV {} 行",
                            fmt_count(db),
                            fmt_count(csv)
                        ));
                        if ui.button("⏹ 中断").clicked() {
                            do_cancel = true;
                        }
                    } else if ui
                        .add(egui::Button::new(egui::RichText::new("照合を実行").strong()))
                        .clicked()
                    {
                        do_run = true;
                    }
                });
                if let Some(m) = &d.config_msg {
                    ui.add_space(4.0);
                    ui.colored_label(egui::Color32::from_rgb(248, 113, 113), m);
                }
                if let Some(n) = &d.note {
                    ui.add_space(2.0);
                    ui.label(egui::RichText::new(n).color(MUTED).small());
                }
            }

            // ── 結果 ──
            if let Some(res) = &self.verify_result {
                ui.add_space(14.0);
                ui.separator();
                ui.add_space(6.0);
                verify_result_ui(ui, res, &mut self.verify_filter);
            }
        });

        if let Some(n) = open_local {
            self.open_verify_local(n);
        }
        if let Some(n) = open_gcs {
            self.open_verify_gcs(n);
        }
        if back {
            self.verify = None;
            self.verify_result = None;
        }
        if do_run {
            self.start_verify();
        }
        if do_cancel {
            self.verify_cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn plan_view(&mut self, ui: &mut egui::Ui) {
        let mut run = false;
        egui::Panel::top("plan_bar").show(ui, |ui| {
            ui.add_space(6.0);
            ui.label(
                egui::RichText::new("クエリの実行計画（EXPLAIN 相当・クエリは実行しません）")
                    .color(MUTED)
                    .small(),
            );
            ui.add_space(4.0);
            egui::TextEdit::multiline(&mut self.sql)
                .desired_rows(3)
                .desired_width(f32::INFINITY)
                .code_editor()
                .show(ui);
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!self.plan_pending, egui::Button::new("実行計画を取得"))
                    .clicked()
                {
                    run = true;
                }
                let cmd_enter =
                    ui.input(|i| i.modifiers.command && i.key_pressed(egui::Key::Enter));
                if cmd_enter && !self.plan_pending {
                    run = true;
                }
                if self.plan_pending {
                    ui.spinner();
                    ui.label(egui::RichText::new("取得中…").color(MUTED));
                }
                if let Some(p) = &self.plan_result {
                    if let Some(e) = &p.error {
                        ui.colored_label(egui::Color32::from_rgb(248, 113, 113), e);
                    } else {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} ノード · {} ms",
                                p.lines.len(),
                                p.elapsed_ms
                            ))
                            .color(MUTED)
                            .small(),
                        );
                    }
                }
            });
            ui.add_space(4.0);
        });

        egui::CentralPanel::default_margins().show(ui, |ui| {
            let Some(p) = &self.plan_result else {
                centered_hint(
                    ui,
                    "SQL を入力して「実行計画を取得」を押してください（実行はされません）",
                );
                return;
            };
            if p.error.is_some() {
                return; // エラーは上部に表示済み
            }
            if p.lines.is_empty() {
                centered_hint(ui, "実行計画が空です");
                return;
            }
            egui::ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.spacing_mut().item_spacing.y = 2.0;
                    for line in &p.lines {
                        ui.horizontal(|ui| {
                            ui.add_space(4.0 + line.depth as f32 * 18.0);
                            let marker = if line.scalar { "·" } else { "▸" };
                            let mcol = if line.scalar { MUTED } else { DIAGRAM_ACCENT };
                            ui.label(egui::RichText::new(marker).color(mcol).monospace());
                            let ncol = if line.scalar { MUTED } else { TEXT };
                            let name = egui::RichText::new(&line.name).color(ncol);
                            ui.label(if line.scalar { name } else { name.strong() });
                            if !line.detail.is_empty() {
                                ui.label(
                                    egui::RichText::new(&line.detail).color(MUTED).small(),
                                );
                            }
                        });
                    }
                });
        });

        if run {
            self.run_plan();
        }
    }

    fn schema_view(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("schema_bar").show(ui, |ui| {
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
                            .color(MUTED)
                            .small(),
                        );
                    }
                }
                ui.separator();
                legend(ui, DIAGRAM_ACCENT, "インターリーブ");
                legend(ui, CPU_COLOR, "外部キー");
                legend(ui, PK_COLOR, "PK");
                ui.separator();
                ui.label(
                    egui::RichText::new(
                        "ヘッダ: クリックで CREATE 文表示 / ドラッグで移動 ・ 行: クリックでコピー",
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

        let mut show_ddl: Option<String> = None;
        {
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
            egui::CentralPanel::default_margins().show(ui, |ui| {
                Self::draw_graph(
                    ui,
                    g,
                    node_positions,
                    selected,
                    diagram_pan,
                    diagram_zoom,
                    copy_note,
                    &mut show_ddl,
                );
            });
        }
        // テーブル名がクリックされたら CREATE 文ウィンドウを開く。
        if let Some(table) = show_ddl {
            self.open_ddl_view(&table);
        }
    }

    /// 指定テーブルの CREATE 文を組み立ててウィンドウを開く。
    /// 実 DDL（GetDatabaseDdl）があればそれを、無ければ近似 DDL を表示する。
    fn open_ddl_view(&mut self, table: &str) {
        let Some(g) = &self.schema_graph else { return };
        let ddl = g
            .ddl
            .get(table)
            .cloned()
            .or_else(|| g.nodes.iter().find(|n| n.name == table).map(build_ddl))
            .unwrap_or_else(|| format!("-- {table} の DDL を取得できませんでした"));
        self.ddl_view = Some((table.to_string(), ddl));
    }

    /// CREATE 文（DDL）ウィンドウ。コピー可能なテキストとして表示する。
    fn ddl_window(&mut self, ctx: &egui::Context) {
        let Some((table, ddl)) = self.ddl_view.clone() else {
            return;
        };
        let mut open = true;
        let mut copy = false;
        let mut close = false;
        egui::Window::new(format!("CREATE 文 — {table}"))
            .open(&mut open)
            .default_size([560.0, 420.0])
            .collapsible(false)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if ui.button("📋 コピー").clicked() {
                        copy = true;
                    }
                    if ui.button("閉じる").clicked() {
                        close = true;
                    }
                });
                ui.add_space(6.0);
                egui::ScrollArea::both().auto_shrink([false, false]).show(ui, |ui| {
                    // 等幅で選択コピーできるよう、編集不可の TextEdit を使う。
                    let mut text = ddl.clone();
                    ui.add(
                        egui::TextEdit::multiline(&mut text)
                            .font(egui::TextStyle::Monospace)
                            .desired_width(f32::INFINITY)
                            .desired_rows(18)
                            .code_editor()
                            .interactive(true),
                    );
                });
            });
        if copy {
            ctx.copy_text(ddl);
            self.copy_note = Some(format!("コピー: {table} の CREATE 文"));
        }
        if close || !open {
            self.ddl_view = None;
        }
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
        show_ddl: &mut Option<String>,
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
            let scroll = ui.input(|i| i.smooth_scroll_delta.y);
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
            ui.fonts_mut(|f| {
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
                EdgeKind::Interleave => DIAGRAM_ACCENT,
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
            let rounding = egui::CornerRadius::ZERO;
            painter.rect_filled(screen, rounding, dim(ELEVATED));
            let border = if is_sel {
                egui::Stroke::new(2.0, DIAGRAM_ACCENT)
            } else {
                egui::Stroke::new(1.0, dim(BORDER))
            };
            painter.rect_stroke(screen, rounding, border, egui::StrokeKind::Middle);

            // ヘッダ（ドラッグハンドル + 選択 + 右クリックメニュー）
            let header = egui::Rect::from_min_max(
                screen.min,
                egui::pos2(screen.max.x, screen.min.y + HEADER_H * z),
            );
            painter.rect_filled(header, rounding, dim(DIAGRAM_ACCENT.gamma_multiply(0.85)));
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
                // クリックで選択ハイライト + CREATE 文（DDL）を表示する。
                *selected = Some(node.name.clone());
                *show_ddl = Some(node.name.clone());
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
                if ui.button("CREATE 文を表示").clicked() {
                    *show_ddl = Some(name.clone());
                    ui.close();
                }
                if ui.button("テーブル名をコピー").clicked() {
                    ui.ctx().copy_text(name.clone());
                    *copy_note = Some(copied(&name));
                    ui.close();
                }
                if ui.button("カラム一覧をコピー").clicked() {
                    ui.ctx().copy_text(cols_joined.clone());
                    *copy_note = Some(format!("コピー: {name} のカラム"));
                    ui.close();
                }
                if !idx_joined.is_empty() && ui.button("インデックス一覧をコピー").clicked()
                {
                    ui.ctx().copy_text(idx_joined.clone());
                    *copy_note = Some(format!("コピー: {name} のインデックス"));
                    ui.close();
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
                        dim(DIAGRAM_ACCENT),
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
/// VS Code エクスプローラー風の1行（全幅・コンパクト・ホバーで全幅ハイライト）。
/// indent はテキスト開始位置、mono で等幅。返り値でクリック判定。
fn explorer_row(
    ui: &mut egui::Ui,
    indent: f32,
    text: &str,
    fg: egui::Color32,
    mono: bool,
) -> egui::Response {
    let h = 20.0;
    let w = ui.available_width();
    let (rect, resp) = ui.allocate_exact_size(egui::vec2(w, h), egui::Sense::click());
    let p = ui.painter();
    if resp.hovered() {
        p.rect_filled(rect, 0.0, LIST_HOVER);
    }
    let font = if mono {
        egui::FontId::monospace(12.0)
    } else {
        egui::FontId::proportional(13.0)
    };
    p.text(
        egui::pos2(rect.left() + indent, rect.center().y),
        egui::Align2::LEFT_CENTER,
        text,
        font,
        fg,
    );
    if resp.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }
    resp
}

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
    let (rect, resp) = ui.allocate_exact_size(egui::vec2(w, 42.0), egui::Sense::click());
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
        egui::Rect::from_center_size(rect.center(), egui::vec2(20.0, 20.0)),
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
    let outer = r.width() * 0.40;
    let inner = r.width() * 0.16;
    p.circle_stroke(c, outer * 0.6, stroke);
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
    let rad = r.width() * 0.40;
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
    let stroke = egui::Stroke::new(1.6, color);
    let cx = r.center().x;
    let cy = r.center().y;
    let half = r.height() * 0.40; // 全アイコン共通の半径目安（箱の約80%）
    let rx = r.width() * 0.34;
    let ry = r.height() * 0.11;
    let top = cy - half + ry;
    let bot = cy + half - ry;
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

/// アプリのマーク: CSV（表＋グリッド線）。
fn draw_csv_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    let stroke = egui::Stroke::new(1.6, color);
    let s = r.width() * 0.74; // 他アイコンと同等の見た目サイズに
    let rect = egui::Rect::from_center_size(r.center(), egui::vec2(s, s * 0.92));
    p.rect_stroke(rect, egui::CornerRadius::same(2), stroke, egui::StrokeKind::Inside);
    // 横線2本・縦線2本でセル感を出す。
    for f in [1.0 / 3.0, 2.0 / 3.0] {
        let y = rect.top() + rect.height() * f;
        p.line_segment([egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)], stroke);
        let x = rect.left() + rect.width() * f;
        p.line_segment([egui::pos2(x, rect.top()), egui::pos2(x, rect.bottom())], stroke);
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

/// 再起動後の再開用に保存するインポートジョブ（未完のみ）。
/// 実行時専用の cancel/id/進捗は持たず、再開に必要な要求内容だけを保存する。
#[derive(serde::Serialize, serde::Deserialize)]
struct SavedImportJob {
    table: String,
    columns: Vec<query::ImportColumn>,
    source: query::ImportSource,
    source_name: String,
    has_header: bool,
    mode: query::ImportMode,
    empty_as_null: bool,
    encoding: query::Encoding,
    delimiter: u8,
    skip_bad_rows: bool,
    null_token: Option<String>,
}

/// 未完ジョブの保存先（チェックポイントと同じ ~/.spanner-viewer/ 配下）。
fn import_jobs_file() -> std::path::PathBuf {
    let base = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(".spanner-viewer").join("import_jobs.json")
}

/// 未完（Done 以外）のジョブをディスクに保存する。空なら削除。
fn save_import_jobs(jobs: &[ImportJob]) {
    let saved: Vec<SavedImportJob> = jobs
        .iter()
        .filter(|j| j.status != JobStatus::Done)
        .map(|j| SavedImportJob {
            table: j.req.table.clone(),
            columns: j.req.columns.clone(),
            source: j.req.source.clone(),
            source_name: j.source_name.clone(),
            has_header: j.req.has_header,
            mode: j.req.mode,
            empty_as_null: j.req.empty_as_null,
            encoding: j.req.encoding,
            delimiter: j.req.delimiter,
            skip_bad_rows: j.req.skip_bad_rows,
            null_token: j.req.null_token.clone(),
        })
        .collect();
    let path = import_jobs_file();
    if saved.is_empty() {
        let _ = std::fs::remove_file(&path);
        return;
    }
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if let Ok(json) = serde_json::to_string_pretty(&saved) {
        let _ = std::fs::write(&path, json);
    }
}

fn load_import_jobs() -> Vec<SavedImportJob> {
    std::fs::read_to_string(import_jobs_file())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

/// 保存済みの未完ジョブを「中断」状態の ImportJob に復元する（id は連番で振り直す）。
fn restore_import_jobs() -> (Vec<ImportJob>, u64) {
    saved_jobs_to_import_jobs(load_import_jobs())
}

fn saved_jobs_to_import_jobs(saved: Vec<SavedImportJob>) -> (Vec<ImportJob>, u64) {
    let mut jobs = Vec::new();
    let mut next_id = 1u64;
    for s in saved {
        let req = query::ImportRequest {
            id: next_id,
            table: s.table,
            columns: s.columns,
            source: s.source,
            has_header: s.has_header,
            mode: s.mode,
            empty_as_null: s.empty_as_null,
            fresh: false, // チェックポイントから再開する
            encoding: s.encoding,
            delimiter: s.delimiter,
            skip_bad_rows: s.skip_bad_rows,
            dry_run: false,
            null_token: s.null_token,
            cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        jobs.push(ImportJob {
            req,
            source_name: s.source_name,
            sent: false,
            status: JobStatus::Cancelled, // プロセス終了で中断扱い
            started: None,
            progress: None,
            result: Some("前回終了で中断。「⟳ 再キュー」で続きから再開できます。".into()),
            outcome: None,
        });
        next_id += 1;
    }
    (jobs, next_id)
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
            .env("PATH", k8s::augmented_path())
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
/// k8s 通信フロー型アーキテクチャ図を描く。
/// 左→右に Ingress → Service → Workload → Pod の列で並べ、namespace ごとに枠で囲む。
/// 矢印は通信/所有の流れ。ドラッグでパン、ホイールでズーム、ノードクリックで選択。
fn draw_topology(
    ui: &mut egui::Ui,
    graph: Option<&k8s::ArchGraph>,
    pan: &mut egui::Vec2,
    zoom: &mut f32,
    selected: &mut Option<String>,
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
        centered_hint(ui, "リソースがありません");
        return;
    }

    let rect = ui.available_rect_before_wrap();
    let bg = ui.interact(rect, ui.id().with("arch_bg"), egui::Sense::click_and_drag());
    if bg.dragged() {
        *pan += bg.drag_delta();
    }
    if bg.clicked() {
        *selected = None;
    }
    if bg.hovered() {
        let scroll = ui.input(|i| i.smooth_scroll_delta.y);
        if scroll != 0.0 {
            let f = (1.0 + scroll * 0.0015).clamp(0.85, 1.18);
            *zoom = (*zoom * f).clamp(0.3, 3.0);
        }
    }
    let z = *zoom;
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, BASE);

    // ── レイアウト定数（ワールド座標） ──
    let nw = 188.0_f32;
    let nh = 48.0_f32;
    let col_gap = 64.0_f32;
    let row_gap = 14.0_f32;
    let ns_pad = 16.0_f32;
    let ns_head = 30.0_f32;
    let ns_gap = 34.0_f32;
    let col_x = |c: usize| ns_pad + c as f32 * (nw + col_gap);
    let inner_w = col_x(3) + nw + ns_pad;

    // namespace ごとにまとめる。
    use std::collections::BTreeMap;
    let mut by_ns: BTreeMap<String, Vec<&k8s::ArchNode>> = BTreeMap::new();
    for n in &graph.nodes {
        by_ns.entry(n.ns.clone()).or_default().push(n);
    }

    // 各ノードのワールド矩形と、namespace ブロック矩形を決める。
    let mut rects: std::collections::HashMap<String, egui::Rect> = Default::default();
    let mut blocks: Vec<(String, egui::Rect)> = Vec::new();
    let mut cursor_y = ns_pad + 22.0; // 列見出しのぶん少し下げる
    for (ns, nodes) in &by_ns {
        let mut col_counts = [0usize; 4];
        for n in nodes {
            col_counts[n.kind.column()] += 1;
        }
        let max_rows = col_counts.iter().copied().max().unwrap_or(1).max(1);
        let body_h = max_rows as f32 * nh + (max_rows as f32 - 1.0).max(0.0) * row_gap;
        let block_h = ns_head + ns_pad + body_h + ns_pad;
        let block_top = cursor_y;
        let mut next_row = [0usize; 4];
        for n in nodes {
            let c = n.kind.column();
            let x = col_x(c);
            let y = block_top + ns_head + ns_pad + next_row[c] as f32 * (nh + row_gap);
            next_row[c] += 1;
            rects.insert(
                n.id.clone(),
                egui::Rect::from_min_size(egui::pos2(x, y), egui::vec2(nw, nh)),
            );
        }
        blocks.push((
            ns.clone(),
            egui::Rect::from_min_size(
                egui::pos2(0.0, block_top),
                egui::vec2(inner_w, block_h),
            ),
        ));
        cursor_y = block_top + block_h + ns_gap;
    }

    // ワールド → スクリーン変換。
    let tf = |p: egui::Pos2| -> egui::Pos2 { rect.min + *pan + (p.to_vec2() * z) };
    let tr = |r: egui::Rect| -> egui::Rect { egui::Rect::from_min_max(tf(r.min), tf(r.max)) };

    // namespace ブロック枠。
    for (ns, br) in &blocks {
        painter.rect(
            tr(*br),
            egui::CornerRadius::same(6),
            ELEVATED,
            egui::Stroke::new(1.0, egui::Color32::from_gray(70)),
            egui::StrokeKind::Inside,
        );
        painter.text(
            tf(br.min + egui::vec2(10.0, 8.0)),
            egui::Align2::LEFT_TOP,
            format!("namespace: {ns}"),
            egui::FontId::proportional((12.0 * z).clamp(8.0, 20.0)),
            MUTED,
        );
    }

    // 列見出し。
    let headers = ["Ingress", "Service", "Workload", "Pod"];
    for (c, h) in headers.iter().enumerate() {
        let p = tf(egui::pos2(col_x(c) + nw * 0.5, 6.0));
        painter.text(
            p,
            egui::Align2::CENTER_TOP,
            *h,
            egui::FontId::proportional((11.0 * z).clamp(7.0, 18.0)),
            DIAGRAM_ACCENT,
        );
    }

    // エッジ（左→右の elbow + 右向き矢じり）。
    for (from, to) in &graph.edges {
        let (Some(a), Some(b)) = (rects.get(from), rects.get(to)) else {
            continue;
        };
        let p_from = tf(egui::pos2(a.max.x, a.center().y));
        let p_to = tf(egui::pos2(b.min.x, b.center().y));
        let midx = (p_from.x + p_to.x) * 0.5;
        let hi = selected.as_deref() == Some(from.as_str())
            || selected.as_deref() == Some(to.as_str());
        let col = if hi { DIAGRAM_ACCENT } else { egui::Color32::from_gray(110) };
        let stroke = egui::Stroke::new((1.5 * z).max(1.0), col);
        painter.add(egui::Shape::line(
            vec![
                p_from,
                egui::pos2(midx, p_from.y),
                egui::pos2(midx, p_to.y),
                p_to,
            ],
            stroke,
        ));
        let size = (8.0 * z).max(5.0);
        painter.add(egui::Shape::convex_polygon(
            vec![
                p_to,
                egui::pos2(p_to.x - size, p_to.y - size * 0.5),
                egui::pos2(p_to.x - size, p_to.y + size * 0.5),
            ],
            col,
            egui::Stroke::NONE,
        ));
    }

    // ノード。
    let kind_color = |k: k8s::ArchKind| match k {
        k8s::ArchKind::Ingress => egui::Color32::from_rgb(167, 139, 250),
        k8s::ArchKind::Service => DIAGRAM_ACCENT,
        k8s::ArchKind::Workload => egui::Color32::from_rgb(52, 211, 153),
        k8s::ArchKind::Pod => egui::Color32::from_rgb(125, 211, 252),
    };
    for n in &graph.nodes {
        let Some(r) = rects.get(&n.id) else {
            continue;
        };
        let s = tr(*r);
        let sel = selected.as_deref() == Some(n.id.as_str());
        let c = kind_color(n.kind);
        let resp = ui.interact(s, ui.id().with(("arch", &n.id)), egui::Sense::click());
        if resp.clicked() {
            *selected = Some(n.id.clone());
        }
        let bw = if sel || resp.hovered() { 2.0 } else { 1.0 };
        painter.rect(
            s,
            egui::CornerRadius::same(6),
            egui::Color32::from_gray(28),
            egui::Stroke::new(bw, c),
            egui::StrokeKind::Inside,
        );
        // 左の色帯。
        let bar = egui::Rect::from_min_max(
            s.min,
            egui::pos2(s.min.x + (4.0 * z).max(2.0), s.max.y),
        );
        painter.rect_filled(bar, egui::CornerRadius::same(6), c);
        let name_fs = (12.5 * z).clamp(7.0, 22.0);
        let sub_fs = (10.0 * z).clamp(6.0, 18.0);
        painter.text(
            tf(r.min + egui::vec2(10.0, 8.0)),
            egui::Align2::LEFT_TOP,
            &n.name,
            egui::FontId::proportional(name_fs),
            egui::Color32::from_gray(230),
        );
        painter.text(
            tf(r.min + egui::vec2(10.0, 28.0)),
            egui::Align2::LEFT_TOP,
            &n.sub,
            egui::FontId::proportional(sub_fs),
            MUTED,
        );
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
                                ui.close();
                            }
                            if ui.button("列の値をコピー").clicked() {
                                let vals: Vec<String> = order
                                    .iter()
                                    .map(|&r| {
                                        result.rows[r].get(col_idx).cloned().unwrap_or_default()
                                    })
                                    .collect();
                                ui.ctx().copy_text(vals.join("\n"));
                                ui.close();
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
                                    ui.close();
                                }
                                if ui.button("行をコピー (TSV)").clicked() {
                                    ui.ctx().copy_text(row.join("\t"));
                                    ui.close();
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

/// VS Code 風のタブを 1 つ描く。戻り値 (アクティブ化された, 閉じるが押された)。
/// アクティブはエディタ背景色＋上にアクセント線、非アクティブは暗いタブ色。
fn draw_vscode_tab(
    ui: &mut egui::Ui,
    title: &str,
    active: bool,
    loading: bool,
    idx: usize,
) -> (bool, bool) {
    let h = 34.0;
    let pad = 11.0;
    let close_box = 16.0;
    let font = egui::FontId::proportional(13.0);
    let label = if loading {
        format!("● {title}")
    } else {
        title.to_string()
    };
    // タイトル幅を実測してタブ幅を決める。
    let galley = ui
        .painter()
        .layout_no_wrap(label.clone(), font.clone(), egui::Color32::WHITE);
    let tab_w = pad + galley.size().x + 8.0 + close_box + 8.0;

    let (rect, resp) = ui.allocate_exact_size(egui::vec2(tab_w, h), egui::Sense::click());
    // 閉じる × の当たり判定（先に取って描画は後でまとめて行う）。
    let close_center = egui::pos2(rect.right() - pad - close_box * 0.5, rect.center().y);
    let close_rect = egui::Rect::from_center_size(close_center, egui::vec2(close_box, close_box));
    let close_resp = ui.interact(
        close_rect,
        ui.id().with(("vstab_close", idx)),
        egui::Sense::click(),
    );

    let painter = ui.painter();
    let bg = if active {
        BASE // エディタ背景 #1e1e1e
    } else if resp.hovered() {
        egui::Color32::from_rgb(50, 50, 50)
    } else {
        egui::Color32::from_rgb(45, 45, 45) // 非アクティブタブ #2d2d2d
    };
    painter.rect_filled(rect, 0.0, bg);
    if active {
        // 上端 2px のアクセント線。
        painter.rect_filled(
            egui::Rect::from_min_max(rect.min, egui::pos2(rect.right(), rect.top() + 2.0)),
            0.0,
            ACCENT,
        );
    }
    // 右側の区切り線。
    painter.vline(
        rect.right(),
        rect.y_range(),
        egui::Stroke::new(1.0, egui::Color32::from_gray(37)),
    );
    // タイトル。
    let tcol = if active {
        egui::Color32::from_gray(230)
    } else {
        egui::Color32::from_gray(150)
    };
    painter.text(
        egui::pos2(rect.left() + pad, rect.center().y),
        egui::Align2::LEFT_CENTER,
        &label,
        font,
        tcol,
    );
    // 閉じる ×（ホバーで丸い下地）。
    if close_resp.hovered() {
        painter.rect_filled(
            close_rect,
            egui::CornerRadius::same(3),
            egui::Color32::from_gray(75),
        );
    }
    let show_close = active || resp.hovered() || close_resp.hovered();
    if show_close {
        let xcol = if close_resp.hovered() {
            egui::Color32::from_gray(235)
        } else {
            egui::Color32::from_gray(160)
        };
        painter.text(
            close_center,
            egui::Align2::CENTER_CENTER,
            "×",
            egui::FontId::proportional(15.0),
            xcol,
        );
    }
    (resp.clicked() && !close_resp.clicked(), close_resp.clicked())
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
const BASE: egui::Color32 = egui::Color32::from_rgb(30, 30, 30); // エディタ #1e1e1e（グリッド等）
const PANEL: egui::Color32 = egui::Color32::from_rgb(37, 37, 38); // サイドバー/パネル #252526
const ELEVATED: egui::Color32 = egui::Color32::from_rgb(45, 45, 45); // ウィンドウ/ヘッダ #2d2d2d
const BORDER: egui::Color32 = egui::Color32::from_rgb(60, 60, 60); // 境界 #3c3c3c
const INPUT_BG: egui::Color32 = egui::Color32::from_rgb(60, 60, 60); // 入力 #3c3c3c
const ROW_ALT: egui::Color32 = egui::Color32::from_rgb(42, 42, 42); // 縞模様（控えめ）
const LIST_HOVER: egui::Color32 = egui::Color32::from_rgb(42, 45, 46); // 一覧ホバー #2a2d2e
const ACTIVITY_BG: egui::Color32 = egui::Color32::from_rgb(51, 51, 51); // アクティビティバー #333333
const STATUS_BG: egui::Color32 = egui::Color32::from_rgb(0, 122, 204); // ステータスバー #007acc
const BUTTON_BG: egui::Color32 = egui::Color32::from_rgb(14, 99, 156); // ボタン #0e639c

fn setup_style(ctx: &egui::Context) {
    use egui::FontFamily::{Monospace, Proportional};
    use egui::{CornerRadius, FontId, Stroke, TextStyle};

    let mut v = egui::Visuals::dark();
    v.override_text_color = Some(TEXT);
    v.panel_fill = PANEL;
    v.window_fill = ELEVATED;
    v.window_stroke = Stroke::new(1.0, BORDER);
    v.faint_bg_color = ROW_ALT;
    v.extreme_bg_color = INPUT_BG;
    v.code_bg_color = INPUT_BG;
    v.hyperlink_color = ACCENT;
    // 選択ハイライトは VS Code のリスト選択色 #264f78。
    v.selection.bg_fill = egui::Color32::from_rgb(38, 79, 120);
    v.selection.stroke = Stroke::new(1.0, ACCENT);

    // 角丸は小さめ（VS Code はほぼ角ばっている）・影は控えめ。
    v.window_corner_radius = CornerRadius::same(5);
    v.menu_corner_radius = CornerRadius::same(3);
    v.window_shadow = egui::epaint::Shadow {
        offset: [0, 4],
        blur: 16,
        spread: 0,
        color: egui::Color32::from_black_alpha(120),
    };
    v.popup_shadow = egui::epaint::Shadow {
        offset: [0, 2],
        blur: 10,
        spread: 0,
        color: egui::Color32::from_black_alpha(100),
    };

    // ウィジェット: 平らな #3c3c3c、ホバーで少し明るく、アクティブは VS Code 青。
    let round = CornerRadius::same(3);
    let w = &mut v.widgets;
    w.noninteractive.corner_radius = round;
    w.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    w.noninteractive.fg_stroke = Stroke::new(1.0, TEXT);

    w.inactive.corner_radius = round;
    w.inactive.weak_bg_fill = INPUT_BG;
    w.inactive.bg_fill = INPUT_BG;
    w.inactive.bg_stroke = Stroke::new(1.0, BORDER);
    w.inactive.fg_stroke = Stroke::new(1.0, TEXT);

    w.hovered.corner_radius = round;
    w.hovered.weak_bg_fill = egui::Color32::from_rgb(70, 70, 70);
    w.hovered.bg_fill = egui::Color32::from_rgb(70, 70, 70);
    w.hovered.bg_stroke = Stroke::new(1.0, egui::Color32::from_rgb(90, 90, 90));
    w.hovered.fg_stroke = Stroke::new(1.0, egui::Color32::from_rgb(240, 240, 240));
    w.hovered.expansion = 0.0;

    w.active.corner_radius = round;
    w.active.weak_bg_fill = BUTTON_BG;
    w.active.bg_fill = BUTTON_BG;
    w.active.bg_stroke = Stroke::new(1.0, ACCENT);
    w.active.fg_stroke = Stroke::new(1.0, egui::Color32::WHITE);
    w.active.expansion = 0.0;

    w.open.corner_radius = round;
    w.open.weak_bg_fill = egui::Color32::from_rgb(70, 70, 70);
    w.open.bg_fill = INPUT_BG;
    w.open.bg_stroke = Stroke::new(1.0, ACCENT);

    ctx.set_visuals(v);

    ctx.all_styles_mut(|s| {
        // VS Code に合わせる: UI は 13px、行間（縦）をかなり詰める。
        s.spacing.item_spacing = egui::vec2(6.0, 2.0);
        s.spacing.button_padding = egui::vec2(8.0, 2.0);
        s.spacing.interact_size.y = 18.0;
        s.spacing.window_margin = egui::Margin::same(8);
        s.spacing.menu_margin = egui::Margin::same(5);
        s.spacing.scroll.bar_width = 10.0;
        s.spacing.scroll.floating = false;
        s.text_styles = [
            // VS Code は大きな見出しを使わない。見出しも 13px（強調は bold）。
            (TextStyle::Heading, FontId::new(13.0, Proportional)),
            (TextStyle::Body, FontId::new(13.0, Proportional)),
            (TextStyle::Button, FontId::new(13.0, Proportional)),
            (TextStyle::Monospace, FontId::new(12.0, Monospace)),
            (TextStyle::Small, FontId::new(11.0, Proportional)),
        ]
        .into_iter()
        .collect();
    });
}

/// 日本語対応のシステムフォントを読み込み、既定フォントのフォールバックに追加する。
/// 見つからなければ何もしない（英数字は既定フォントで表示される）。
fn install_japanese_font(ctx: &egui::Context) {
    use std::sync::Arc;
    let mut fonts = egui::FontDefinitions::default();

    // 欧文・数字は VS Code と同じく macOS のシステムフォント San Francisco を最優先。
    // 無ければ Helvetica。
    let latin: [&str; 2] = [
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ];
    // 和文は macOS 標準のヒラギノ角ゴシック。
    let jp: [&str; 3] = [
        "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    ];
    // 等幅は VS Code の既定 Menlo（無ければ SF Mono）。
    let mono: [&str; 2] = [
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/SFNSMono.ttf",
    ];

    // 欧文・和文ともヒラギノ角ゴシックで統一する。SF Pro と 2 フォント混在にすると
    // 欧文が和文より小さく/細く見えて「英語と漢字の高さが違う」不揃いになるため、
    // 設計上調和した単一フォントにする。SF はヒラギノに無いグリフ用のフォールバック。
    // 単一フォント前提なので欧文/和文の相互ベースラインずれは原理的に発生しない。
    // y_offset はボタン等での上下中央寄せの微調整（render_font_alignment_check で確認）。
    const PROP_Y_OFFSET: f32 = 0.06;
    let mut front: Vec<String> = Vec::new();
    let have_jp = if let Some(b) = jp.iter().find_map(|p| std::fs::read(p).ok()) {
        let data = egui::FontData::from_owned(b).tweak(egui::FontTweak {
            y_offset_factor: PROP_Y_OFFSET,
            ..Default::default()
        });
        fonts.font_data.insert("jp".to_owned(), Arc::new(data));
        front.push("jp".to_owned());
        true
    } else {
        false
    };
    if let Some(b) = latin.iter().find_map(|p| std::fs::read(p).ok()) {
        let data = egui::FontData::from_owned(b).tweak(egui::FontTweak {
            y_offset_factor: PROP_Y_OFFSET,
            ..Default::default()
        });
        fonts.font_data.insert("latin".to_owned(), Arc::new(data));
        front.push("latin".to_owned()); // ヒラギノの後ろ＝フォールバック
    }
    let have_mono = if let Some(b) = mono.iter().find_map(|p| std::fs::read(p).ok()) {
        fonts
            .font_data
            .insert("mono".to_owned(), Arc::new(egui::FontData::from_owned(b)));
        true
    } else {
        false
    };

    let prop = fonts
        .families
        .entry(egui::FontFamily::Proportional)
        .or_default();
    for (i, name) in front.iter().enumerate() {
        prop.insert(i, name.clone());
    }
    // 等幅は Menlo を主にし、和文はヒラギノで補う。
    let monof = fonts
        .families
        .entry(egui::FontFamily::Monospace)
        .or_default();
    if have_mono {
        monof.insert(0, "mono".to_owned());
    }
    if have_jp {
        monof.push("jp".to_owned());
    }
    ctx.set_fonts(fonts);
}

/// ステータスバー用の小さなカラーチップ（ラベル + 値）。
fn chip(ui: &mut egui::Ui, label: &str, value: &str, color: egui::Color32) {
    egui::Frame::NONE
        .fill(ELEVATED)
        .corner_radius(egui::CornerRadius::same(7))
        .inner_margin(egui::Margin::symmetric(10, 4))
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

/// 並列インポートのディスパッチ判定。実行中テーブル集合 `running` に対し、
/// `table` を新たに動かしてよいか。別テーブルなら並列可・同一テーブルは直列・
/// 同時実行は `max` まで。
fn can_dispatch(
    running: &std::collections::HashSet<String>,
    table: &str,
    max: usize,
) -> bool {
    running.len() < max && !running.contains(table)
}

/// 取込中の総件数を推定する（書込済 行数 × 全体バイト / 読込済バイト）。
/// バイト情報が無い / まだ読めていないときは None。推定は最低でも written 以上にする。
fn import_total_estimate(p: &ImportProg) -> Option<usize> {
    let total = p.bytes_total?;
    if p.bytes_done == 0 || total == 0 || p.written == 0 {
        return None;
    }
    let est = (p.written as f64) * (total as f64) / (p.bytes_done as f64);
    Some((est.round() as usize).max(p.written))
}

/// 取込の進捗バーを自前で描画する（egui の ProgressBar はトラック色が見分けに
/// くく "常に満タンに見える" ため）。frac=None はインデターミネート（往復する帯）。
fn paint_import_bar(ui: &mut egui::Ui, frac: Option<f32>, text: &str) {
    let h = 20.0;
    let w = ui.available_width().max(40.0);
    let (rect, _) = ui.allocate_exact_size(egui::vec2(w, h), egui::Sense::hover());
    if !ui.is_rect_visible(rect) {
        return;
    }
    let painter = ui.painter();
    let r = egui::CornerRadius::same(4);
    // トラック（未充填）はハッキリ暗いグレーにして充填部と区別する。
    painter.rect_filled(rect, r, egui::Color32::from_gray(58));
    match frac {
        Some(f) => {
            let fw = rect.width() * f.clamp(0.0, 1.0);
            if fw > 1.0 {
                let fill = egui::Rect::from_min_size(rect.min, egui::vec2(fw, rect.height()));
                painter.rect_filled(fill, r, ACCENT);
            }
        }
        None => {
            // 30% 幅の帯を左右に往復させる（総量不明時）。
            let t = ui.input(|i| i.time);
            let seg = rect.width() * 0.3;
            let span = (rect.width() - seg).max(0.0);
            let phase = (t * 0.6).rem_euclid(1.0) as f32; // 0..1
            let tri = 1.0 - (2.0 * phase - 1.0).abs(); // 0→1→0
            let x = rect.left() + span * tri;
            let fill =
                egui::Rect::from_min_size(egui::pos2(x, rect.top()), egui::vec2(seg, rect.height()));
            painter.rect_filled(fill, r, ACCENT.gamma_multiply(0.85));
            ui.ctx().request_repaint();
        }
    }
    painter.text(
        egui::pos2(rect.left() + 8.0, rect.center().y),
        egui::Align2::LEFT_CENTER,
        text,
        egui::FontId::proportional(12.0),
        egui::Color32::WHITE,
    );
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

/// 照合結果（サマリーカード + 不一致サンプル一覧）を描画する。
fn verify_result_ui(
    ui: &mut egui::Ui,
    res: &query::VerifyOutcome,
    filter: &mut Option<query::VerifyKind>,
) {
    use query::VerifyKind;
    if let Some(e) = &res.error {
        ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
        return;
    }
    ui.heading("照合結果");
    ui.add_space(6.0);
    ui.horizontal_wrapped(|ui| {
        verify_stat(ui, "一致", res.matched, egui::Color32::from_rgb(74, 222, 128));
        verify_stat(
            ui,
            "値差異",
            res.value_mismatch,
            egui::Color32::from_rgb(251, 191, 36),
        );
        verify_stat(
            ui,
            "CSVのみ",
            res.csv_only,
            egui::Color32::from_rgb(96, 165, 250),
        );
        verify_stat(
            ui,
            "DBのみ",
            res.db_only,
            egui::Color32::from_rgb(248, 113, 113),
        );
    });
    ui.add_space(6.0);
    let perfect = res.value_mismatch == 0 && res.csv_only == 0 && res.db_only == 0;
    if perfect {
        ui.colored_label(
            egui::Color32::from_rgb(74, 222, 128),
            "✓ 完全一致（CSV と DB のレコードは同一です）",
        );
    } else {
        ui.colored_label(
            egui::Color32::from_rgb(251, 191, 36),
            "✗ 差分があります（下の一覧で詳細を確認）",
        );
    }
    let extra = format!(
        "{}{}",
        if res.csv_dup > 0 {
            format!(" / CSV内PK重複 {}", fmt_count(res.csv_dup))
        } else {
            String::new()
        },
        if res.db_truncated { " / DB打切あり" } else { "" },
    );
    ui.label(
        egui::RichText::new(format!(
            "CSV {} 行 / DB {} 行{extra}",
            fmt_count(res.csv_rows),
            fmt_count(res.db_rows),
        ))
        .color(MUTED)
        .small(),
    );
    if let Some(n) = &res.note {
        ui.label(egui::RichText::new(n).color(MUTED).small());
    }
    if res.samples.is_empty() {
        return;
    }
    ui.add_space(8.0);
    ui.horizontal(|ui| {
        ui.label("表示:");
        if ui.selectable_label(filter.is_none(), "すべて").clicked() {
            *filter = None;
        }
        if ui
            .selectable_label(*filter == Some(VerifyKind::ValueMismatch), "値差異")
            .clicked()
        {
            *filter = Some(VerifyKind::ValueMismatch);
        }
        if ui
            .selectable_label(*filter == Some(VerifyKind::CsvOnly), "CSVのみ")
            .clicked()
        {
            *filter = Some(VerifyKind::CsvOnly);
        }
        if ui
            .selectable_label(*filter == Some(VerifyKind::DbOnly), "DBのみ")
            .clicked()
        {
            *filter = Some(VerifyKind::DbOnly);
        }
    });
    ui.add_space(4.0);
    let active = *filter;
    egui::ScrollArea::vertical()
        .max_height(260.0)
        .auto_shrink([false, false])
        .id_salt("verify_samples")
        .show(ui, |ui| {
            egui::Grid::new("verify_sample_grid")
                .striped(true)
                .num_columns(3)
                .spacing([12.0, 3.0])
                .show(ui, |ui| {
                    ui.label(egui::RichText::new("種別").color(MUTED));
                    ui.label(egui::RichText::new("主キー").color(MUTED));
                    ui.label(egui::RichText::new("詳細").color(MUTED));
                    ui.end_row();
                    for s in res
                        .samples
                        .iter()
                        .filter(|s| active.is_none_or(|f| f == s.kind))
                    {
                        let (lbl, c) = match s.kind {
                            VerifyKind::ValueMismatch => {
                                ("値差異", egui::Color32::from_rgb(251, 191, 36))
                            }
                            VerifyKind::CsvOnly => {
                                ("CSVのみ", egui::Color32::from_rgb(96, 165, 250))
                            }
                            VerifyKind::DbOnly => {
                                ("DBのみ", egui::Color32::from_rgb(248, 113, 113))
                            }
                        };
                        ui.label(egui::RichText::new(lbl).color(c));
                        ui.label(egui::RichText::new(&s.key).monospace());
                        ui.label(egui::RichText::new(&s.detail).monospace().small());
                        ui.end_row();
                    }
                });
        });
    if res.samples_truncated {
        ui.label(
            egui::RichText::new(format!(
                "（サンプルは最大 {} 件まで。総数は上のカウントを参照）",
                fmt_count(res.samples.len())
            ))
            .color(MUTED)
            .small(),
        );
    }
}

/// 照合サマリーの 1 カード（ラベル + 件数）。
fn verify_stat(ui: &mut egui::Ui, label: &str, n: usize, color: egui::Color32) {
    egui::Frame::NONE
        .fill(ELEVATED)
        .corner_radius(egui::CornerRadius::same(6))
        .inner_margin(egui::Margin::symmetric(12, 6))
        .show(ui, |ui| {
            ui.vertical(|ui| {
                ui.label(egui::RichText::new(label).color(MUTED).small());
                ui.label(egui::RichText::new(fmt_count(n)).color(color).size(18.0).strong());
            });
        });
}

/// 未割当の主キー列名を返す（空なら OK）。
/// SQL 補完で使うキーワード。
const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN",
    "ORDER BY", "GROUP BY", "HAVING", "LIMIT", "OFFSET", "JOIN", "LEFT JOIN", "INNER JOIN",
    "ON", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "ASC", "DESC", "UNION",
    "WITH", "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM", "CREATE TABLE",
    "CREATE INDEX", "ALTER TABLE", "DROP TABLE", "PRIMARY KEY", "TRUE", "FALSE",
    "CURRENT_TIMESTAMP", "TIMESTAMP", "STRING", "INT64", "BOOL", "NUMERIC", "FLOAT64",
];

/// SQL 補完候補（前方一致・大小無視）。テーブル名→カラム名→キーワードの順、最大 max 件。
fn sql_completions(word: &str, tables: &[String], columns: &[String], max: usize) -> Vec<String> {
    let w = word.to_lowercase();
    if w.is_empty() {
        return Vec::new();
    }
    let kw: Vec<String> = SQL_KEYWORDS.iter().map(|s| s.to_string()).collect();
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for cand in tables.iter().chain(columns.iter()).chain(kw.iter()) {
        if out.len() >= max {
            break;
        }
        let cl = cand.to_lowercase();
        if cl.starts_with(&w) && cl != w && seen.insert(cl) {
            out.push(cand.clone());
        }
    }
    out
}

/// SQL 補完を適用する。`range`(バイト) を `候補 + 空白` で置換し、確定後のカーソル
/// 文字位置を `set_cursor` に入れる（次フレームで TextEdit に反映）。
fn apply_sql_completion(
    sql: &mut String,
    range: (usize, usize),
    cand: &str,
    set_cursor: &mut Option<usize>,
) {
    let (s, e) = range;
    if s > e || e > sql.len() || !sql.is_char_boundary(s) || !sql.is_char_boundary(e) {
        return;
    }
    let rep = format!("{cand} ");
    sql.replace_range(s..e, &rep);
    let new_byte = s + rep.len();
    *set_cursor = Some(sql[..new_byte].chars().count());
}

/// カーソル（文字位置）直前の識別子トークンのバイト範囲を返す。
fn current_word_range(text: &str, char_idx: usize) -> (usize, usize) {
    let byte_idx = text
        .char_indices()
        .nth(char_idx)
        .map(|(b, _)| b)
        .unwrap_or(text.len());
    let before = &text[..byte_idx];
    let start = before
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
        .last()
        .map(|(b, _)| b)
        .unwrap_or(byte_idx);
    (start, byte_idx)
}

/// 取り込み/照合のマッピング初期値を作る。
/// ヘッダ有り: テーブル列名 ↔ CSV 見出しを大小無視で対応付け（一致しなければ None）。
/// ヘッダ無し: 位置で対応（テーブル列 i ← CSV 列 i。CSV 列が足りなければ None）。
fn auto_mapping(
    table_columns: &[query::Column],
    csv_headers: &[String],
    has_header: bool,
    ncols: usize,
) -> Vec<Option<usize>> {
    if has_header {
        let lower: Vec<String> = csv_headers.iter().map(|h| h.trim().to_lowercase()).collect();
        table_columns
            .iter()
            .map(|c| {
                let name = c.name.trim().to_lowercase();
                lower.iter().position(|h| *h == name)
            })
            .collect()
    } else {
        // ヘッダ無しは「列1, 列2, …」を順番にテーブル列へ割り当てる（スキップにしない）。
        (0..table_columns.len())
            .map(|i| if i < ncols { Some(i) } else { None })
            .collect()
    }
}

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
/// 方式ラベル（挿入のみ / 上書き挿入）。
fn mode_label(mode: query::ImportMode) -> &'static str {
    match mode {
        query::ImportMode::Insert => "挿入のみ",
        query::ImportMode::InsertOrUpdate => "上書き挿入",
    }
}

/// 証跡レポート（Markdown）を組み立てる。
fn report_markdown(jobs: &[ImportJob], ts: &chrono::DateTime<chrono::Local>) -> String {
    let mut s = String::new();
    s.push_str("# インポート証跡レポート\n\n");
    s.push_str(&format!("生成日時: {}\n\n", ts.format("%Y-%m-%d %H:%M:%S")));
    // 完了したジョブだけを対象にする（待機中/失敗中の 0 行行を混ぜない）。
    let done: Vec<&ImportJob> = jobs
        .iter()
        .filter(|j| j.status == JobStatus::Done && j.outcome.is_some())
        .collect();
    s.push_str(
        "| テーブル | 方式 | CSV行数 | 取込前 | 取込後 | 新規挿入 | 更新(推定) | スキップ | エラー |\n",
    );
    s.push_str("|---|---|--:|--:|--:|--:|--:|--:|---|\n");
    let mut total_written = 0usize;
    for j in &done {
        let o = j.outcome.as_ref().unwrap();
        total_written += o.written;
        let mode = if o.dry_run {
            "検証(ドライラン)".to_string()
        } else {
            mode_label(j.req.mode).to_string()
        };
        // before/after が取れていれば 新規挿入=after-before、更新=書込-新規（>=0）。
        let (inserted, updated) = match (o.before_count, o.after_count) {
            (Some(b), Some(a)) => {
                let ins = (a - b).max(0);
                let upd = (o.written as i64 - ins).max(0);
                (fmt_count(ins as usize), fmt_count(upd as usize))
            }
            _ => ("-".to_string(), "-".to_string()),
        };
        let cell = |v: Option<i64>| v.map(|n| fmt_count(n as usize)).unwrap_or_else(|| "-".into());
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            j.req.table,
            mode,
            fmt_count(o.total),
            cell(o.before_count),
            cell(o.after_count),
            inserted,
            updated,
            fmt_count(o.skipped),
            o.error.clone().unwrap_or_else(|| "-".into()),
        ));
    }
    s.push_str(&format!(
        "\n対象: 完了 {} ジョブ / 書込 {} 行\n",
        done.len(),
        fmt_count(total_written)
    ));
    s.push_str(
        "\n※ CSV行数=入力データ行数、書込=BatchWrite 成功行数。\n\
         「新規挿入/更新」は取込前後の COUNT(*) 差分からの推定です\n\
         （同時書き込みや CSV 内の重複キーがあると正確でない場合があります）。\n",
    );
    s
}

/// 証跡レポート（CSV）を組み立てる。
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
/// プロジェクト一覧を selectable で描画し、クリックされた項目を返す。
/// 実 UI（設定のプルダウン内）と egui_kittest の操作テストで同じ関数を共有する。
fn project_list_ui(ui: &mut egui::Ui, projects: &[String], selected: &str) -> Option<String> {
    let mut clicked = None;
    for p in projects {
        if ui.selectable_label(selected == p, p).clicked() {
            clicked = Some(p.clone());
        }
    }
    clicked
}

fn combo_text(val: &str, placeholder: &str) -> String {
    if val.is_empty() {
        format!("{placeholder}…")
    } else {
        val.to_string()
    }
}

/// 接続先カスケード選択の状態遷移（描画から切り離して単体テストするための純ロジック）。
///
/// 「プロジェクトを選ぶと instance/DB がリセットされる」「3 つ揃って初めて接続できる」
/// といった規則をここに集約し、UI はこれを呼ぶだけにする。
mod cascade {
    /// プロジェクト選択時: 値を更新し、下位（instance/DB と各候補一覧）を消す。
    pub fn select_project(
        project: &mut String,
        instance: &mut String,
        database: &mut String,
        instances: &mut Vec<String>,
        databases: &mut Vec<String>,
        chosen: &str,
    ) {
        chosen.clone_into(project);
        instance.clear();
        database.clear();
        instances.clear();
        databases.clear();
    }

    /// インスタンス選択時: 値を更新し、DB と DB 候補一覧を消す。
    pub fn select_instance(
        instance: &mut String,
        database: &mut String,
        databases: &mut Vec<String>,
        chosen: &str,
    ) {
        chosen.clone_into(instance);
        database.clear();
        databases.clear();
    }

    /// project/instance/database が 3 つとも空でなければ接続を適用できる。
    pub fn ready(project: &str, instance: &str, database: &str) -> bool {
        !project.is_empty() && !instance.is_empty() && !database.is_empty()
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

    /// プロジェクトを選ぶと instance/DB と各候補一覧がリセットされる。
    #[test]
    fn cascade_select_project_resets_lower() {
        let mut project = "old-proj".to_string();
        let mut instance = "old-inst".to_string();
        let mut database = "old-db".to_string();
        let mut instances = vec!["i1".to_string(), "i2".to_string()];
        let mut databases = vec!["d1".to_string()];

        cascade::select_project(
            &mut project,
            &mut instance,
            &mut database,
            &mut instances,
            &mut databases,
            "new-proj",
        );

        assert_eq!(project, "new-proj");
        assert!(instance.is_empty(), "instance はクリアされる");
        assert!(database.is_empty(), "database はクリアされる");
        assert!(instances.is_empty(), "instance 候補はクリアされる");
        assert!(databases.is_empty(), "database 候補はクリアされる");
    }

    /// インスタンスを選ぶと DB と DB 候補だけがリセットされる（project は保持）。
    #[test]
    fn cascade_select_instance_resets_database_only() {
        let mut instance = "old-inst".to_string();
        let mut database = "old-db".to_string();
        let mut databases = vec!["d1".to_string(), "d2".to_string()];

        cascade::select_instance(&mut instance, &mut database, &mut databases, "new-inst");

        assert_eq!(instance, "new-inst");
        assert!(database.is_empty());
        assert!(databases.is_empty());
    }

    /// 3 つ揃ったときだけ接続適用できる。
    #[test]
    fn cascade_ready_requires_all_three() {
        assert!(cascade::ready("p", "i", "d"));
        assert!(!cascade::ready("", "i", "d"));
        assert!(!cascade::ready("p", "", "d"));
        assert!(!cascade::ready("p", "i", ""));
        assert!(!cascade::ready("", "", ""));
    }

    /// 標準の Context（run_ui）でヘッドレスにクリックを再現し、project_list_ui が
    /// クリックされた項目を返すことを確認する（egui_kittest を使う版は別テスト）。
    #[test]
    fn project_list_ui_click_returns_item() {
        use egui::{Event, PointerButton, Pos2, Rect};

        let ctx = egui::Context::default();
        let projects = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
        ];
        let selected = String::new();
        let screen = Rect::from_min_size(Pos2::ZERO, egui::vec2(400.0, 400.0));
        let base_input = || egui::RawInput {
            screen_rect: Some(screen),
            ..Default::default()
        };

        // パス0: レイアウトして "beta" の矩形（クリック座標）を得る。
        let beta_rect = std::cell::Cell::new(None);
        let _ = ctx.run_ui(base_input(), |ui| {
            egui::CentralPanel::default_margins().show(ui, |ui| {
                for p in &projects {
                    let r = ui.selectable_label(false, p);
                    if p == "beta" {
                        beta_rect.set(Some(r.rect));
                    }
                }
            });
        });
        let pos = beta_rect.get().expect("beta の矩形").center();

        // パス1: ポインタを重ねて押下。
        let mut down = base_input();
        down.events.push(Event::PointerMoved(pos));
        down.events.push(Event::PointerButton {
            pos,
            button: PointerButton::Primary,
            pressed: true,
            modifiers: Default::default(),
        });
        let _ = ctx.run_ui(down, |ui| {
            egui::CentralPanel::default_margins().show(ui, |ui| {
                let _ = project_list_ui(ui, &projects, &selected);
            });
        });

        // パス2: 離してクリック確定。返り値を捕捉する。
        let clicked = std::cell::RefCell::new(None);
        let mut up = base_input();
        up.events.push(Event::PointerButton {
            pos,
            button: PointerButton::Primary,
            pressed: false,
            modifiers: Default::default(),
        });
        let _ = ctx.run_ui(up, |ui| {
            egui::CentralPanel::default_margins().show(ui, |ui| {
                if let Some(p) = project_list_ui(ui, &projects, &selected) {
                    *clicked.borrow_mut() = Some(p);
                }
            });
        });

        assert_eq!(
            clicked.into_inner().as_deref(),
            Some("beta"),
            "クリックした項目が返るべき"
        );
    }

    /// egui_kittest でヘッドレスに描画し、ラベル "beta" をクリックして
    /// project_list_ui がその項目を返すことを確認する。
    #[test]
    fn project_list_ui_kittest_click() {
        use egui_kittest::kittest::Queryable;
        use egui_kittest::Harness;

        let projects = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
        ];
        let selected = String::new();
        let clicked: std::cell::RefCell<Option<String>> = std::cell::RefCell::new(None);

        let mut harness = Harness::new_ui(|ui| {
            if let Some(p) = project_list_ui(ui, &projects, &selected) {
                *clicked.borrow_mut() = Some(p);
            }
        });

        harness.get_by_label("beta").click();
        harness.run();

        assert_eq!(clicked.borrow().as_deref(), Some("beta"));
    }

    /// E2E: 実アプリ（MonitorApp）を kittest で動かし、実際の import_loop を
    /// 介してエミュレータ Spanner へ CSV を取り込み、行が入ることを確認する。
    /// UI のジョブキュー → チャネル → 背景ループ → Spanner → チャネル → UI まで通す。
    /// emulator が必要なので既定では実行しない（GPU も要るため #[ignore]）。
    /// 単独実行推奨: `cargo test --ignored e2e_import_pipeline -- --test-threads=1`
    #[ignore = "emulator + GPU が必要。E2E は単独で実行する"]
    #[test]
    fn e2e_import_pipeline() {
        use egui_kittest::Harness;
        if std::env::var("SPANNER_EMULATOR_HOST").is_err() {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        }
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let project = std::env::var("SPANNER_PROJECT").unwrap();
        let instance = std::env::var("SPANNER_INSTANCE").unwrap();
        let database = std::env::var("SPANNER_DATABASE").unwrap();
        let db = format!("projects/{project}/instances/{instance}/databases/{database}");
        let table = "E2EImport";

        fn block<F: std::future::Future>(f: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f)
        }

        // 1) テーブル作成＋既存行クリア（DDL は admin クライアントで）。
        block(async {
            use gcloud_spanner::admin::client::Client as AdminClient;
            use gcloud_spanner::admin::AdminClientConfig;
            use gcloud_spanner::client::{Client, ClientConfig};
            use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
            let cfg = AdminClientConfig::default().with_auth().await.unwrap();
            let admin = AdminClient::new(cfg).await.unwrap();
            let req = UpdateDatabaseDdlRequest {
                database: db.clone(),
                statements: vec![format!(
                    "CREATE TABLE IF NOT EXISTS {table} \
                     (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)"
                )],
                ..Default::default()
            };
            admin
                .database()
                .update_database_ddl(req, None)
                .await
                .unwrap()
                .wait(None)
                .await
                .unwrap();
            let c = Client::new(&db, ClientConfig::default().with_auth().await.unwrap())
                .await
                .unwrap();
            let _ = c
                .apply(vec![gcloud_spanner::mutation::delete(
                    table,
                    gcloud_spanner::key::all_keys(),
                )])
                .await;
        });

        // 2) 実 import_loop を背景ランタイムで起動。
        let (import_req_tx, import_req_rx) = tokio::sync::mpsc::unbounded_channel();
        let (import_res_tx, import_res_rx) = std::sync::mpsc::channel();
        let cfg = query::Config {
            project,
            instance,
            database,
            mock: false,
        };
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(query::import_loop(cfg, import_req_rx, import_res_tx));
        });

        // 3) 実 MonitorApp を kittest で構築（import チャネルだけ実物に差し替え）。
        let mut ch = make_test_channels();
        ch.import_req_tx = import_req_tx;
        ch.import_res_rx = import_res_rx;
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1000.0, 720.0))
            .build_eframe(|cc| MonitorApp::new(ch, cc));

        // 4) CSV を用意し、アプリのジョブキューに投入（UI 経由のディスパッチ）。
        let csv = "Id,Name\n1,alice\n2,bob\n3,carol\n";
        let path = std::env::temp_dir().join("spanner_viewer_e2e.csv");
        std::fs::write(&path, csv).unwrap();
        let req = query::ImportRequest {
            id: 0,
            table: table.into(),
            columns: vec![
                query::ImportColumn { name: "Id".into(), ty: "INT64".into(), src_index: 0 },
                query::ImportColumn {
                    name: "Name".into(),
                    ty: "STRING(MAX)".into(),
                    src_index: 1,
                },
            ],
            source: query::ImportSource::File(path),
            has_header: true,
            mode: query::ImportMode::Insert,
            empty_as_null: true,
            fresh: true,
            encoding: query::Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        harness.state_mut().push_job(req, "e2e.csv".into());

        // 5) ジョブが完了するまでフレームを回す（drain がチャネルを取り込む）。
        // run() は安定するまで回ろうとして、取込中の再描画で収束せず panic するので
        // step()（1 フレーム進める）を使う。
        let mut status = None;
        for _ in 0..200 {
            harness.step();
            status = harness.state().import_jobs.first().map(|j| j.status);
            if matches!(status, Some(JobStatus::Done) | Some(JobStatus::Failed)) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(status, Some(JobStatus::Done), "ジョブが完了するはず");
        let written = harness
            .state()
            .import_jobs
            .first()
            .and_then(|j| j.outcome.as_ref())
            .map(|o| o.written);
        assert_eq!(written, Some(3), "3 行書き込まれるはず");

        // 6) エミュレータを直接読んで実データを検証。
        let n: i64 = block(async {
            use gcloud_spanner::client::{Client, ClientConfig};
            use gcloud_spanner::statement::Statement;
            let c = Client::new(&db, ClientConfig::default().with_auth().await.unwrap())
                .await
                .unwrap();
            let mut tx = c.single().await.unwrap();
            let mut it = tx
                .query(Statement::new(format!("SELECT COUNT(*) FROM {table}")))
                .await
                .unwrap();
            let row = it.next().await.unwrap().unwrap();
            row.column::<i64>(0).unwrap()
        });
        assert_eq!(n, 3, "Spanner に 3 行入っているはず");
    }

    /// 欧文（SF Pro）と和文（ヒラギノ）が混在したときのベースライン揃いを視覚確認する。
    /// target/ui_shots/font_check.png に出力。GPU 必須のため #[ignore]。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_font_alignment_check() {
        use egui_kittest::Harness;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        std::fs::create_dir_all("target/ui_shots").unwrap();

        struct FontApp;
        impl eframe::App for FontApp {
            fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
                ui.add_space(8.0);
                for sz in [13.0_f32, 18.0, 26.0] {
                    ui.label(egui::RichText::new("Abg123 あ漢字Test ボタンRecords 件数Xy").size(sz));
                    ui.add_space(6.0);
                }
                ui.separator();
                ui.heading("接続(emu) Project 漢字 123");
                let _ = ui.button("照合を実行 Run 99 件");
                ui.horizontal(|ui| {
                    ui.label("CSV件数");
                    ui.label(egui::RichText::new("12,345").strong());
                    ui.label("行Records");
                });
                // UI で使う記号グリフが豆腐(□)になっていないか確認する。
                ui.separator();
                for s in [
                    "⟳ 再キュー", "⏹ 中断", "取消 ×", "⟳ 更新", "✓ ✗ ⚠", "▲ ▼ ← → ↔",
                    "🔑 📋 🔍 ● ×",
                ] {
                    ui.label(egui::RichText::new(s).size(20.0));
                }
            }
        }

        let mut h = Harness::builder()
            .with_size(egui::vec2(720.0, 460.0))
            .build_eframe(|cc| {
                install_japanese_font(&cc.egui_ctx);
                setup_style(&cc.egui_ctx);
                FontApp
            });
        h.run();
        match h.render() {
            Ok(img) => img.save("target/ui_shots/font_check.png").unwrap(),
            Err(e) => eprintln!("[render] font_check 失敗: {e}"),
        }

        // 比較用: ヒラギノ単一フォント（欧文も和文も同一フォント＝設計上の調和した高さ）。
        let mut h2 = Harness::builder()
            .with_size(egui::vec2(720.0, 460.0))
            .build_eframe(|cc| {
                use std::sync::Arc;
                let mut fonts = egui::FontDefinitions::default();
                let jp = std::fs::read("/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc").unwrap();
                fonts.font_data.insert(
                    "hira".into(),
                    Arc::new(egui::FontData::from_owned(jp).tweak(egui::FontTweak {
                        y_offset_factor: 0.05,
                        ..Default::default()
                    })),
                );
                fonts
                    .families
                    .get_mut(&egui::FontFamily::Proportional)
                    .unwrap()
                    .insert(0, "hira".into());
                cc.egui_ctx.set_fonts(fonts);
                setup_style(&cc.egui_ctx);
                FontApp
            });
        h2.run();
        match h2.render() {
            Ok(img) => img.save("target/ui_shots/font_hiragino.png").unwrap(),
            Err(e) => eprintln!("[render] font_hiragino 失敗: {e}"),
        }
    }

    /// CSV ビューアの列幅が内容に応じて変わることを視覚確認する。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_csv_column_widths() {
        use egui_kittest::Harness;
        use std::sync::atomic::AtomicU64;
        use std::sync::Arc;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        std::fs::create_dir_all("target/ui_shots").unwrap();
        let body = "id,name,description,country\n\
                    1,Al,short,JP\n\
                    2,Bob,A much longer description that should widen this column considerably,US\n\
                    42,Charlie,mid,United Kingdom\n\
                    7,Dee,tiny,フランス\n";
        let path = std::env::temp_dir().join("spanner_viewer_csv_widths.csv");
        std::fs::write(&path, body).unwrap();
        let idx = csvview::CsvIndex::build(&path, Arc::new(AtomicU64::new(0))).unwrap();

        let mut harness = Harness::builder()
            .with_size(egui::vec2(900.0, 240.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));
        {
            let app = harness.state_mut();
            app.section = Section::Csv;
            let mut tab = CsvTab::new(path.clone());
            tab.index = Some(Arc::new(idx));
            app.csv_tabs.push(tab);
            // VS Code 風タブの見た目確認用に複数タブを足す（2 番目はローディング表示）。
            for name in ["users.csv", "orders_2024.csv"] {
                let p = std::env::temp_dir().join(name);
                std::fs::write(&p, "a,b\n1,2\n").ok();
                let mut t = CsvTab::new(p);
                t.loading = name.starts_with("orders");
                app.csv_tabs.push(t);
            }
            app.csv_active = 0;
        }
        for _ in 0..3 {
            harness.step();
        }
        match harness.render() {
            Ok(img) => img.save("target/ui_shots/csv_widths.png").unwrap(),
            Err(e) => eprintln!("[render] csv_widths 失敗: {e}"),
        }
    }

    /// 取込中の進捗バー（％・行数・速度・ETA）が描画されることを視覚確認する。
    /// 進捗中ジョブを差し込んでインポート画面を PNG 出力する（GPU 必須のため #[ignore]）。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_import_progress_to_png() {
        use egui_kittest::Harness;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::path::Path::new("target/ui_shots");
        std::fs::create_dir_all(dir).unwrap();
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1000.0, 460.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));

        {
            let app = harness.state_mut();
            app.section = Section::Spanner;
            app.view = View::Import;
            let req = query::ImportRequest {
                id: 0,
                table: "Users".into(),
                columns: vec![],
                source: query::ImportSource::File("/tmp/users.csv".into()),
                has_header: true,
                mode: query::ImportMode::InsertOrUpdate,
                empty_as_null: true,
                fresh: false,
                encoding: query::Encoding::Utf8,
                delimiter: b',',
                skip_bad_rows: false,
                dry_run: false,
                null_token: None,
                cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            app.import_jobs.push(ImportJob {
                req,
                source_name: "users.csv".into(),
                sent: true,
                status: JobStatus::Running,
                started: std::time::Instant::now()
                    .checked_sub(std::time::Duration::from_secs(6)),
                progress: Some(ImportProg {
                    frac: Some(0.42),
                    written: 420_000,
                    bytes_done: 42_000_000,
                    bytes_total: Some(100_000_000),
                }),
                result: None,
                outcome: None,
            });
        }
        harness.step();
        match harness.render() {
            Ok(img) => img.save(dir.join("07_import_progress.png")).unwrap(),
            Err(e) => eprintln!("[render] 失敗: {e}"),
        }
    }

    /// データタブのテーブルツリー（VS Code エクスプローラー風）を描画。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_data_tree_to_png() {
        use egui_kittest::Harness;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::path::Path::new("target/ui_shots");
        std::fs::create_dir_all(dir).unwrap();
        let col = |n: &str, t: &str, pk: bool| query::Column {
            name: n.into(),
            ty: t.into(),
            pk,
        };
        let node = |n: &str, cols: Vec<query::Column>| query::TableNode {
            name: n.into(),
            columns: cols,
            indexes: vec![],
        };
        let g = query::SchemaGraph {
            nodes: vec![
                node("Users", vec![col("Id", "INT64", true), col("Name", "STRING(MAX)", false)]),
                node("Orders", vec![col("OrderId", "INT64", true), col("UserId", "INT64", false)]),
                node("Products", vec![col("Sku", "STRING(36)", true)]),
            ],
            edges: vec![],
            error: None,
            ddl: std::collections::HashMap::new(),
        };
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1000.0, 560.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));
        {
            let app = harness.state_mut();
            app.section = Section::Spanner;
            app.view = View::Data;
            app.schema_graph = Some(g);
            app.tree_expanded.insert("Users".to_string());
        }
        harness.step();
        match harness.render() {
            Ok(img) => img.save(dir.join("10_data_tree.png")).unwrap(),
            Err(e) => eprintln!("[render] 失敗: {e}"),
        }
    }

    /// CSV ビューア（仮想化グリッド）を描画して PNG 出力（視覚確認用）。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_csv_viewer_to_png() {
        use egui_kittest::Harness;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::path::Path::new("target/ui_shots");
        std::fs::create_dir_all(dir).unwrap();
        // テスト用 CSV を作って索引化。
        let mut body = String::from("id,name,email,score,city\n");
        for i in 0..2000 {
            body.push_str(&format!(
                "{i},user_{i},user{i}@example.com,{},Tokyo-{}\n",
                i * 7 % 1000,
                i % 50
            ));
        }
        let path = std::env::temp_dir().join("spanner_viewer_render.csv");
        std::fs::write(&path, body).unwrap();
        let idx = csvview::CsvIndex::build(
            &path,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        )
        .unwrap();

        let mut harness = Harness::builder()
            .with_size(egui::vec2(1100.0, 560.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));
        {
            let app = harness.state_mut();
            app.section = Section::Csv;
            let mut tab = CsvTab::new(path.clone());
            tab.index = Some(std::sync::Arc::new(idx));
            app.csv_tabs.push(tab);
            // 2 つ目のタブ（タブバーの見た目確認用）。
            let p2 = std::env::temp_dir().join("spanner_viewer_render2.csv");
            std::fs::write(&p2, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
            let idx2 = csvview::CsvIndex::build(
                &p2,
                std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            )
            .unwrap();
            let mut t2 = CsvTab::new(p2);
            t2.index = Some(std::sync::Arc::new(idx2));
            app.csv_tabs.push(t2);
            app.csv_active = 0;
        }
        harness.step();
        match harness.render() {
            Ok(img) => img.save(dir.join("09_csv_viewer.png")).unwrap(),
            Err(e) => eprintln!("[render] 失敗: {e}"),
        }
    }

    /// k8s 通信フロー型アーキテクチャ図を描画して PNG 出力（視覚確認用）。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_arch_diagram_to_png() {
        use egui_kittest::Harness;
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::path::Path::new("target/ui_shots");
        std::fs::create_dir_all(dir).unwrap();
        let mut harness = Harness::builder()
            .with_size(egui::vec2(1100.0, 560.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));
        {
            let app = harness.state_mut();
            app.section = Section::Kube;
            app.kube_view = KubeView::Diagram;
            app.kube_graph = Some(k8s::sample_arch_graph());
        }
        harness.step();
        match harness.render() {
            Ok(img) => img.save(dir.join("08_arch_diagram.png")).unwrap(),
            Err(e) => eprintln!("[render] 失敗: {e}"),
        }
    }

    /// CSV グリッドが「実際に表示する内容」を検証する（ヘッダ/仮想化窓/絞り込み/
    /// ヘッダ無しを通す統合テスト。GPU 不要）。
    #[test]
    fn csv_tab_visible_rows_and_filter() {
        let p = std::env::temp_dir().join("spanner_viewer_tabtest.csv");
        std::fs::write(&p, b"Id,Name\n1,alice\n2,bob\n3,carol\n").unwrap();
        let idx = csvview::CsvIndex::build(
            &p,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        )
        .unwrap();
        let mut tab = CsvTab::new(p);
        tab.index = Some(std::sync::Arc::new(idx));

        // ヘッダ。
        assert_eq!(tab.header_cells(), vec!["Id", "Name"]);
        // 仮想化窓: 先頭から全データ行。
        assert_eq!(
            tab.visible_rows(0, 10),
            vec![
                vec!["1", "alice"],
                vec!["2", "bob"],
                vec!["3", "carol"]
            ]
        );
        // 途中から1行だけ。
        assert_eq!(tab.visible_rows(1, 1), vec![vec!["2", "bob"]]);
        // 絞り込み（ファイル行3=carol のみ表示）。
        tab.matches = Some(std::sync::Arc::new(vec![3]));
        assert_eq!(tab.visible_rows(0, 10), vec![vec!["3", "carol"]]);
        // ヘッダ無し: 列名は 列1,列2、先頭行もデータ扱い。
        tab.matches = None;
        tab.has_header = false;
        assert_eq!(tab.header_cells(), vec!["列1", "列2"]);
        assert_eq!(tab.visible_rows(0, 1), vec![vec!["Id", "Name"]]);
    }

    /// テスト用に Channels を作る（背景ループは無いので送受信端は捨てる）。
    fn make_test_channels() -> Channels {
        use std::sync::mpsc::channel;
        use tokio::sync::mpsc::unbounded_channel;
        let (_a, sample_rx) = channel();
        let (req_tx, _b) = unbounded_channel();
        let (_c, res_rx) = channel();
        let (import_req_tx, _d) = unbounded_channel();
        let (_e, import_res_rx) = channel();
        let (gcs_req_tx, _f) = unbounded_channel();
        let (_g, gcs_res_rx) = channel();
        let (verify_req_tx, _vf) = unbounded_channel();
        let (_vg, verify_res_rx) = channel();
        let (_h, schema_rx) = channel();
        let (_hp, plan_rx) = channel();
        let (_i, kube_metrics_rx) = channel();
        let (kube_topo_req_tx, _j) = unbounded_channel();
        let (_k, kube_topo_rx) = channel();
        let (kube_log_req_tx, _l) = unbounded_channel();
        let (_m, kube_log_rx) = channel();
        let (kube_ev_req_tx, _n) = unbounded_channel();
        let (_o, kube_ev_rx) = channel();
        let (kube_action_req_tx, _p) = unbounded_channel();
        let (_q, kube_action_rx) = channel();
        let (kube_res_req_tx, _r) = unbounded_channel();
        let (_s, kube_res_rx) = channel();
        let (kube_pf_req_tx, _t) = unbounded_channel();
        let (_u, kube_pf_rx) = channel();
        // 受信端を握り続けて切断扱いを避ける（描画に影響しないよう leak）。
        for far in [
            Box::new(_a) as Box<dyn std::any::Any>,
            Box::new(_b),
            Box::new(_c),
            Box::new(_d),
            Box::new(_e),
            Box::new(_f),
            Box::new(_g),
            Box::new(_h),
            Box::new(_i),
            Box::new(_j),
            Box::new(_k),
            Box::new(_l),
            Box::new(_m),
            Box::new(_n),
            Box::new(_o),
            Box::new(_p),
            Box::new(_q),
            Box::new(_r),
            Box::new(_s),
            Box::new(_t),
            Box::new(_u),
            Box::new(_vf),
            Box::new(_vg),
            Box::new(_hp),
        ] {
            Box::leak(far);
        }
        Channels {
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
            poll_interval: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(30)),
            conn_info: "test-project/test-instance/test-db".into(),
        }
    }

    /// 実アプリの各画面を wgpu でヘッドレス描画し PNG に保存する（視覚確認用）。
    /// 画像は target/ui_shots/*.png に出る。スナップショット比較ではなく目視用。
    /// GPU アダプタが要るので既定では実行せず、`cargo test -- --ignored render_app_screens_to_png`
    /// で明示的に走らせる。
    #[ignore = "wgpu アダプタが必要。視覚確認時のみ手動実行する"]
    #[test]
    fn render_app_screens_to_png() {
        use egui_kittest::Harness;
        // 背景スレッド（ADC チェック）が rustls プロバイダを要求するため入れておく。
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::path::Path::new("target/ui_shots");
        std::fs::create_dir_all(dir).unwrap();

        let mut harness = Harness::builder()
            .with_size(egui::vec2(1200.0, 780.0))
            .build_eframe(|cc| MonitorApp::new(make_test_channels(), cc));

        let shots = [
            (Section::Spanner, View::Monitor, false, "01_monitor"),
            (Section::Spanner, View::Data, false, "02_data"),
            (Section::Spanner, View::Schema, false, "03_schema"),
            (Section::Spanner, View::Import, false, "04_import"),
            (Section::Spanner, View::Monitor, true, "05_settings"),
            (Section::Kube, View::Monitor, false, "06_kube"),
        ];
        for (sec, view, settings, name) in shots {
            {
                let app = harness.state_mut();
                app.section = sec;
                app.view = view;
                app.settings_open = settings;
            }
            harness.run();
            match harness.render() {
                Ok(img) => img.save(dir.join(format!("{name}.png"))).unwrap(),
                Err(e) => eprintln!("[render] {name} 失敗（wgpu アダプタ無し?）: {e}"),
            }
        }

        // 照合タブ: マッピング画面 + サンプル結果を注入して描画する。
        {
            let app = harness.state_mut();
            app.section = Section::Spanner;
            app.view = View::Verify;
            app.settings_open = false;
            app.verify = Some(VerifyState {
                table: "Users".into(),
                table_columns: vec![
                    query::Column { name: "Id".into(), ty: "INT64".into(), pk: true },
                    query::Column { name: "Name".into(), ty: "STRING(MAX)".into(), pk: false },
                    query::Column { name: "Email".into(), ty: "STRING(MAX)".into(), pk: false },
                ],
                source: query::ImportSource::File("users.csv".into()),
                source_name: "users.csv".into(),
                preview_bytes: b"Id,Name,Email\n1,a,a@x\n".to_vec(),
                records: vec![
                    vec!["Id".into(), "Name".into(), "Email".into()],
                    vec!["1".into(), "a".into(), "a@x".into()],
                ],
                csv_headers: vec!["Id".into(), "Name".into(), "Email".into()],
                encoding: query::Encoding::Utf8,
                delimiter: b',',
                has_header: true,
                empty_as_null: true,
                numeric_match: true,
                null_token: String::new(),
                mapping: vec![Some(0), Some(1), Some(2)],
                note: None,
                config_msg: None,
            });
            app.verify_result = Some(query::VerifyOutcome {
                table: "Users".into(),
                csv_rows: 10_000,
                db_rows: 9_998,
                matched: 9_980,
                value_mismatch: 3,
                csv_only: 12,
                db_only: 5,
                csv_dup: 0,
                samples: vec![
                    query::VerifySample {
                        kind: query::VerifyKind::ValueMismatch,
                        key: "43".into(),
                        detail: "Name: 'Tanaka'(csv) ≠ 'Tanaca'(db)".into(),
                    },
                    query::VerifySample {
                        kind: query::VerifyKind::CsvOnly,
                        key: "99".into(),
                        detail: String::new(),
                    },
                    query::VerifySample {
                        kind: query::VerifyKind::DbOnly,
                        key: "12".into(),
                        detail: String::new(),
                    },
                ],
                samples_truncated: false,
                db_truncated: false,
                elapsed_ms: 1234,
                error: None,
                note: None,
            });
        }
        harness.run();
        match harness.render() {
            Ok(img) => img.save(dir.join("07_verify.png")).unwrap(),
            Err(e) => eprintln!("[render] 07_verify 失敗: {e}"),
        }

        // 実行計画タブ（合成プランを注入）。
        {
            let app = harness.state_mut();
            app.section = Section::Spanner;
            app.view = View::Plan;
            app.verify = None;
            app.sql = "SELECT u.Name, COUNT(*) FROM Users u JOIN Orders o ON o.UserId = u.Id GROUP BY u.Name".into();
            let pl = |depth, name: &str, detail: &str, scalar| query::PlanLine {
                depth,
                name: name.into(),
                detail: detail.into(),
                scalar,
            };
            app.plan_result = Some(query::PlanOutcome {
                lines: vec![
                    pl(0, "Distributed Union", "", false),
                    pl(1, "Serialize Result", "", false),
                    pl(2, "Global Stream Aggregate", "GROUP BY u.Name", false),
                    pl(3, "Distributed Cross Apply", "", false),
                    pl(4, "Table Scan", "scan_target=Users", false),
                    pl(4, "Index Scan", "scan_target=IDX_Orders_UserId", false),
                    pl(5, "Reference", "$UserId", true),
                ],
                elapsed_ms: 42,
                error: None,
            });
        }
        harness.run();
        match harness.render() {
            Ok(img) => img.save(dir.join("11_plan.png")).unwrap(),
            Err(e) => eprintln!("[render] 11_plan 失敗: {e}"),
        }

        // スキーマ図 + CREATE 文ウィンドウ。
        {
            let app = harness.state_mut();
            app.section = Section::Spanner;
            app.view = View::Schema;
            app.verify = None;
            app.ddl_view = Some((
                "Orders".into(),
                "CREATE TABLE Orders (\n  \
                 OrderId INT64 NOT NULL,\n  \
                 UserId INT64,\n  \
                 Amount NUMERIC,\n) PRIMARY KEY(OrderId);\n\n\
                 CREATE INDEX IDX_Orders_User ON Orders(UserId);\n\n\
                 ALTER TABLE Orders ADD CONSTRAINT FK_Orders_Users \
                 FOREIGN KEY(UserId) REFERENCES Users(Id);"
                    .into(),
            ));
        }
        // スキーマ未取得でスピナーが回り続けるため run() ではなく step() で 1 フレーム描く。
        for _ in 0..3 {
            harness.step();
        }
        match harness.render() {
            Ok(img) => img.save(dir.join("08_ddl.png")).unwrap(),
            Err(e) => eprintln!("[render] 08_ddl 失敗: {e}"),
        }

        eprintln!("[render] PNG 出力先: {}", dir.display());
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

    /// 未完ジョブの保存→復元: 内容が保たれ、状態は「中断」、id は振り直し、fresh=false。
    #[test]
    fn import_jobs_save_restore_roundtrip() {
        let saved = vec![
            SavedImportJob {
                table: "Users".into(),
                columns: vec![query::ImportColumn {
                    name: "Id".into(),
                    ty: "INT64".into(),
                    src_index: 0,
                }],
                source: query::ImportSource::File("/tmp/u.csv".into()),
                source_name: "u.csv".into(),
                has_header: true,
                mode: query::ImportMode::InsertOrUpdate,
                empty_as_null: true,
                encoding: query::Encoding::Utf8,
                delimiter: b',',
                skip_bad_rows: false,
                null_token: Some("<null>".into()),
            },
            SavedImportJob {
                table: "Orders".into(),
                columns: vec![],
                source: query::ImportSource::Gcs("gs://b/o.csv".into()),
                source_name: "o.csv".into(),
                has_header: false,
                mode: query::ImportMode::Insert,
                empty_as_null: false,
                encoding: query::Encoding::ShiftJis,
                delimiter: b'\t',
                skip_bad_rows: true,
                null_token: None,
            },
        ];
        // serde 往復で壊れないこと。
        let json = serde_json::to_string(&saved).unwrap();
        let back: Vec<SavedImportJob> = serde_json::from_str(&json).unwrap();
        let (jobs, next_id) = saved_jobs_to_import_jobs(back);
        assert_eq!(jobs.len(), 2);
        assert_eq!(next_id, 3);
        // 中断状態・id 振り直し・チェックポイント再開(fresh=false)。
        assert_eq!(jobs[0].status, JobStatus::Cancelled);
        assert_eq!(jobs[0].req.id, 1);
        assert_eq!(jobs[1].req.id, 2);
        assert!(!jobs[0].req.fresh);
        // 内容が保たれている。
        assert_eq!(jobs[0].req.table, "Users");
        assert_eq!(jobs[0].req.null_token.as_deref(), Some("<null>"));
        assert_eq!(jobs[0].req.mode, query::ImportMode::InsertOrUpdate);
        assert_eq!(jobs[1].req.encoding, query::Encoding::ShiftJis);
        assert_eq!(jobs[1].req.delimiter, b'\t');
        matches!(jobs[1].req.source, query::ImportSource::Gcs(_));
    }

    /// 並列ディスパッチ判定: 別テーブルは並列・同一テーブルは直列・上限まで。
    #[test]
    fn can_dispatch_rules() {
        use std::collections::HashSet;
        let mut running: HashSet<String> = HashSet::new();
        // 何も走っていなければ送れる。
        assert!(can_dispatch(&running, "A", 3));
        running.insert("A".into());
        // 別テーブルは並列で送れる。
        assert!(can_dispatch(&running, "B", 3));
        // 同一テーブルは直列（送れない）。
        assert!(!can_dispatch(&running, "A", 3));
        running.insert("B".into());
        running.insert("C".into());
        // 上限(3)に達したら別テーブルでも送れない。
        assert!(!can_dispatch(&running, "D", 3));
        // 上限が大きければ送れる。
        assert!(can_dispatch(&running, "D", 5));
    }

    /// 総件数の推定（書込済 × 全体/読込済）。情報不足なら None。
    #[test]
    fn import_total_estimate_cases() {
        let p = |written, done, total| ImportProg {
            frac: None,
            written,
            bytes_done: done,
            bytes_total: total,
        };
        // 42% 読込で 42 万行 → 約 100 万行。
        assert_eq!(import_total_estimate(&p(420_000, 42, Some(100))), Some(1_000_000));
        // 全体バイト不明。
        assert_eq!(import_total_estimate(&p(100, 10, None)), None);
        // まだ 0 行 / 0 バイト。
        assert_eq!(import_total_estimate(&p(0, 10, Some(100))), None);
        assert_eq!(import_total_estimate(&p(100, 0, Some(100))), None);
        // 推定は最低でも written 以上（読込が書込に先行しても下回らない）。
        assert_eq!(import_total_estimate(&p(100, 100, Some(50))), Some(100));
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

    /// SQL 補完候補（前方一致・大小無視・テーブル/カラム優先）。
    #[test]
    fn sql_completions_basic() {
        let tables = vec!["Users".to_string(), "Orders".to_string()];
        let cols = vec!["UserId".to_string(), "Name".to_string()];
        // "us" → Users(テーブル) と UserId(カラム)。
        assert_eq!(sql_completions("us", &tables, &cols, 8), vec!["Users", "UserId"]);
        // 大小無視（"OR" 自体は完全一致で除外、"Orders"/"ORDER BY" が出る）。
        assert_eq!(sql_completions("OR", &tables, &cols, 8), vec!["Orders", "ORDER BY"]);
        // キーワードも候補に。
        assert_eq!(sql_completions("sel", &tables, &cols, 8), vec!["SELECT"]);
        // 空はなし。
        assert!(sql_completions("", &tables, &cols, 8).is_empty());
        // 完全一致は除外（打ち終わっている語は提案しない）。
        assert!(sql_completions("name", &tables, &cols, 8).is_empty());
    }

    /// カーソル直前の識別子トークン範囲。
    #[test]
    fn current_word_range_cases() {
        let t = "SELECT * FROM Us";
        // 末尾（16文字目）の直前の単語は "Us"（byte 14..16）。
        assert_eq!(current_word_range(t, 16), (14, 16));
        // 記号の直後は空語。
        assert_eq!(current_word_range("a = ", 4), (4, 4));
        // 日本語混じりでも byte 境界で正しく返す。
        let j = "あ FROM Te";
        let n = j.chars().count();
        let (s, e) = current_word_range(j, n);
        assert_eq!(&j[s..e], "Te");
    }

    /// 補完適用: 単語を候補+空白で置換し、カーソル文字位置を返す。
    #[test]
    fn apply_sql_completion_replaces() {
        let mut sql = "SELECT * FROM Us".to_string();
        let mut cur = None;
        apply_sql_completion(&mut sql, (14, 16), "Users", &mut cur);
        assert_eq!(sql, "SELECT * FROM Users ");
        assert_eq!(cur, Some(20));
    }

    /// ImportDialog.recompute: ヘッダ無は位置対応・ヘッダ有は名前一致。列順が保たれる。
    #[test]
    fn import_dialog_no_header_positional_mapping() {
        let col = |n: &str, t: &str, pk: bool| query::Column {
            name: n.into(),
            ty: t.into(),
            pk,
        };
        let mut d = ImportDialog {
            table: "T".into(),
            table_columns: vec![
                col("Id", "INT64", true),
                col("Name", "STRING(MAX)", false),
                col("Score", "FLOAT64", false),
            ],
            source: query::ImportSource::File("/dev/null".into()),
            file_name: "x.csv".into(),
            preview_bytes: vec![],
            records: vec![
                vec!["1".into(), "alice".into(), "1.5".into()],
                vec!["2".into(), "bob".into(), "2.0".into()],
            ],
            encoding: query::Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            null_token: String::new(),
            has_header: false,
            csv_headers: vec![],
            mapping: vec![],
            mode: query::ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            note: None,
            config_msg: None,
        };
        // ヘッダ無し → 位置対応（Id←列1, Name←列2, Score←列3）。
        d.recompute();
        assert_eq!(d.mapping, vec![Some(0), Some(1), Some(2)]);
        assert_eq!(
            d.csv_headers,
            vec!["列1".to_string(), "列2".to_string(), "列3".to_string()]
        );
        // ヘッダ有りに切替: 先頭行が見出しになり、テーブル列名と一致しないので全て None。
        d.has_header = true;
        d.recompute();
        assert_eq!(d.mapping, vec![None, None, None]);
        assert_eq!(
            d.csv_headers,
            vec!["1".to_string(), "alice".to_string(), "1.5".to_string()]
        );
    }

    /// マッピング初期値: ヘッダ有=名前一致 / ヘッダ無=位置対応（スキップにしない）。
    #[test]
    fn auto_mapping_header_vs_positional() {
        let cols = vec![
            query::Column { name: "Id".into(), ty: "INT64".into(), pk: true },
            query::Column { name: "Name".into(), ty: "STRING(MAX)".into(), pk: false },
            query::Column { name: "Extra".into(), ty: "STRING(MAX)".into(), pk: false },
        ];
        // ヘッダ有り: 名前一致（Name↔name, Id↔ID）。Extra は CSV に無いので None。
        let headers = vec!["ID".to_string(), "name".to_string()];
        assert_eq!(
            auto_mapping(&cols, &headers, true, 2),
            vec![Some(0), Some(1), None]
        );
        // ヘッダ無し: 位置で対応（列1→Id, 列2→Name, 列3→Extra）。CSV が 2 列なら 3 列目は None。
        let h2 = vec!["列1".to_string(), "列2".to_string()];
        assert_eq!(
            auto_mapping(&cols, &h2, false, 2),
            vec![Some(0), Some(1), None]
        );
        // CSV が 3 列あれば 3 列とも割り当て。
        assert_eq!(
            auto_mapping(&cols, &[], false, 3),
            vec![Some(0), Some(1), Some(2)]
        );
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

    /// 証跡レポート Markdown: 完了ジョブのみ・元件数/新規挿入/更新の推定・
    /// 未完了ジョブは除外。
    #[test]
    fn report_markdown_counts_and_filters() {
        fn job(
            table: &str,
            status: JobStatus,
            outcome: Option<query::ImportOutcome>,
        ) -> ImportJob {
            ImportJob {
                req: query::ImportRequest {
                    id: 0,
                    table: table.into(),
                    columns: vec![],
                    source: query::ImportSource::File("/tmp/x.csv".into()),
                    has_header: true,
                    mode: query::ImportMode::InsertOrUpdate,
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
                status,
                started: None,
                progress: None,
                result: None,
                outcome,
            }
        }
        // 完了: CSV100行, 取込前10→取込後80 → 新規70, 更新=written(100)-70=30。
        let done = job(
            "A",
            JobStatus::Done,
            Some(query::ImportOutcome {
                written: 100,
                total: 100,
                before_count: Some(10),
                after_count: Some(80),
                ..Default::default()
            }),
        );
        // 待機中（outcome なし）は除外されるべき。
        let queued = job("B", JobStatus::Queued, None);
        let ts = chrono::Local::now();
        let md = report_markdown(&[done, queued], &ts);
        assert!(md.contains("| A |"), "完了ジョブは載る");
        assert!(!md.contains("| B |"), "未完了ジョブは載らない");
        // 新規挿入 70 / 更新 30 が出る。
        assert!(md.contains("| 70 | 30 |"), "新規/更新の推定: {md}");
        assert!(md.contains("対象: 完了 1 ジョブ"), "完了のみ集計");
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
