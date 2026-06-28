//! テーブルデータビューア用のクエリワーカー。
//! UI から SQL を受け取り、Spanner で実行して結果（列名 + 文字列化した行）を返す。
//! 監視側とは別系統で、オンデマンド（実行ボタン）で動く。

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use gcloud_spanner::client::{Client, ClientConfig};
use gcloud_spanner::mutation::{insert, insert_or_update};
use gcloud_spanner::row::{Error as RowError, Row};
use gcloud_spanner::statement::{Statement, ToKind};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::UnboundedReceiver;

// BatchWrite（生 gRPC）用。gcloud-spanner の Client は batch_write を公開して
// いないため、同じ認証・接続スタック（gcloud-gax + google-cloud-auth）を使って
// SpannerClient を直接組み立てる。
use google_cloud_gax::conn::{
    Channel as GaxChannel, ConnectionManager as GaxConnManager, ConnectionOptions, Environment,
};
use google_cloud_googleapis::spanner::v1::spanner_client::SpannerClient;
use google_cloud_googleapis::spanner::v1::{
    batch_write_request::MutationGroup, BatchCreateSessionsRequest, BatchWriteRequest,
    DeleteSessionRequest, Session,
};

/// 1 クエリで取得する最大行数（UI 保護のため上限）
const MAX_ROWS: usize = 1000;

/// 各テーブルのカラム一覧（ダイアグラムのボックス内に表示）。
const COLUMNS_SQL: &str = "\
SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE \
FROM INFORMATION_SCHEMA.COLUMNS \
WHERE TABLE_SCHEMA = '' \
ORDER BY TABLE_NAME, ORDINAL_POSITION";

/// 各テーブルのセカンダリインデックスとその構成カラム。
const INDEXES_SQL: &str = "\
SELECT i.TABLE_NAME, i.INDEX_NAME, i.IS_UNIQUE, ic.COLUMN_NAME \
FROM INFORMATION_SCHEMA.INDEXES i \
JOIN INFORMATION_SCHEMA.INDEX_COLUMNS ic \
  ON i.TABLE_SCHEMA = ic.TABLE_SCHEMA \
 AND i.TABLE_NAME = ic.TABLE_NAME \
 AND i.INDEX_NAME = ic.INDEX_NAME \
WHERE i.TABLE_SCHEMA = '' AND i.INDEX_TYPE = 'INDEX' \
 AND ic.ORDINAL_POSITION IS NOT NULL \
ORDER BY i.TABLE_NAME, i.INDEX_NAME, ic.ORDINAL_POSITION";

/// 主キーを構成するカラム（PK バッジ表示用）。
const PK_SQL: &str = "\
SELECT TABLE_NAME, COLUMN_NAME \
FROM INFORMATION_SCHEMA.INDEX_COLUMNS \
WHERE TABLE_SCHEMA = '' AND INDEX_TYPE = 'PRIMARY_KEY'";

/// テーブル間の依存（インターリーブの親子 + 外部キー）を一覧する SQL。
pub const DEPENDENCY_SQL: &str = "\
SELECT TABLE_NAME AS `テーブル`, 'インターリーブ' AS `種別`, \
       PARENT_TABLE_NAME AS `依存先`, IFNULL(ON_DELETE_ACTION, '') AS `詳細` \
FROM INFORMATION_SCHEMA.TABLES \
WHERE TABLE_SCHEMA = '' AND PARENT_TABLE_NAME IS NOT NULL \
UNION ALL \
SELECT tc.TABLE_NAME, '外部キー', ctu.TABLE_NAME, tc.CONSTRAINT_NAME \
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE ctu \
  ON tc.CONSTRAINT_CATALOG = ctu.CONSTRAINT_CATALOG \
 AND tc.CONSTRAINT_SCHEMA = ctu.CONSTRAINT_SCHEMA \
 AND tc.CONSTRAINT_NAME = ctu.CONSTRAINT_NAME \
WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY' AND tc.TABLE_SCHEMA = '' \
ORDER BY 1, 2";

/// リクエストの種別（結果の振り分けに使う）
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Target {
    #[default]
    Data,
    Schema,
    /// クエリの実行計画（EXPLAIN 相当。PLAN モードで取得）。
    Plan,
}

/// 実行計画ツリーの 1 ノード（表示用）。
#[derive(Clone, Debug)]
pub struct PlanLine {
    /// インデント深さ（0=ルート）。
    pub depth: usize,
    /// 演算子名（display_name）。
    pub name: String,
    /// 補足（short_representation や metadata の要約）。
    pub detail: String,
    /// スカラ（式）ノードか（リレーショナル演算子と区別して薄く表示）。
    pub scalar: bool,
}

/// 実行計画の取得結果（UI へ返す）。
#[derive(Clone, Debug, Default)]
pub struct PlanOutcome {
    pub lines: Vec<PlanLine>,
    pub elapsed_ms: u128,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct Config {
    pub project: String,
    pub instance: String,
    pub database: String,
    pub mock: bool,
}

/// 接続先 Spanner 環境（project/instance/database）。設定画面から切り替える。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpannerEnv {
    pub project: String,
    pub instance: String,
    pub database: String,
}

impl SpannerEnv {
    pub fn configured(&self) -> bool {
        !(self.project.is_empty() || self.instance.is_empty() || self.database.is_empty())
    }
}

/// 現在選択中の接続先（query / monitoring が参照する）。
static CURRENT_ENV: std::sync::Mutex<Option<SpannerEnv>> = std::sync::Mutex::new(None);

/// 接続先を設定する（設定画面の選択時）。
pub fn set_spanner_env(env: SpannerEnv) {
    *CURRENT_ENV.lock().unwrap() = Some(env);
}

/// まだ設定されていなければ初期値を入れる（起動時の seed 用）。
pub fn init_spanner_env(env: SpannerEnv) {
    let mut cur = CURRENT_ENV.lock().unwrap();
    if cur.is_none() {
        *cur = Some(env);
    }
}

/// 現在の接続先を取得。
pub fn current_spanner_env() -> SpannerEnv {
    CURRENT_ENV.lock().unwrap().clone().unwrap_or_default()
}

/// クエリ実行結果（UI へ返す）
#[derive(Clone, Debug, Default)]
pub struct QueryOutcome {
    pub target: Target,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub truncated: bool,
    pub elapsed_ms: u128,
    pub error: Option<String>,
}

// ── CSV インポート用モデル ──

/// 既存行と衝突したときの書き込み方式。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ImportMode {
    /// 新規挿入のみ。主キーが既存だと失敗する。
    #[default]
    Insert,
    /// 既存行があれば上書き（INSERT OR UPDATE 相当）。
    InsertOrUpdate,
}

/// インポート先の 1 カラム（名前・型・対応する CSV 列）。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ImportColumn {
    pub name: String,
    /// SPANNER_TYPE 文字列（例: "INT64" / "STRING(MAX)"）。値の変換に使う。
    pub ty: String,
    /// この列に書き込む CSV 側の列インデックス（0 始まり）。
    pub src_index: usize,
}

/// CSV の取得元。行データは UI に溜め込まず、ここから都度ストリーミングする。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ImportSource {
    /// ローカルファイル。
    File(std::path::PathBuf),
    /// GCS オブジェクト（gs://bucket/object）。
    Gcs(String),
}

/// CSV の文字コード。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Encoding {
    #[default]
    Utf8,
    /// Shift-JIS / CP932（Windows 日本語）。
    ShiftJis,
}

impl Encoding {
    /// バイト列を UTF-8 文字列へデコードする（不正シーケンスは置換）。
    pub fn decode(self, bytes: &[u8]) -> String {
        match self {
            Encoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
            Encoding::ShiftJis => encoding_rs::SHIFT_JIS.decode(bytes).0.into_owned(),
        }
    }
}

/// CSV を 1 テーブルへストリーミング取り込みする要求。
/// 行データは持たず、`source` から逐次読み出す（ローカルに全行を溜めない）。
#[derive(Clone, Debug)]
pub struct ImportRequest {
    /// ジョブ識別子（UI が進捗/完了を正しいジョブ行へ紐付けるために使う）。
    pub id: u64,
    pub table: String,
    /// 書き込む列（各列が対応する CSV 列インデックスを持つ）。
    pub columns: Vec<ImportColumn>,
    /// 取得元。
    pub source: ImportSource,
    /// 先頭行をヘッダとして読み飛ばすか。
    pub has_header: bool,
    pub mode: ImportMode,
    /// 空欄を NULL として扱うか（false なら空文字列として書き込む）。
    pub empty_as_null: bool,
    /// 前回のチェックポイントを無視して最初からやり直すか。
    pub fresh: bool,
    /// 文字コード。
    pub encoding: Encoding,
    /// フィールド区切り文字（',' / '\t' / ';' など）。
    pub delimiter: u8,
    /// 型変換やコミットに失敗した行をスキップして続行するか（false なら即中断）。
    pub skip_bad_rows: bool,
    /// 書き込まずに検証だけ行うか（ドライラン）。
    pub dry_run: bool,
    /// この文字列に一致するセルを NULL として扱う（空欄とは別。例 "NULL" / "\\N"）。
    pub null_token: Option<String>,
    /// 中断フラグ。UI から true にすると安全に停止する（続きから再開可）。
    pub cancel: Arc<AtomicBool>,
}

/// インポート結果（UI へ返す）。
#[derive(Clone, Debug, Default)]
pub struct ImportOutcome {
    /// 対応するジョブ識別子（UI がジョブ行へ紐付ける）。
    pub id: u64,
    pub table: String,
    /// 書き込めた行数（ドライランでは「書き込める」と判定した行数）。
    pub written: usize,
    /// 要求された総行数。
    pub total: usize,
    /// 前回チェックポイントから再開してスキップした行数（無駄を省いた分）。
    pub resumed: usize,
    /// スキップ（不正行）した行数。
    pub skipped: usize,
    /// 中断で停止したか。
    pub cancelled: bool,
    /// ドライラン（検証のみ）だったか。
    pub dry_run: bool,
    /// リジェクト（スキップ行）を書き出したファイルパス。
    pub reject_path: Option<String>,
    pub elapsed_ms: u128,
    pub error: Option<String>,
    /// 取込前のテーブル行数（COUNT(*)。取得できなければ None）。
    pub before_count: Option<i64>,
    /// 取込後のテーブル行数（COUNT(*)。取得できなければ None）。
    /// 新規挿入 ≈ after-before、更新 ≈ written-(after-before)（upsert の近似）。
    pub after_count: Option<i64>,
}

/// インポート中に背景 → UI へ流すイベント（進捗 / 完了）。
#[derive(Clone, Debug)]
pub enum ImportProgress {
    /// 取込の途中経過。`bytes_total` が分かれば割合表示に使える。
    /// バウンドチャネルにより読み出しは書き込みに追従するので、
    /// bytes ベースの割合は実際の取込進捗とほぼ一致する。
    Progress {
        /// 対応するジョブ識別子。
        id: u64,
        /// これまでに書き込めた行数。
        written: usize,
        /// 読み出した累積バイト数。
        bytes_done: u64,
        /// ソース全体のバイト数（不明なら None）。
        bytes_total: Option<u64>,
    },
    /// 完了（結果）。
    Done(ImportOutcome),
}

// ── CSV ↔ DB 照合（突合）用モデル ──

/// 照合に使う 1 カラム（テーブル列名・主キーか・対応する CSV 列）。
#[derive(Clone, Debug)]
pub struct VerifyColumn {
    pub name: String,
    /// 主キー構成カラムか（行の突合キーに使う）。
    pub pk: bool,
    /// この列に対応する CSV 側の列インデックス（0 始まり）。
    pub src_index: usize,
}

/// CSV とテーブルを主キーで突合し、各カラム値まで比較する要求。
#[derive(Clone, Debug)]
pub struct VerifyRequest {
    pub table: String,
    /// 比較対象の列（主キー列を最低 1 つ含むこと）。
    pub columns: Vec<VerifyColumn>,
    pub source: ImportSource,
    pub has_header: bool,
    pub encoding: Encoding,
    pub delimiter: u8,
    /// 空欄を NULL とみなして比較するか（DB の NULL は "NULL" 表記）。
    pub empty_as_null: bool,
    /// この文字列を NULL とみなす（空欄とは別。例 "NULL" / "\\N"）。
    pub null_token: Option<String>,
    /// 中断フラグ。
    pub cancel: Arc<AtomicBool>,
}

/// 不一致サンプルの種別。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyKind {
    /// 主キーは一致するが、いずれかのカラム値が異なる。
    ValueMismatch,
    /// CSV にあるが DB に無い主キー。
    CsvOnly,
    /// DB にあるが CSV に無い主キー。
    DbOnly,
}

/// 不一致 1 件のサンプル（一覧表示用。件数は別途集計、サンプルは打ち切りあり）。
#[derive(Clone, Debug)]
pub struct VerifySample {
    pub kind: VerifyKind,
    /// 主キーの表示（複合キーは ", " 連結）。
    pub key: String,
    /// 詳細（値差異なら "列名: 'csv値' ≠ 'db値'"、片側のみなら空）。
    pub detail: String,
}

/// 照合結果（UI へ返す）。
#[derive(Clone, Debug, Default)]
pub struct VerifyOutcome {
    pub table: String,
    /// 読み込んだ CSV データ行数（ヘッダ除く）。
    pub csv_rows: usize,
    /// 読み込んだ DB 行数。
    pub db_rows: usize,
    /// 主キー一致かつ全カラム値一致。
    pub matched: usize,
    /// 主キー一致だが値が異なる。
    pub value_mismatch: usize,
    /// CSV のみ（DB に無い）。
    pub csv_only: usize,
    /// DB のみ（CSV に無い）。
    pub db_only: usize,
    /// CSV 内で主キーが重複していた行数（2 回目以降）。
    pub csv_dup: usize,
    /// 不一致サンプル（種別ごとに一覧。総数は上のカウント）。
    pub samples: Vec<VerifySample>,
    /// サンプルを上限で打ち切ったか。
    pub samples_truncated: bool,
    /// DB 行を上限で打ち切ったか（巨大テーブル時）。
    pub db_truncated: bool,
    pub elapsed_ms: u128,
    pub error: Option<String>,
    /// 補足（モード制限など）。
    pub note: Option<String>,
}

/// 照合中に背景 → UI へ流すイベント（進捗 / 完了）。
#[derive(Clone, Debug)]
pub enum VerifyProgress {
    Progress {
        /// 現在のフェーズ（"DB読込" / "CSV照合"）。
        phase: &'static str,
        db_rows: usize,
        csv_rows: usize,
    },
    Done(VerifyOutcome),
}

/// GCS から CSV プレビューを取得した結果（UI へ返す）。
#[derive(Clone, Debug, Default)]
pub struct GcsFetchOutcome {
    /// 要求された gs:// URI（エコーバック。インポートダイアログのソース名に使う）。
    pub uri: String,
    /// 取得できたプレビューの生バイト（文字コード未確定）。失敗時は None。
    pub data: Option<Vec<u8>>,
    pub error: Option<String>,
}

/// バケット内オブジェクト一覧の結果（UI へ返す）。
#[derive(Clone, Debug, Default)]
pub struct GcsListOutcome {
    pub bucket: String,
    /// 一覧した prefix（現在のフォルダ位置）。
    pub prefix: String,
    /// この階層のオブジェクト（フルパス）。
    pub objects: Vec<String>,
    /// delimiter='/' による疑似フォルダ（末尾 / 付きのフルパス）。
    pub folders: Vec<String>,
    pub error: Option<String>,
}

/// UI → 背景の GCS 要求。
#[derive(Clone, Debug)]
pub enum GcsRequest {
    /// `gs://bucket/object` をダウンロードする。
    Fetch(String),
    /// `gs://bucket/prefix`（prefix 省略可）のオブジェクトを一覧する。
    List(String),
}

/// 背景 → UI の GCS 応答。
#[derive(Clone, Debug)]
pub enum GcsResponse {
    Fetched(GcsFetchOutcome),
    Listed(GcsListOutcome),
}

// ── スキーマダイアグラム用モデル ──

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    Interleave,
    ForeignKey,
}

/// 1 カラム
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub pk: bool, // 主キー構成カラムか
}

/// テーブル（ダイアグラムのノード）
#[derive(Clone, Debug)]
pub struct TableNode {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<String>, // "IndexName (col, ...) [UNIQUE]" 形式
}

/// 依存関係（ダイアグラムのエッジ）。from が to に依存する。
#[derive(Clone, Debug)]
pub struct Edge {
    pub from: String,
    pub to: String,
    pub kind: EdgeKind,
    pub label: String,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaGraph {
    pub nodes: Vec<TableNode>,
    pub edges: Vec<Edge>,
    pub error: Option<String>,
    /// テーブル名 → 実 DDL（GetDatabaseDdl 由来。CREATE TABLE + 付随する
    /// CREATE INDEX / ALTER TABLE を連結）。取得できなければ空（UI 側で近似 DDL に
    /// フォールバック）。
    pub ddl: std::collections::HashMap<String, String>,
}

const NO_CONFIG: &str = "接続先が未設定です。右上でプロジェクト/インスタンス/DB を選ぶか、\
SPANNER_PROJECT / SPANNER_INSTANCE / SPANNER_DATABASE 環境変数で指定してください。\
一覧取得には GCP 認証が必要です（gcloud ADC ログイン、または GOOGLE_APPLICATION_CREDENTIALS のサービスアカウント鍵）。";

/// UI からのリクエストを順次処理する。req 側が閉じたら終了。
/// データクエリは `data_tx`、スキーマ図は `schema_tx` に結果を返す。
pub async fn query_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<(Target, String)>,
    data_tx: std::sync::mpsc::Sender<QueryOutcome>,
    schema_tx: std::sync::mpsc::Sender<SchemaGraph>,
    plan_tx: std::sync::mpsc::Sender<PlanOutcome>,
) {
    // 起動時の env（環境変数由来）を初期値として seed（設定画面が未指定のとき用）。
    init_spanner_env(SpannerEnv {
        project: cfg.project.clone(),
        instance: cfg.instance.clone(),
        database: cfg.database.clone(),
    });
    let mock = cfg.mock;
    // クライアントは接続先 env ごとにキャッシュ。env が変わったら作り直す。
    // tokio の Mutex は poison しないので、パニック後もロックは解放される。
    let client: std::sync::Arc<tokio::sync::Mutex<Option<(SpannerEnv, Client)>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(None));

    while let Some((target, sql)) = req_rx.recv().await {
        let client = client.clone();
        // タスク用とパニック時のエラー通知用に別々のクローンを用意する。
        let data_tx_task = data_tx.clone();
        let schema_tx_task = schema_tx.clone();
        let plan_tx_task = plan_tx.clone();
        let data_tx_err = data_tx.clone();
        let schema_tx_err = schema_tx.clone();
        let plan_tx_err = plan_tx.clone();

        // 各リクエストは独立タスクで実行する。Spanner クライアント側で万一
        // パニックが起きても、この query_loop 自体は生き続け、UI からの次の
        // 実行を受け付けられるようにする（実行ボタンが無反応になるのを防ぐ）。
        let handle = tokio::spawn(async move {
            let env = current_spanner_env();
            let configured = env.configured();
            match target {
                Target::Data => {
                    let start = Instant::now();
                    let mut guard = client.lock().await;
                    let mut out = if mock {
                        mock_data(&sql)
                    } else if !configured {
                        QueryOutcome {
                            error: Some(NO_CONFIG.into()),
                            ..Default::default()
                        }
                    } else {
                        ensure_and_run(&mut guard, &env, &sql).await
                    };
                    out.target = Target::Data;
                    out.elapsed_ms = start.elapsed().as_millis();
                    let _ = data_tx_task.send(out);
                }
                Target::Schema => {
                    let mut guard = client.lock().await;
                    let graph = if mock {
                        mock_graph()
                    } else if !configured {
                        SchemaGraph {
                            error: Some(NO_CONFIG.into()),
                            ..Default::default()
                        }
                    } else {
                        match ensure_client(&mut guard, &env).await {
                            Ok(c) => {
                                let mut g = fetch_schema(c).await;
                                // 実 DDL をベストエフォートで取得（失敗時は近似 DDL に
                                // フォールバックするので無視してよい）。
                                if g.error.is_none() {
                                    g.ddl = fetch_ddl(&env).await;
                                }
                                g
                            }
                            Err(e) => SchemaGraph {
                                error: Some(e),
                                ..Default::default()
                            },
                        }
                    };
                    let _ = schema_tx_task.send(graph);
                }
                Target::Plan => {
                    let start = Instant::now();
                    let mut guard = client.lock().await;
                    let mut out = if mock {
                        PlanOutcome {
                            error: Some("モックモードでは実行計画を取得できません".into()),
                            ..Default::default()
                        }
                    } else if !configured {
                        PlanOutcome {
                            error: Some(NO_CONFIG.into()),
                            ..Default::default()
                        }
                    } else {
                        match ensure_client(&mut guard, &env).await {
                            Ok(c) => run_plan(c, &sql).await,
                            Err(e) => PlanOutcome {
                                error: Some(e),
                                ..Default::default()
                            },
                        }
                    };
                    out.elapsed_ms = start.elapsed().as_millis();
                    let _ = plan_tx_task.send(out);
                }
            }
        });

        // タスクがパニックで落ちた場合は、エラーとして UI に返し無反応を防ぐ。
        if let Err(join_err) = handle.await {
            let msg = panic_message(join_err);
            match target {
                Target::Data => {
                    let _ = data_tx_err.send(QueryOutcome {
                        target: Target::Data,
                        error: Some(msg),
                        ..Default::default()
                    });
                }
                Target::Schema => {
                    let _ = schema_tx_err.send(SchemaGraph {
                        error: Some(msg),
                        ..Default::default()
                    });
                }
                Target::Plan => {
                    let _ = plan_tx_err.send(PlanOutcome {
                        error: Some(msg),
                        ..Default::default()
                    });
                }
            }
        }
    }
}

/// UI からの CSV インポート要求を順次処理する。req 側が閉じたら終了。
/// 結果は `res_tx` に返す。書き込み系なのでデータ取得の query_loop とは別系統。
pub async fn import_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<ImportRequest>,
    res_tx: std::sync::mpsc::Sender<ImportProgress>,
) {
    init_spanner_env(SpannerEnv {
        project: cfg.project.clone(),
        instance: cfg.instance.clone(),
        database: cfg.database.clone(),
    });
    let mock = cfg.mock;

    // 各要求は独立タスクで実行し、await で待たずに次を受け付ける。これにより
    // 別テーブルへのジョブを並列実行できる（同一テーブルの直列化は UI 側の投入で担保）。
    // 進捗/完了は req.id を載せて返すので、UI は正しいジョブ行へ紐付けられる。
    while let Some(req) = req_rx.recv().await {
        let res_tx_task = res_tx.clone();
        let res_tx_err = res_tx.clone();
        let id = req.id;
        let table = req.table.clone();

        let inner = tokio::spawn(async move {
            let start = Instant::now();
            let env = current_spanner_env();
            let mut out = if !mock && !env.configured() {
                ImportOutcome {
                    error: Some(NO_CONFIG.into()),
                    ..Default::default()
                }
            } else {
                // ソースからストリーミングし、並列 BatchWrite で投入する。
                // 途中経過は res_tx_task に Progress として流す。
                run_streaming_import(&env, &req, mock, &res_tx_task).await
            };
            out.id = req.id;
            out.table = req.table.clone();
            out.elapsed_ms = start.elapsed().as_millis();
            let _ = res_tx_task.send(ImportProgress::Done(out));
        });

        // パニック時も必ず Done を返してジョブが Running のまま残らないようにする。
        // 監視タスクで join するので、本ループは次の要求をすぐ受け付けられる。
        tokio::spawn(async move {
            if let Err(join_err) = inner.await {
                let _ = res_tx_err.send(ImportProgress::Done(ImportOutcome {
                    id,
                    table,
                    error: Some(panic_message(join_err)),
                    ..Default::default()
                }));
            }
        });
    }
}

/// 照合で DB に読み込む最大行数（メモリ保護。超えたら note で通知）。
const VERIFY_DB_CAP: usize = 5_000_000;
/// 不一致サンプルの保持上限（UI 表示用。総数は別途集計）。
const VERIFY_SAMPLE_CAP: usize = 500;
/// 複合主キーの内部連結に使う区切り（データに現れない制御文字）。
const KEY_SEP: char = '\u{1}';

/// UI からの CSV↔DB 照合要求を順次処理する。req 側が閉じたら終了。
pub async fn verify_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<VerifyRequest>,
    res_tx: std::sync::mpsc::Sender<VerifyProgress>,
) {
    init_spanner_env(SpannerEnv {
        project: cfg.project.clone(),
        instance: cfg.instance.clone(),
        database: cfg.database.clone(),
    });
    let mock = cfg.mock;

    while let Some(req) = req_rx.recv().await {
        let res_tx_task = res_tx.clone();
        let res_tx_err = res_tx.clone();
        let table = req.table.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let env = current_spanner_env();
            let mut out = if !mock && !env.configured() {
                VerifyOutcome {
                    error: Some(NO_CONFIG.into()),
                    ..Default::default()
                }
            } else {
                run_verify(&env, &req, mock, &res_tx_task).await
            };
            out.table = req.table.clone();
            out.elapsed_ms = start.elapsed().as_millis();
            let _ = res_tx_task.send(VerifyProgress::Done(out));
        });

        if let Err(join_err) = handle.await {
            let _ = res_tx_err.send(VerifyProgress::Done(VerifyOutcome {
                table,
                error: Some(panic_message(join_err)),
                ..Default::default()
            }));
        }
    }
}

/// CSV 1 セルを比較用に正規化する。空欄/NULL トークンは "NULL" に揃える
/// （DB の NULL も "NULL" で文字列化されるため、これで突合できる）。
fn canon_cell(raw: &str, empty_as_null: bool, null_token: Option<&str>) -> String {
    if let Some(tok) = null_token {
        if raw == tok {
            return "NULL".to_string();
        }
    }
    // 取込側 convert_cell と同じく <null>/(null) は既定で NULL 扱いにし、突合が一致する。
    if is_default_null_token(raw) {
        return "NULL".to_string();
    }
    if empty_as_null && raw.is_empty() {
        return "NULL".to_string();
    }
    raw.to_string()
}

/// テーブル全行を読み込む（行数上限 cap）。UI 保護の MAX_ROWS は適用しない。
async fn read_all_rows(
    client: &Client,
    sql: &str,
    cap: usize,
) -> anyhow::Result<(Vec<Vec<String>>, bool)> {
    let mut tx = client.single().await?;
    let mut iter = tx.query(Statement::new(sql)).await?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut truncated = false;
    while let Some(row) = iter.next().await? {
        if rows.len() >= cap {
            truncated = true;
            break;
        }
        let mut r = Vec::new();
        let mut i = 0;
        while let Some(cell) = stringify_cell(&row, i) {
            r.push(cell);
            i += 1;
        }
        rows.push(r);
    }
    Ok((rows, truncated))
}

/// CSV とテーブルを主キーで突合し、全カラム値まで比較する。
async fn run_verify(
    env: &SpannerEnv,
    req: &VerifyRequest,
    mock: bool,
    progress: &std::sync::mpsc::Sender<VerifyProgress>,
) -> VerifyOutcome {
    // 比較する列（テーブル列名・PK・CSV列）。並び順 = SELECT・値比較の順。
    if req.columns.is_empty() {
        return VerifyOutcome {
            error: Some("比較する列がありません".into()),
            ..Default::default()
        };
    }
    let pk_positions: Vec<usize> = req
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.pk)
        .map(|(i, _)| i)
        .collect();
    if pk_positions.is_empty() {
        return VerifyOutcome {
            error: Some("主キー列が割り当てられていません（突合キーに必要です）".into()),
            ..Default::default()
        };
    }

    // ── DB 側を全件読み込み、主キー → 全カラム値 のマップを作る ──
    // モックモードでは実テーブルが無いので空とみなす（CSV 件数のみ）。
    let mut db_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut db_truncated = false;
    if !mock {
        let select_cols = req
            .columns
            .iter()
            .map(|c| format!("`{}`", c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_cols} FROM `{}`", req.table);
        let client = match build_client(env).await {
            Ok(c) => c,
            Err(e) => {
                return VerifyOutcome {
                    error: Some(format!("接続/認証に失敗: {e}")),
                    ..Default::default()
                }
            }
        };
        let _ = progress.send(VerifyProgress::Progress {
            phase: "DB読込",
            db_rows: 0,
            csv_rows: 0,
        });
        let (rows, truncated) = match read_all_rows(&client, &sql, VERIFY_DB_CAP).await {
            Ok(v) => v,
            Err(e) => {
                return VerifyOutcome {
                    error: Some(format!("DB 読み込みに失敗: {e}")),
                    ..Default::default()
                }
            }
        };
        db_truncated = truncated;
        db_map.reserve(rows.len());
        for row in rows {
            // SELECT の列順 = req.columns 順。PK 位置から突合キーを作る。
            let key = pk_positions
                .iter()
                .map(|&p| row.get(p).cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(&KEY_SEP.to_string());
            db_map.insert(key, row);
        }
        let _ = progress.send(VerifyProgress::Progress {
            phase: "DB読込",
            db_rows: db_map.len(),
            csv_rows: 0,
        });
    }

    // ── CSV をストリーミングし、各行を DB マップと突合 ──
    let (mut source, _bytes_total, _id) = match open_source(&req.source).await {
        Ok(s) => s,
        Err(e) => {
            return VerifyOutcome {
                error: Some(format!("CSV を開けません: {e}")),
                ..Default::default()
            }
        }
    };

    let mut out = VerifyOutcome {
        db_rows: db_map.len(),
        db_truncated,
        ..Default::default()
    };
    // CSV 側で突合できた DB キー（db_only 算出用）と、CSV 内重複検出用。
    let mut matched_keys: HashSet<String> = HashSet::new();
    let mut csv_seen: HashSet<String> = HashSet::new();

    let mut streamer = CsvStreamer::new(req.encoding, req.delimiter);
    let mut recs: Vec<Vec<String>> = Vec::new();
    let mut first_chunk = true;
    let mut row_no = 0usize; // 1 始まりの CSV 行番号（ヘッダ含む）

    let process = |rec: &[String],
                       out: &mut VerifyOutcome,
                       matched_keys: &mut HashSet<String>,
                       csv_seen: &mut HashSet<String>| {
        out.csv_rows += 1;
        // 比較用に各列を正規化。列不足は空欄（→ empty_as_null で NULL）扱い。
        let cell = |src: usize| -> String {
            let raw = rec.get(src).map(|s| s.as_str()).unwrap_or("");
            canon_cell(raw, req.empty_as_null, req.null_token.as_deref())
        };
        let key_disp = pk_positions
            .iter()
            .map(|&p| cell(req.columns[p].src_index))
            .collect::<Vec<_>>();
        let key = key_disp.join(&KEY_SEP.to_string());
        let key_show = key_disp.join(", ");
        if !csv_seen.insert(key.clone()) {
            out.csv_dup += 1;
        }
        match db_map.get(&key) {
            None => {
                out.csv_only += 1;
                push_sample(out, VerifyKind::CsvOnly, key_show, String::new());
            }
            Some(db_row) => {
                matched_keys.insert(key.clone());
                // 全カラム値を比較し、最初に異なる列を詳細に出す。
                let mut diff: Option<String> = None;
                for (i, c) in req.columns.iter().enumerate() {
                    let cv = cell(c.src_index);
                    let dv = db_row.get(i).cloned().unwrap_or_default();
                    if cv != dv {
                        diff = Some(format!("{}: '{cv}'(csv) ≠ '{dv}'(db)", c.name));
                        break;
                    }
                }
                match diff {
                    None => out.matched += 1,
                    Some(detail) => {
                        out.value_mismatch += 1;
                        push_sample(out, VerifyKind::ValueMismatch, key_show, detail);
                    }
                }
            }
        }
    };

    loop {
        if req.cancel.load(Ordering::Relaxed) {
            out.note = Some("中断しました（部分的な結果）".into());
            break;
        }
        let chunk = match source.next_chunk().await {
            Ok(Some(mut b)) => {
                if first_chunk {
                    strip_bom(&mut b);
                    first_chunk = false;
                }
                b
            }
            Ok(None) => break,
            Err(e) => {
                out.error = Some(format!("CSV 読み込み中にエラー: {e}"));
                return out;
            }
        };
        streamer.push(&chunk, &mut recs);
        for rec in recs.drain(..) {
            row_no += 1;
            if req.has_header && row_no == 1 {
                continue; // ヘッダ行は飛ばす
            }
            // 列数が極端に少ない行も突合は試みる（不足分は空欄補完）。
            process(&rec, &mut out, &mut matched_keys, &mut csv_seen);
        }
        if out.csv_rows.is_multiple_of(50_000) {
            let _ = progress.send(VerifyProgress::Progress {
                phase: "CSV照合",
                db_rows: out.db_rows,
                csv_rows: out.csv_rows,
            });
        }
    }
    // 末尾の未確定レコードを処理。
    streamer.finish(&mut recs);
    for rec in recs.drain(..) {
        row_no += 1;
        if req.has_header && row_no == 1 {
            continue;
        }
        process(&rec, &mut out, &mut matched_keys, &mut csv_seen);
    }

    // DB のみ（CSV で突合されなかった DB キー）。
    out.db_only = db_map.len().saturating_sub(matched_keys.len());
    for (key, _) in db_map.iter() {
        if out.samples.len() >= VERIFY_SAMPLE_CAP {
            out.samples_truncated = true;
            break;
        }
        if !matched_keys.contains(key) {
            let key_show = key.split(KEY_SEP).collect::<Vec<_>>().join(", ");
            out.samples.push(VerifySample {
                kind: VerifyKind::DbOnly,
                key: key_show,
                detail: String::new(),
            });
        }
    }

    if mock {
        out.note = Some(
            "モックモードでは実テーブルが無いため、全行を『CSVのみ』として扱います。".into(),
        );
    } else if db_truncated {
        out.note = Some(format!(
            "DB 行が上限 {VERIFY_DB_CAP} 件で打ち切られました。結果は部分的です。"
        ));
    }
    out
}

/// 不一致サンプルを上限まで蓄える（超過分は truncated フラグのみ立てる）。
fn push_sample(out: &mut VerifyOutcome, kind: VerifyKind, key: String, detail: String) {
    if out.samples.len() < VERIFY_SAMPLE_CAP {
        out.samples.push(VerifySample { kind, key, detail });
    } else {
        out.samples_truncated = true;
    }
}

/// GCS の読み取りに使う OAuth スコープ（読み取り専用）。
const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_only";

/// 一覧 API 用スコープ（ADC の既定スコープ）。
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Resource Manager の projects 応答 1 ページを (projectId 群, nextPageToken) に解析する。
/// v1(`lifecycleState`)・v3(`state`) の両方に対応し、削除予約中（DELETE_REQUESTED）は除外。
fn parse_projects_page(body: &str) -> anyhow::Result<(Vec<String>, Option<String>)> {
    #[derive(serde::Deserialize, Default)]
    struct Resp {
        #[serde(default)]
        projects: Vec<Proj>,
        #[serde(rename = "nextPageToken")]
        next_page_token: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct Proj {
        #[serde(rename = "projectId")]
        project_id: String,
        #[serde(default, rename = "lifecycleState")]
        lifecycle_state: Option<String>,
        #[serde(default)]
        state: Option<String>,
    }
    let r: Resp = serde_json::from_str(body)?;
    let ids = r
        .projects
        .into_iter()
        .filter(|p| {
            p.lifecycle_state.as_deref() != Some("DELETE_REQUESTED")
                && p.state.as_deref() != Some("DELETE_REQUESTED")
        })
        .map(|p| p.project_id)
        .collect();
    Ok((ids, r.next_page_token))
}

/// `{ <field>: [ { name: "projects/.../X" }, ... ], "nextPageToken": "..." }` から
/// 末尾セグメント X 群と次ページトークンを取り出す（Spanner instances/databases 応答）。
fn parse_resource_names(body: &str, field: &str) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let v: serde_json::Value = serde_json::from_str(body)?;
    let mut out: Vec<String> = Vec::new();
    if let Some(arr) = v.get(field).and_then(|x| x.as_array()) {
        for item in arr {
            if let Some(name) = item.get("name").and_then(|n| n.as_str()) {
                if let Some(seg) = name.rsplit('/').next() {
                    if !seg.is_empty() {
                        out.push(seg.to_string());
                    }
                }
            }
        }
    }
    let next = v
        .get("nextPageToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .map(String::from);
    Ok((out, next))
}

/// ページングしながら指定フィールドの末尾セグメントを全て集める。
async fn list_paged(base_url: &str, field: &str) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    let mut page = String::new();
    loop {
        let url = if page.is_empty() {
            format!("{base_url}?pageSize=500")
        } else {
            format!("{base_url}?pageSize=500&pageToken={page}")
        };
        let body = fetch_text(&url).await?;
        let (ids, next) = parse_resource_names(&body, field)?;
        out.extend(ids);
        match next {
            Some(t) if out.len() < 5000 => page = t,
            _ => break,
        }
    }
    out.sort();
    Ok(out)
}

/// ADC のトークンを取得する（cloud-platform スコープ）。
async fn cloud_token() -> anyhow::Result<String> {
    let provider = gcp_auth::provider().await?;
    let token = provider.token(&[CLOUD_PLATFORM_SCOPE]).await?;
    Ok(token.as_str().to_string())
}

/// ADC（Application Default Credentials）でトークンを取得できるか確認する。
/// 成功＝ログイン済み。失敗＝未ログイン/認証情報なし。
pub async fn check_adc() -> anyhow::Result<()> {
    cloud_token().await.map(|_| ())
}

/// ADC で利用可能なプロジェクト ID を一覧する。
/// v3 `projects:search` を使う（フォルダ/組織経由でアクセスできるものも含めて返す。
/// v1 `projects.list` は直接 IAM 付与されたものしか返さず取りこぼすため）。
pub async fn list_projects() -> anyhow::Result<Vec<String>> {
    let token = cloud_token().await?;
    let client = reqwest::Client::new();
    let mut out = Vec::new();
    let mut page = String::new();
    loop {
        // state:ACTIVE で削除済み/保留中を除外（余計なものを出さない）。
        let mut q = vec![
            ("pageSize", "1000".to_string()),
            ("query", "state:ACTIVE".to_string()),
        ];
        if !page.is_empty() {
            q.push(("pageToken", page.clone()));
        }
        let body = client
            .get("https://cloudresourcemanager.googleapis.com/v3/projects:search")
            .bearer_auth(&token)
            .query(&q)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let (ids, next) = parse_projects_page(&body)?;
        out.extend(ids);
        match next {
            Some(t) if !t.is_empty() && out.len() < 5000 => page = t,
            _ => break,
        }
    }
    out.sort();
    Ok(out)
}

/// 指定プロジェクトの Spanner インスタンス ID を一覧する（全ページ）。
pub async fn list_instances(project: &str) -> anyhow::Result<Vec<String>> {
    list_paged(
        &format!("https://spanner.googleapis.com/v1/projects/{project}/instances"),
        "instances",
    )
    .await
}

/// 指定インスタンスの Spanner データベース ID を一覧する（全ページ）。
pub async fn list_databases(project: &str, instance: &str) -> anyhow::Result<Vec<String>> {
    list_paged(
        &format!(
            "https://spanner.googleapis.com/v1/projects/{project}/instances/{instance}/databases"
        ),
        "databases",
    )
    .await
}

/// ADC 認証付き GET の本文を取得する。
async fn fetch_text(url: &str) -> anyhow::Result<String> {
    let token = cloud_token().await?;
    Ok(reqwest::Client::new()
        .get(url)
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

/// デモ/モック用の CSV（実 GCS に触らず動作確認するため）。
const MOCK_GCS_CSV: &str = "Id,Name,Score,Active,Note\n\
                            1,Alice,12.5,true,gcs-mock\n\
                            2,Bob,7.0,false,gcs-mock\n";

/// UI からの GCS 要求（取得 / 一覧）を順次処理する。req 側が閉じたら終了。
pub async fn gcs_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<GcsRequest>,
    res_tx: std::sync::mpsc::Sender<GcsResponse>,
) {
    let mock = cfg.mock;
    while let Some(req) = req_rx.recv().await {
        let res_tx_task = res_tx.clone();
        let res_tx_err = res_tx.clone();
        let req_err = req.clone();

        // 各要求は独立タスクで実行する。万一パニックしてもループは生き残る。
        let handle = tokio::spawn(async move {
            let resp = match req {
                GcsRequest::Fetch(uri) => GcsResponse::Fetched(do_gcs_fetch(mock, uri).await),
                GcsRequest::List(loc) => GcsResponse::Listed(do_gcs_list(mock, loc).await),
            };
            let _ = res_tx_task.send(resp);
        });

        if let Err(join_err) = handle.await {
            let msg = panic_message(join_err);
            let resp = match req_err {
                GcsRequest::Fetch(uri) => GcsResponse::Fetched(GcsFetchOutcome {
                    uri,
                    data: None,
                    error: Some(msg),
                }),
                GcsRequest::List(loc) => {
                    let (bucket, prefix) =
                        split_gs_location(&loc).unwrap_or_else(|_| (String::new(), String::new()));
                    GcsResponse::Listed(GcsListOutcome {
                        bucket,
                        prefix,
                        error: Some(msg),
                        ..Default::default()
                    })
                }
            };
            let _ = res_tx_err.send(resp);
        }
    }
}

/// `gs://bucket/object` のプレビューを取得した結果を組み立てる。
async fn do_gcs_fetch(mock: bool, uri: String) -> GcsFetchOutcome {
    if mock {
        return GcsFetchOutcome {
            uri,
            data: Some(MOCK_GCS_CSV.as_bytes().to_vec()),
            error: None,
        };
    }
    match fetch_gcs_object(&uri).await {
        Ok(data) => GcsFetchOutcome {
            uri,
            data: Some(data),
            error: None,
        },
        Err(e) => GcsFetchOutcome {
            uri,
            data: None,
            error: Some(e.to_string()),
        },
    }
}

/// `gs://bucket/prefix` のオブジェクトを一覧した結果を組み立てる。
async fn do_gcs_list(mock: bool, loc: String) -> GcsListOutcome {
    let (bucket, prefix) = match split_gs_location(&loc) {
        Ok(v) => v,
        Err(e) => {
            return GcsListOutcome {
                error: Some(e),
                ..Default::default()
            }
        }
    };
    if mock {
        // prefix 直下に擬似的なフォルダとファイルを返す。
        return GcsListOutcome {
            bucket,
            prefix: prefix.clone(),
            folders: vec![format!("{prefix}sub/")],
            objects: vec![format!("{prefix}users.csv"), format!("{prefix}orders.csv")],
            error: None,
        };
    }
    match list_gcs_objects(&bucket, &prefix).await {
        Ok((objects, folders)) => GcsListOutcome {
            bucket,
            prefix,
            objects,
            folders,
            error: None,
        },
        Err(e) => GcsListOutcome {
            bucket,
            prefix,
            error: Some(e.to_string()),
            ..Default::default()
        },
    }
}

/// `gs://bucket/object` を (bucket, object) に分解する（object 必須）。
fn parse_gs_uri(uri: &str) -> Result<(String, String), String> {
    let rest = uri
        .trim()
        .strip_prefix("gs://")
        .ok_or_else(|| "gs://bucket/path.csv の形式で指定してください".to_string())?;
    let (bucket, object) = rest
        .split_once('/')
        .ok_or_else(|| "オブジェクトのパスがありません（例: gs://my-bucket/data.csv）".to_string())?;
    if bucket.is_empty() || object.is_empty() {
        return Err("バケット名またはオブジェクト名が空です".into());
    }
    Ok((bucket.to_string(), object.to_string()))
}

/// `gs://bucket/prefix` を (bucket, prefix) に分解する（prefix は省略可）。
pub fn split_gs_location(uri: &str) -> Result<(String, String), String> {
    let rest = uri
        .trim()
        .strip_prefix("gs://")
        .ok_or_else(|| "gs://bucket/... の形式で指定してください".to_string())?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return Err("バケット名が空です".into());
    }
    Ok((bucket.to_string(), prefix.to_string()))
}

/// オブジェクト名を GCS JSON API 用に percent-encode する。
/// スラッシュ含めて非予約文字以外をすべてエスケープする（パスセグメント1個として渡すため）。
fn encode_object(object: &str) -> String {
    let mut out = String::with_capacity(object.len());
    for b in object.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// マッピング用のプレビュー範囲だけ取得する（先頭バイトのみ。全体は落とさない）。
const GCS_PREVIEW_BYTES: usize = 128 * 1024;

/// GCS オブジェクトの先頭を Range 取得して文字列で返す（プレビュー用）。
async fn fetch_gcs_object(uri: &str) -> anyhow::Result<Vec<u8>> {
    let (bucket, object) = parse_gs_uri(uri).map_err(|e| anyhow::anyhow!(e))?;
    let provider = gcp_auth::provider().await?;
    let token = provider.token(&[GCS_SCOPE]).await?;
    let encoded = encode_object(&object);
    // alt=media で本文を取得。Range で先頭だけに絞る。
    let url =
        format!("https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}?alt=media");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .bearer_auth(token.as_str())
        .header("Range", format!("bytes=0-{}", GCS_PREVIEW_BYTES - 1))
        .send()
        .await?
        .error_for_status()?;
    // 文字コード未確定なので生バイトのまま返す（プレビュー側でデコード）。
    Ok(resp.bytes().await?.to_vec())
}

/// GCS JSON API でバケット内オブジェクトを一覧する。
/// delimiter='/' で 1 階層分（直下のファイルと擬似フォルダ）を返す。
/// 戻り値は (オブジェクト名, 擬似フォルダ prefix)。
async fn list_gcs_objects(
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let provider = gcp_auth::provider().await?;
    let token = provider.token(&[GCS_SCOPE]).await?;
    let url = format!("https://storage.googleapis.com/storage/v1/b/{bucket}/o");
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .bearer_auth(token.as_str())
        .query(&[
            ("prefix", prefix),
            ("delimiter", "/"),
            ("maxResults", "1000"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<GcsListResponse>()
        .await?;
    let objects = resp.items.into_iter().map(|i| i.name).collect();
    Ok((objects, resp.prefixes))
}

#[derive(serde::Deserialize, Default)]
struct GcsListResponse {
    #[serde(default)]
    items: Vec<GcsListItem>,
    /// delimiter による共通プレフィックス（疑似フォルダ）。
    #[serde(default)]
    prefixes: Vec<String>,
}

#[derive(serde::Deserialize)]
struct GcsListItem {
    name: String,
}

// ── ストリーミング取り込み（並列 BatchWrite） ──

/// 本番 Spanner で同時に走らせる BatchWrite ストリーム数（= セッション数）。
const IMPORT_CONCURRENCY: usize = 8;

/// 実際に使う並列数。エミュレータは「同時に 1 トランザクションのみ」対応のため
/// 1 に落とす（並列にすると ABORTED になる）。本番は IMPORT_CONCURRENCY。
fn import_concurrency() -> usize {
    match std::env::var("SPANNER_EMULATOR_HOST") {
        Ok(h) if !h.is_empty() => 1,
        _ => IMPORT_CONCURRENCY,
    }
}

// ── 再開（チェックポイント） ──
//
// バッチには決定的な連番 index を振る（同じファイル・列・per_request なら同じ index =
// 同じ行集合）。完全にコミットできたバッチの index だけをファイルへ追記し、毎回 flush する。
// 再実行時に index が一致するバッチはスキップ → Spanner への再書き込みを省く。

/// チェックポイント保存ディレクトリ（~/.spanner-viewer/import-progress、無ければ temp）。
fn checkpoint_dir() -> PathBuf {
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(".spanner-viewer").join("import-progress")
}

/// 取り込みの同一性シグネチャ。これが一致するチェックポイントだけ再開に使う
/// （ファイルが変わったり列マッピングが変わったら別物として扱う）。mode は含めない
/// （Insert で落ちても上書き挿入で再開できるようにするため）。
fn import_signature(req: &ImportRequest, per_request: usize) -> String {
    let src = match &req.source {
        ImportSource::File(p) => {
            let meta = std::fs::metadata(p).ok();
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(0);
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            format!("file:{}:{len}:{mtime}", p.display())
        }
        ImportSource::Gcs(uri) => format!("gcs:{uri}"),
    };
    let cols: Vec<String> = req
        .columns
        .iter()
        .map(|c| format!("{}|{}|{}", c.name, c.ty, c.src_index))
        .collect();
    // v2: チェックポイント形式が「idx offset」に変わったため版を上げる
    // （旧 v1 ファイルはシグネチャ不一致で無視され、安全に最初からになる）。
    format!(
        "v2\ttable={}\tper={per_request}\thdr={}\tnull={}\tsrc={src}\tcols={}",
        req.table,
        req.has_header,
        req.empty_as_null,
        cols.join(",")
    )
}

/// シグネチャからチェックポイントのファイルパスを作る。
fn checkpoint_path(sig: &str) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sig.hash(&mut h);
    checkpoint_dir().join(format!("ckpt-{:016x}.txt", h.finish()))
}

/// 既存チェックポイントから「コミット済みバッチ index → (終端バイト位置, 累積データ行数)」
/// を読む（シグネチャ一致時のみ）。各行は `idx offset rows`。
fn load_checkpoint(path: &Path, sig: &str) -> HashMap<usize, (u64, usize)> {
    let mut map = HashMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return map;
    };
    let mut lines = text.lines();
    if lines.next() != Some(sig) {
        return map; // シグネチャ不一致 → 再開に使わない
    }
    for l in lines {
        let mut it = l.split_whitespace();
        if let (Some(i), Some(o), Some(r)) = (it.next(), it.next(), it.next()) {
            if let (Ok(i), Ok(o), Ok(r)) =
                (i.parse::<usize>(), o.parse::<u64>(), r.parse::<usize>())
            {
                map.insert(i, (o, r));
            }
        }
    }
    map
}

/// コミット済みバッチ集合から「先頭から連続して全部コミット済みの末尾」を求める。
/// 戻り値 (連続済みバッチ数 k, そこまでの終端バイト位置, 累積データ行数)。
/// k 番目以降の飛び石コミットはバイト的に飛ばせないので committed 判定で再送スキップする。
fn contiguous_resume_point(committed: &HashMap<usize, (u64, usize)>) -> (usize, u64, usize) {
    let mut k = 0usize;
    let mut offset = 0u64;
    let mut rows = 0usize;
    while let Some(&(off, r)) = committed.get(&k) {
        offset = off;
        rows = r;
        k += 1;
    }
    (k, offset, rows)
}

/// コミット済みバッチ index を追記するライタ（都度 flush でクラッシュ耐性）。
struct CheckpointWriter {
    file: std::sync::Mutex<Option<std::fs::File>>,
}

impl CheckpointWriter {
    /// `new_file=true` で新規作成（シグネチャ行を書く）、false で追記オープン。
    fn open(path: &Path, sig: &str, new_file: bool) -> Self {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = if new_file {
            std::fs::File::create(path).ok().and_then(|mut f| {
                writeln!(f, "{sig}").ok()?;
                let _ = f.flush();
                Some(f)
            })
        } else {
            std::fs::OpenOptions::new().append(true).open(path).ok()
        };
        CheckpointWriter {
            file: std::sync::Mutex::new(file),
        }
    }

    /// バッチ index・終端バイト位置・累積データ行数を確定として記録する（コミット後に呼ぶ）。
    fn mark(&self, idx: usize, end_offset: u64, end_row: usize) {
        if let Ok(mut g) = self.file.lock() {
            if let Some(f) = g.as_mut() {
                let _ = writeln!(f, "{idx} {end_offset} {end_row}");
                let _ = f.flush();
            }
        }
    }
}

/// スキップした行（リジェクト）を CSV として書き出すライタ。最初の 1 件で遅延作成。
struct RejectWriter {
    path: PathBuf,
    delimiter: u8,
    file: std::sync::Mutex<Option<std::fs::File>>,
    any: AtomicBool,
}

impl RejectWriter {
    fn new(path: PathBuf, delimiter: u8) -> Self {
        RejectWriter {
            path,
            delimiter,
            file: std::sync::Mutex::new(None),
            any: AtomicBool::new(false),
        }
    }

    /// 1 行を理由付きで記録する（理由, 元のフィールド…）。
    fn reject(&self, reason: &str, row: &[String]) {
        if let Ok(mut g) = self.file.lock() {
            if g.is_none() {
                if let Some(p) = self.path.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                if let Ok(mut f) = std::fs::File::create(&self.path) {
                    let _ = writeln!(f, "# rejected rows: reason{}original fields...", self.delimiter as char);
                    *g = Some(f);
                }
            }
            if let Some(f) = g.as_mut() {
                let mut line = csv_escape(reason, self.delimiter);
                for c in row {
                    line.push(self.delimiter as char);
                    line.push_str(&csv_escape(c, self.delimiter));
                }
                let _ = writeln!(f, "{line}");
                let _ = f.flush();
                self.any.store(true, Ordering::Relaxed);
            }
        }
    }

    /// 何か書いたか（リジェクトファイルが存在するか）。
    fn had_any(&self) -> bool {
        self.any.load(Ordering::Relaxed)
    }
}

/// リジェクトファイルのパス（チェックポイントと同じディレクトリ）。
fn reject_path(sig: &str) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sig.hash(&mut h);
    checkpoint_dir().join(format!("rejects-{:016x}.csv", h.finish()))
}

/// CSV フィールドのエスケープ（区切り/" /改行を含むときだけクォート）。
fn csv_escape(s: &str, delim: u8) -> String {
    let d = delim as char;
    if s.contains(d) || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// CSV をソースからストリーミングし、行を貯めずに並列 BatchWrite で投入する。
/// - ローカルにもアプリにも全行を溜めない（メモリは概ね「1 バッチ × 並列数」）。
/// - 1 行 = 1 ミューテーショングループ（独立・非原子）を複数セッションで同時投入。
/// - チェックポイントで再開可能（コミット済みバッチはスキップ＝再書き込みしない）。
async fn run_streaming_import(
    env: &SpannerEnv,
    req: &ImportRequest,
    mock: bool,
    progress: &std::sync::mpsc::Sender<ImportProgress>,
) -> ImportOutcome {
    if req.columns.is_empty() {
        return ImportOutcome {
            error: Some("書き込む列がありません".into()),
            ..Default::default()
        };
    }
    // 1 リクエストに詰める行数（セル予算 ÷ 列数）。
    let per_request = (BATCH_CELLS_PER_REQUEST / req.columns.len().max(1)).max(1);

    let (mut source, bytes_total, src_identity) = match open_source(&req.source).await {
        Ok(s) => s,
        Err(e) => {
            return ImportOutcome {
                error: Some(format!("ソースを開けません: {e}")),
                ..Default::default()
            }
        }
    };

    // モック: 書き込まず行数だけ数える。
    if mock {
        let mut streamer = CsvStreamer::new(req.encoding, req.delimiter);
        let mut recs = Vec::new();
        let mut total = 0usize;
        let mut first = true;
        while let Ok(Some(mut bytes)) = source.next_chunk().await {
            if first {
                strip_bom(&mut bytes);
                first = false;
            }
            streamer.push(&bytes, &mut recs);
            total += recs.len();
            recs.clear();
        }
        streamer.finish(&mut recs);
        total += recs.len();
        if req.has_header && total > 0 {
            total -= 1;
        }
        return ImportOutcome {
            written: total,
            total,
            ..Default::default()
        };
    }

    // ドライラン: Spanner に繋がず、全行を型変換して検証だけする。
    if req.dry_run {
        return dry_run_import(req, &mut source, per_request, bytes_total, progress).await;
    }

    // チェックポイント（再開）準備。GCS は URI だけでは中身の同一性を担保できない
    // （同一 URI で中身が差し替わる運用がある）ため、世代/サイズを署名に含める。
    // これにより中身が変われば署名が変わり、古いチェックポイントで誤ってスキップして
    // 新データを取りこぼす（サイレントなデータ欠損）ことを防ぐ。
    let mut sig = import_signature(req, per_request);
    if let Some(id) = &src_identity {
        sig.push_str(&format!("\tsrcid={id}"));
    }
    let ckpt_path = checkpoint_path(&sig);
    let committed: HashMap<usize, (u64, usize)> = if req.fresh {
        HashMap::new()
    } else {
        load_checkpoint(&ckpt_path, &sig)
    };
    let resuming = !committed.is_empty();
    // 連続コミット済みの先頭部分（バッチ数 k・終端バイト位置・累積行数）。ここまで
    // シークして読み飛ばす。飛び石コミット（k 番目以降）は committed で再送スキップ。
    let (mut resume_batch, mut resume_offset, mut resume_rows) =
        contiguous_resume_point(&committed);
    // 既存の続きから（resuming）なら追記、それ以外は新規作成。
    let ckpt = Arc::new(CheckpointWriter::open(&ckpt_path, &sig, !resuming));
    // リジェクト（スキップ行）出力。
    let rej_path = reject_path(&sig);
    let reject = Arc::new(RejectWriter::new(rej_path.clone(), req.delimiter));
    let skipped = Arc::new(AtomicUsize::new(0));
    // 再開時は、途中まで入ったバッチを安全に再送するため上書き挿入に切り替える（冪等）。
    let mut effective_req = req.clone();
    if resuming {
        effective_req.mode = ImportMode::InsertOrUpdate;
    }

    // 接続 + セッション（並列数ぶん）。
    let database = database_path(env);
    let mut client = match connect_spanner(&database).await {
        Ok(c) => c,
        Err(e) => {
            return ImportOutcome {
                error: Some(format!("接続/認証に失敗: {e}")),
                ..Default::default()
            }
        }
    };
    let concurrency = import_concurrency();
    let sessions = match batch_create_sessions(&mut client, &database, concurrency).await {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => {
            return ImportOutcome {
                error: Some("セッションを作成できませんでした".into()),
                ..Default::default()
            }
        }
        Err(e) => {
            return ImportOutcome {
                error: Some(format!("セッション作成に失敗: {e}")),
                ..Default::default()
            }
        }
    };
    let session_names: Vec<String> = sessions.iter().map(|s| s.name.clone()).collect();

    // 証跡レポート用に取込前の行数を控える（ベストエフォート）。
    let before_count = count_rows(env, &req.table).await;

    // 共有状態。
    let written = Arc::new(AtomicUsize::new(0));
    // CSV の期待列数（ヘッダ幅 / 先頭データ行）。プロデューサが設定しワーカーが参照する。
    let expected_cols = Arc::new(AtomicUsize::new(0));
    let bytes_done = Arc::new(AtomicU64::new(0)); // 読み出した累積バイト数（進捗用）
    let abort = Arc::new(AtomicBool::new(false));
    let first_error: Arc<tokio::sync::Mutex<Option<String>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let shared_req = Arc::new(effective_req);

    // バッチ用バウンドチャネル（バックプレッシャ）。
    // 要素は (バッチ index, 先頭行番号, 終端バイト位置, 累積行数, 行)。
    let (tx, rx) =
        tokio::sync::mpsc::channel::<(usize, usize, u64, usize, Vec<Vec<String>>)>(
            (concurrency * 2).max(2),
        );
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    // ワーカー（セッションごとに 1 つ）。
    let skip = shared_req.skip_bad_rows;
    let mut workers = Vec::with_capacity(session_names.len());
    for sname in session_names.iter() {
        let sname = sname.clone();
        let mut wclient = client.clone();
        let database = database.clone();
        let rx = rx.clone();
        let written = written.clone();
        let bytes_done = bytes_done.clone();
        let abort = abort.clone();
        let first_error = first_error.clone();
        let req = shared_req.clone();
        let progress = progress.clone();
        let ckpt = ckpt.clone();
        let reject = reject.clone();
        let skipped = skipped.clone();
        let expected_cols = expected_cols.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let item = { rx.lock().await.recv().await };
                let Some((batch_idx, start_line, end_off, end_row, rows)) = item else {
                    break;
                };
                if abort.load(Ordering::Relaxed) {
                    continue; // 失敗/中断後は残りをドレインするだけ
                }
                // 型変換（ワーカー間で並列）。不正行は skip 設定なら除外して記録。
                let mut groups = Vec::with_capacity(rows.len());
                let mut kept: Vec<&Vec<String>> = Vec::with_capacity(rows.len());
                let mut conv_err = None;
                let exp = expected_cols.load(Ordering::Relaxed);
                for (off, row) in rows.iter().enumerate() {
                    match check_row_width(row, exp).and_then(|_| build_mutation(&req, row)) {
                        Ok(m) => {
                            groups.push(MutationGroup { mutations: vec![m] });
                            kept.push(row);
                        }
                        Err(e) => {
                            if skip {
                                reject.reject(&format!("{} 行目: {e}", start_line + off), row);
                                skipped.fetch_add(1, Ordering::Relaxed);
                            } else {
                                conv_err = Some(format!("{} 行目: {e}", start_line + off));
                                break;
                            }
                        }
                    }
                }
                if let Some(e) = conv_err {
                    set_first_error(&first_error, e).await;
                    abort.store(true, Ordering::Relaxed);
                    continue;
                }
                if groups.is_empty() {
                    // 全行スキップ → このバッチは完了扱い（再送不要）。
                    ckpt.mark(batch_idx, end_off, end_row);
                    continue;
                }
                let n = groups.len();
                // 書き込み。一過性エラーは指数バックオフでリトライし、
                // リトライ時は冪等な上書き挿入で再送する（重複を無害化）。
                let mut attempt = 0usize;
                let mut next_groups = Some(groups);
                let outcome = loop {
                    let g = next_groups.take().expect("groups present");
                    match write_groups(&mut wclient, &database, &sname, g).await {
                        Ok(res) => break Ok(res),
                        Err(status) => {
                            let stop =
                                abort.load(Ordering::Relaxed) || req.cancel.load(Ordering::Relaxed);
                            if is_retryable(&status) && attempt < RETRY_MAX && !stop {
                                attempt += 1;
                                // 待機中も中断要求に即応する（最大 5s 固まらない）。
                                cancellable_sleep(retry_delay(attempt, &sname), &abort, &req.cancel)
                                    .await;
                                if abort.load(Ordering::Relaxed)
                                    || req.cancel.load(Ordering::Relaxed)
                                {
                                    break Err(status); // 待機中に中断
                                }
                                // 既に入った行は上書きされるだけなので安全に再送。
                                next_groups = Some(
                                    kept.iter()
                                        .filter_map(|row| {
                                            build_mutation_with_mode(
                                                &req,
                                                row,
                                                ImportMode::InsertOrUpdate,
                                            )
                                            .ok()
                                            .map(|m| MutationGroup { mutations: vec![m] })
                                        })
                                        .collect(),
                                );
                                continue;
                            }
                            break Err(status);
                        }
                    }
                };
                match outcome {
                    Ok((ok, group_err)) => {
                        written.fetch_add(ok.len(), Ordering::Relaxed);
                        let all_ok = group_err.is_none() && ok.len() == n;
                        if all_ok {
                            ckpt.mark(batch_idx, end_off, end_row); // 完全コミット → 再開用に記録
                        } else if skip {
                            // 失敗グループの行をリジェクトに記録して続行。
                            for (i, row) in kept.iter().enumerate() {
                                if !ok.contains(&i) {
                                    reject.reject("Spanner 書き込み失敗", row);
                                    skipped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            // スキップ分も含め処理済みとして記録（再送しない）。
                            ckpt.mark(batch_idx, end_off, end_row);
                        } else if let Some(e) = group_err {
                            set_first_error(&first_error, e).await;
                            abort.store(true, Ordering::Relaxed);
                        }
                        let _ = progress.send(ImportProgress::Progress {
                            id: req.id,
                            written: written.load(Ordering::Relaxed),
                            bytes_done: bytes_done.load(Ordering::Relaxed),
                            bytes_total,
                        });
                    }
                    Err(status) => {
                        let msg = format!("BatchWrite に失敗（{attempt} 回リトライ後）: {status}");
                        if skip {
                            // skip モードでは中断せず、この バッチをリジェクト記録して続行。
                            for row in kept.iter() {
                                reject.reject(&msg, row);
                                skipped.fetch_add(1, Ordering::Relaxed);
                            }
                            ckpt.mark(batch_idx, end_off, end_row);
                        } else {
                            set_first_error(&first_error, msg).await;
                            abort.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }

    // ── 再開シーク: 連続コミット済みの終端バイト位置まで読み飛ばす ──
    if resume_offset > 0 {
        let ok = match &req.source {
            ImportSource::File(_) => {
                if let ByteSource::File(f) = &mut source {
                    use tokio::io::AsyncSeekExt;
                    f.seek(std::io::SeekFrom::Start(resume_offset)).await.is_ok()
                } else {
                    false
                }
            }
            ImportSource::Gcs(uri) => match gcs_get_stream_range(uri, resume_offset).await {
                Ok(resp) => {
                    source = ByteSource::Gcs(resp);
                    true
                }
                Err(_) => false,
            },
        };
        if !ok {
            // シーク失敗 → 安全に最初から（committed で再送スキップにフォールバック）。
            // resume_rows も 0 に戻し、resumed はスキップループ側で数える。
            resume_batch = 0;
            resume_offset = 0;
            resume_rows = 0;
        }
    }

    // プロデューサ: ソースをストリーミングし、per_request 行ごとに送る。
    let mut streamer = CsvStreamer::new(req.encoding, req.delimiter);
    if resume_offset > 0 {
        // 再開地点は BOM・ヘッダより後なので BOM 処理は不要。
        streamer.bom_checked = true;
    }
    let mut recs: Vec<Vec<String>> = Vec::new();
    let mut rec_offs: Vec<u64> = Vec::new(); // recs と 1:1 のレコード終端バイト位置
    let mut batch: Vec<Vec<String>> = Vec::with_capacity(per_request);
    let mut header_skipped = !req.has_header || resume_offset > 0;
    // 行番号（1 始まり）。再開時はシークで飛ばした行数を足す。
    let mut next_line = resume_rows + 1;
    let mut start_line = next_line; // 現バッチ先頭の行番号
    let mut processed = resume_rows; // データ行の総数（シーク済み＋送出＋スキップ）
    let mut batch_idx = resume_batch; // 続きのバッチ番号から
    let mut resumed = resume_rows; // シークで飛ばした行数
    let mut first_chunk = resume_offset == 0; // 再開時は BOM 除去しない
    let mut bom_len: u64 = 0; // 先頭で除去した BOM のバイト数（絶対位置補正）
    let base_offset = resume_offset; // 絶対位置 = base_offset + bom_len + streamer内オフセット
    if resume_offset > 0 {
        bytes_done.store(resume_offset, Ordering::Relaxed);
    }
    let mut producer_err: Option<String> = None;

    // 進捗を 1 つ送るヘルパ。bytes ベースの割合は、バウンドチャネルで読み出しが
    // 書き込みに追従するため実際の取込進捗とほぼ一致する。
    let emit = || {
        let _ = progress.send(ImportProgress::Progress {
            id: req.id,
            written: written.load(Ordering::Relaxed),
            bytes_done: bytes_done.load(Ordering::Relaxed),
            bytes_total,
        });
    };
    emit(); // 開始（0%）

    'produce: loop {
        // 中断要求があれば停止（abort にも反映してワーカーを止める）。
        if req.cancel.load(Ordering::Relaxed) {
            abort.store(true, Ordering::Relaxed);
        }
        if abort.load(Ordering::Relaxed) {
            break;
        }
        let chunk = match source.next_chunk().await {
            Ok(Some(mut b)) => {
                // BOM 除去前のバイト数で位置を数える。
                bytes_done.fetch_add(b.len() as u64, Ordering::Relaxed);
                if first_chunk {
                    let before = b.len();
                    strip_bom(&mut b);
                    bom_len = (before - b.len()) as u64;
                    first_chunk = false;
                }
                b
            }
            Ok(None) => break,
            Err(e) => {
                producer_err = Some(format!("読み込みに失敗: {e}"));
                break;
            }
        };
        streamer.push_tracked(&chunk, &mut recs, &mut rec_offs);
        let drained: Vec<(Vec<String>, u64)> =
            recs.drain(..).zip(rec_offs.drain(..)).collect();
        for (rec, roff) in drained {
            if !header_skipped {
                header_skipped = true;
                expected_cols.store(rec.len(), Ordering::Relaxed);
                continue;
            }
            // ヘッダ無しのときは先頭データ行の列数を期待値にする（既定値 0 のときだけ）。
            let _ =
                expected_cols.compare_exchange(0, rec.len(), Ordering::Relaxed, Ordering::Relaxed);
            if batch.is_empty() {
                start_line = next_line;
            }
            batch.push(rec);
            next_line += 1;
            if batch.len() >= per_request {
                let n = batch.len();
                processed += n;
                // このバッチ末尾レコードの絶対終端バイト位置（次バッチの開始位置）と累積行数。
                let end_off = base_offset + bom_len + roff;
                let end_row = processed;
                if committed.contains_key(&batch_idx) {
                    // 前回コミット済み（飛び石分） → 再送しない。resumed として数え、
                    // written には含めない（証跡レポートの更新数推定で二重計上しないため）。
                    resumed += n;
                    batch.clear();
                } else if tx
                    .send((batch_idx, start_line, end_off, end_row, std::mem::take(&mut batch)))
                    .await
                    .is_err()
                {
                    break 'produce;
                }
                batch_idx += 1;
            }
        }
        emit(); // チャンクごとに進捗更新
    }
    // 末尾レコードを送る（エラー/中断時は送らない）。
    if producer_err.is_none() && !abort.load(Ordering::Relaxed) {
        streamer.finish_tracked(&mut recs, &mut rec_offs);
        let mut last_off: u64 = base_offset; // 末尾バッチの終端位置
        let drained: Vec<(Vec<String>, u64)> =
            recs.drain(..).zip(rec_offs.drain(..)).collect();
        for (rec, roff) in drained {
            if !header_skipped {
                header_skipped = true;
                expected_cols.store(rec.len(), Ordering::Relaxed);
                continue;
            }
            // ヘッダ無しのときは先頭データ行の列数を期待値にする（既定値 0 のときだけ）。
            let _ =
                expected_cols.compare_exchange(0, rec.len(), Ordering::Relaxed, Ordering::Relaxed);
            if batch.is_empty() {
                start_line = next_line;
            }
            batch.push(rec);
            next_line += 1;
            last_off = base_offset + bom_len + roff;
        }
        if !batch.is_empty() {
            let n = batch.len();
            processed += n;
            if committed.contains_key(&batch_idx) {
                // resumed のみ。written には含めない（上の主ループと同じ理由）。
                resumed += n;
                batch.clear();
            } else {
                let _ = tx
                    .send((batch_idx, start_line, last_off, processed, std::mem::take(&mut batch)))
                    .await;
            }
        }
    }
    drop(tx); // チャネルを閉じてワーカーを終了させる

    for w in workers {
        let _ = w.await;
    }

    // セッション後始末（失敗は無視）。
    for name in &session_names {
        let _ = delete_session(&mut client, &database, name).await;
    }

    let cancelled = req.cancel.load(Ordering::Relaxed);
    let aborted = abort.load(Ordering::Relaxed);
    let mut error = first_error.lock().await.take();
    if error.is_none() && !cancelled {
        error = producer_err;
    }
    // 全行成功（エラー無し・中断無し・キャンセル無し）ならチェックポイントを消す。
    // 失敗/中断時は残し、次回は続きから再開できるようにする。
    if error.is_none() && !aborted && !cancelled {
        let _ = std::fs::remove_file(&ckpt_path);
    }
    // 取込後の行数（ベストエフォート）。新規挿入 ≈ after-before。
    let after_count = count_rows(env, &req.table).await;
    let skipped_n = skipped.load(Ordering::Relaxed);
    ImportOutcome {
        written: written.load(Ordering::Relaxed),
        total: processed,
        resumed,
        skipped: skipped_n,
        cancelled,
        reject_path: if reject.had_any() {
            Some(rej_path.display().to_string())
        } else {
            None
        },
        error,
        before_count,
        after_count,
        ..Default::default()
    }
}

/// ドライラン: Spanner に繋がず、全行を型変換して検証する。
async fn dry_run_import(
    req: &ImportRequest,
    source: &mut ByteSource,
    _per_request: usize,
    bytes_total: Option<u64>,
    progress: &std::sync::mpsc::Sender<ImportProgress>,
) -> ImportOutcome {
    let mut streamer = CsvStreamer::new(req.encoding, req.delimiter);
    let mut recs: Vec<Vec<String>> = Vec::new();
    let mut header_skipped = !req.has_header;
    let mut next_line = 1usize;
    let mut total = 0usize; // データ行数
    let mut valid = 0usize; // 書き込めると判定した行数
    let mut skipped = 0usize; // 型エラー行数
    let mut expected_cols = 0usize; // 期待列数（ヘッダ幅 / 先頭データ行）
    let mut bytes_done: u64 = 0;
    let mut first_chunk = true;
    let mut error: Option<String> = None;

    let emit = |valid: usize, bytes_done: u64| {
        let _ = progress.send(ImportProgress::Progress {
            id: req.id,
            written: valid,
            bytes_done,
            bytes_total,
        });
    };
    emit(0, 0);

    'outer: loop {
        if req.cancel.load(Ordering::Relaxed) {
            break;
        }
        let chunk = match source.next_chunk().await {
            Ok(Some(mut b)) => {
                bytes_done += b.len() as u64;
                if first_chunk {
                    strip_bom(&mut b);
                    first_chunk = false;
                }
                b
            }
            Ok(None) => break,
            Err(e) => {
                error = Some(format!("読み込みに失敗: {e}"));
                break;
            }
        };
        streamer.push(&chunk, &mut recs);
        for rec in recs.drain(..) {
            if !header_skipped {
                header_skipped = true;
                expected_cols = rec.len();
                continue;
            }
            if expected_cols == 0 {
                expected_cols = rec.len();
            }
            total += 1;
            match check_row_width(&rec, expected_cols).and_then(|_| build_mutation(req, &rec)) {
                Ok(_) => valid += 1,
                Err(e) => {
                    skipped += 1;
                    if !req.skip_bad_rows && error.is_none() {
                        error = Some(format!("{next_line} 行目: {e}"));
                        break 'outer;
                    }
                }
            }
            next_line += 1;
        }
        emit(valid, bytes_done);
    }
    if error.is_none() {
        streamer.finish(&mut recs);
        for rec in recs.drain(..) {
            if !header_skipped {
                header_skipped = true;
                expected_cols = rec.len();
                continue;
            }
            if expected_cols == 0 {
                expected_cols = rec.len();
            }
            total += 1;
            match check_row_width(&rec, expected_cols).and_then(|_| build_mutation(req, &rec)) {
                Ok(_) => valid += 1,
                Err(e) => {
                    skipped += 1;
                    if !req.skip_bad_rows && error.is_none() {
                        error = Some(format!("{next_line} 行目: {e}"));
                    }
                }
            }
            next_line += 1;
        }
    }
    ImportOutcome {
        written: valid,
        total,
        skipped,
        cancelled: req.cancel.load(Ordering::Relaxed),
        dry_run: true,
        error,
        ..Default::default()
    }
}

/// 最初のエラーだけを保持する（以降は無視）。
async fn set_first_error(slot: &Arc<tokio::sync::Mutex<Option<String>>>, e: String) {
    let mut g = slot.lock().await;
    if g.is_none() {
        *g = Some(e);
    }
}

/// プレビュー用に、生バイト列を指定の文字コード・区切りで先頭 `max_rows` 行だけ
/// パースする（マッピング画面のヘッダ算出に使う）。
pub fn parse_preview(
    bytes: &[u8],
    encoding: Encoding,
    delimiter: u8,
    max_rows: usize,
) -> Vec<Vec<String>> {
    let mut bytes = bytes.to_vec();
    strip_bom(&mut bytes);
    let mut s = CsvStreamer::new(encoding, delimiter);
    let mut out = Vec::new();
    s.push(&bytes, &mut out);
    s.finish(&mut out);
    out.truncate(max_rows);
    out
}

// ── ソースからのバイトストリーミング ──

/// CSV のバイト列を逐次供給するソース。
enum ByteSource {
    File(tokio::fs::File),
    Gcs(reqwest::Response),
}

/// インポートソースを開く。戻り値は (ソース, 全体バイト数, 内容識別子)。
/// 全体バイト数が分かれば進捗の割合表示に使う（不明なら None）。
/// 内容識別子は再開チェックポイントの同一性検証用。File は署名側で len+mtime を
/// 使うため None。GCS は世代(x-goog-generation)とサイズを返す。
async fn open_source(
    src: &ImportSource,
) -> anyhow::Result<(ByteSource, Option<u64>, Option<String>)> {
    match src {
        ImportSource::File(p) => {
            let f = tokio::fs::File::open(p).await?;
            let total = f.metadata().await.ok().map(|m| m.len());
            Ok((ByteSource::File(f), total, None))
        }
        ImportSource::Gcs(uri) => {
            let resp = gcs_get_stream(uri).await?;
            let total = resp.content_length();
            let generation = resp
                .headers()
                .get("x-goog-generation")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let identity = format!("gen={generation}:len={}", total.unwrap_or(0));
            Ok((ByteSource::Gcs(resp), total, Some(identity)))
        }
    }
}

impl ByteSource {
    /// 次のバイトチャンクを返す。末尾なら None。
    async fn next_chunk(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        match self {
            ByteSource::File(f) => {
                let mut buf = vec![0u8; 64 * 1024];
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    Ok(None)
                } else {
                    buf.truncate(n);
                    Ok(Some(buf))
                }
            }
            ByteSource::Gcs(resp) => Ok(resp.chunk().await?.map(|b| b.to_vec())),
        }
    }
}

/// 先頭の UTF-8 BOM を取り除く。
fn strip_bom(bytes: &mut Vec<u8>) {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        bytes.drain(0..3);
    }
}

/// GCS オブジェクトをストリーミング取得する（本文を一括バッファしない）。
pub async fn gcs_get_stream(uri: &str) -> anyhow::Result<reqwest::Response> {
    let (bucket, object) = parse_gs_uri(uri).map_err(|e| anyhow::anyhow!(e))?;
    let provider = gcp_auth::provider().await?;
    let token = provider.token(&[GCS_SCOPE]).await?;
    let encoded = encode_object(&object);
    let url =
        format!("https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}?alt=media");
    let resp = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token.as_str())
        .send()
        .await?
        .error_for_status()?;
    Ok(resp)
}

/// GCS オブジェクトを `start` バイト目から取得する（再開シーク用。Range リクエスト）。
async fn gcs_get_stream_range(uri: &str, start: u64) -> anyhow::Result<reqwest::Response> {
    let (bucket, object) = parse_gs_uri(uri).map_err(|e| anyhow::anyhow!(e))?;
    let provider = gcp_auth::provider().await?;
    let token = provider.token(&[GCS_SCOPE]).await?;
    let encoded = encode_object(&object);
    let url =
        format!("https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}?alt=media");
    let resp = reqwest::Client::new()
        .get(&url)
        .bearer_auth(token.as_str())
        .header("Range", format!("bytes={start}-"))
        .send()
        .await?
        .error_for_status()?;
    Ok(resp)
}

// ── ストリーミング CSV パーサ（バイト単位・全行を溜めない） ──

/// RFC 4180 風のインクリメンタル CSV パーサ。チャンクを `push` するたびに
/// 完成したレコードを `out` に追加する。構造文字（" 区切り CR LF）は ASCII の
/// ため、UTF-8 / Shift-JIS のマルチバイトを壊さずバイト単位で処理できる
/// （Shift-JIS の後続バイトも 0x40 以上で、区切り/改行/" と衝突しない）。
struct CsvStreamer {
    field: Vec<u8>,
    record: Vec<String>,
    in_quotes: bool,
    pending_quote: bool, // クォート内で直前が " （エスケープ "" か閉じか保留）
    swallow_lf: bool,    // 直前が CRLF の CR（続く LF を 1 つ飲む）
    started: bool,       // 現レコードに何か読み始めたか
    bom_checked: bool,   // 先頭フィールドの BOM 除去を済ませたか
    consumed: u64,       // これまでに処理したバイト数（push したバイト基準。再開オフセット用）
    encoding: Encoding,  // フィールドのデコード方式
    delimiter: u8,       // フィールド区切り文字
}

impl Default for CsvStreamer {
    fn default() -> Self {
        CsvStreamer::new(Encoding::Utf8, b',')
    }
}

impl CsvStreamer {
    fn new(encoding: Encoding, delimiter: u8) -> Self {
        CsvStreamer {
            field: Vec::new(),
            record: Vec::new(),
            in_quotes: false,
            pending_quote: false,
            swallow_lf: false,
            started: false,
            bom_checked: false,
            consumed: 0,
            encoding,
            delimiter,
        }
    }

    fn push(&mut self, bytes: &[u8], out: &mut Vec<Vec<String>>) {
        for &b in bytes {
            self.byte(b, out, None);
        }
    }

    /// `push` と同じだが、各レコード確定時の「処理済みバイト数」を `offsets` に
    /// 1:1 で積む（out[i] のレコードは offsets[i] バイト目で終端＝次レコードの開始位置）。
    /// 再開のシーク位置算出に使う。
    fn push_tracked(&mut self, bytes: &[u8], out: &mut Vec<Vec<String>>, offsets: &mut Vec<u64>) {
        for &b in bytes {
            self.byte(b, out, Some(&mut *offsets));
        }
    }

    fn byte(&mut self, b: u8, out: &mut Vec<Vec<String>>, offsets: Option<&mut Vec<u64>>) {
        self.consumed += 1; // すべてのバイトを位置として数える
        if self.swallow_lf {
            self.swallow_lf = false;
            if b == b'\n' {
                return;
            }
        }
        if self.pending_quote {
            self.pending_quote = false;
            if b == b'"' {
                self.field.push(b'"'); // エスケープされた "
                return;
            }
            self.in_quotes = false; // 閉じクォート → このバイトは通常文脈で処理
        }
        if self.in_quotes {
            match b {
                b'"' => self.pending_quote = true,
                _ => self.field.push(b),
            }
            return;
        }
        if b == self.delimiter {
            self.end_field();
            self.started = true;
            return;
        }
        match b {
            // " はフィールド先頭のときだけ引用開始（RFC 4180）。フィールド途中の
            // " はデータ（例: 5'10" / インチ表記）として literal に扱う。こうしないと
            // 途中の " 以降のカンマ・改行まで引用内として飲み込み、行が結合して
            // 列数ズレ・文字列の途切れを起こす。
            b'"' if self.field.is_empty() => {
                self.in_quotes = true;
                self.started = true;
            }
            b'\r' => {
                self.end_record(out, offsets);
                self.swallow_lf = true;
            }
            b'\n' => self.end_record(out, offsets),
            _ => {
                self.field.push(b);
                self.started = true;
            }
        }
    }

    fn end_field(&mut self) {
        let mut s = self.encoding.decode(&self.field);
        self.field.clear();
        // 先頭フィールドの BOM を除去する。チャンク境界で生バイト strip_bom が
        // 取りこぼしても（GCS の極小先頭チャンク等）、デコード後の U+FEFF をここで落とす。
        if !self.bom_checked {
            self.bom_checked = true;
            if let Some(rest) = s.strip_prefix('\u{feff}') {
                s = rest.to_string();
            }
        }
        self.record.push(s);
    }

    fn end_record(&mut self, out: &mut Vec<Vec<String>>, offsets: Option<&mut Vec<u64>>) {
        self.end_field();
        let rec = std::mem::take(&mut self.record);
        // 全列が空（空行）は捨てる。
        if !(rec.len() == 1 && rec[0].is_empty()) {
            out.push(rec);
            // 確定レコードの終端バイト位置（次レコードの開始位置）を記録する。
            if let Some(o) = offsets {
                o.push(self.consumed);
            }
        }
        self.started = false;
    }

    /// 末尾の改行が無い場合の最終レコードを確定する。
    fn finish(&mut self, out: &mut Vec<Vec<String>>) {
        if self.started || !self.field.is_empty() || !self.record.is_empty() {
            self.end_record(out, None);
        }
    }

    /// `finish` のオフセット追跡版。
    fn finish_tracked(&mut self, out: &mut Vec<Vec<String>>, offsets: &mut Vec<u64>) {
        if self.started || !self.field.is_empty() || !self.record.is_empty() {
            self.end_record(out, Some(offsets));
        }
    }
}

// ── BatchWrite による高スループット投入 ──

const SPANNER_DOMAIN: &str = "spanner.googleapis.com";
const SPANNER_AUDIENCE: &str = "https://spanner.googleapis.com/";
const SPANNER_SCOPES: [&str; 2] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spanner.data",
];

/// 1 回の BatchWrite リクエストに詰めるセル数（行数 × 列数）の目安。
/// Commit 経路より大きめに取り、ストリーミング 1 往復で多数の行を流す。
/// Spanner の 1 リクエストあたりミューテーション上限に余裕を持って収める。
const BATCH_CELLS_PER_REQUEST: usize = 20_000;

/// `gs://` ではなく Spanner のデータベースリソース名を組み立てる。
fn database_path(env: &SpannerEnv) -> String {
    format!(
        "projects/{}/instances/{}/databases/{}",
        env.project, env.instance, env.database
    )
}

/// gcloud-gax の接続スタックで認証済み SpannerClient を作る。
/// SPANNER_EMULATOR_HOST があればエミュレータへ、なければ本番へ繋ぐ。
async fn connect_spanner(database: &str) -> anyhow::Result<SpannerClient<GaxChannel>> {
    let environment = match std::env::var("SPANNER_EMULATOR_HOST") {
        Ok(host) if !host.is_empty() => Environment::Emulator(host),
        _ => {
            let auth_config = gcloud_spanner::client::google_cloud_auth::project::Config::default()
                .with_audience(SPANNER_AUDIENCE)
                .with_scopes(&SPANNER_SCOPES);
            let ts =
                gcloud_spanner::client::google_cloud_auth::token::DefaultTokenSourceProvider::new(
                    auth_config,
                )
                .await?;
            Environment::GoogleCloud(Box::new(ts))
        }
    };
    let cm = GaxConnManager::new(
        1,
        SPANNER_DOMAIN,
        SPANNER_AUDIENCE,
        &environment,
        &ConnectionOptions::default(),
    )
    .await?;
    let _ = database; // ルーティングヘッダは各リクエストで付与する
    Ok(SpannerClient::new(cm.conn()))
}

/// ルーティング用メタデータ（x-goog-request-params と resource-prefix）を付けて
/// リクエストを組み立てる。
fn routed_request<T>(param: String, database: &str, body: T) -> google_cloud_gax::grpc::Request<T> {
    let mut req = google_cloud_gax::create_request(param, body);
    // database は projects/.../databases/... の ASCII なので parse は失敗しない。
    req.metadata_mut()
        .append("google-cloud-resource-prefix", database.parse().unwrap());
    req
}

/// 並列ぶんのセッションをまとめて作成する。
async fn batch_create_sessions(
    client: &mut SpannerClient<GaxChannel>,
    database: &str,
    count: usize,
) -> anyhow::Result<Vec<Session>> {
    let body = BatchCreateSessionsRequest {
        database: database.to_string(),
        session_template: None,
        session_count: count as i32,
    };
    let request = routed_request(format!("database={database}"), database, body);
    Ok(client
        .batch_create_sessions(request)
        .await?
        .into_inner()
        .session)
}

/// 1 リクエスト分のグループを BatchWrite する。
/// 戻り値: (適用に成功したグループ数, 最初のグループ失敗メッセージ)。
/// 1 リクエスト分のグループを BatchWrite する。
/// 戻り値: (成功したグループ index の集合, 最初のグループ失敗メッセージ)。
/// BatchWrite を 1 回実行する。RPC 失敗は gRPC ステータスをそのまま返す（リトライ判定用）。
async fn write_groups(
    client: &mut SpannerClient<GaxChannel>,
    database: &str,
    session: &str,
    groups: Vec<MutationGroup>,
) -> Result<(HashSet<usize>, Option<String>), google_cloud_gax::grpc::Status> {
    let body = BatchWriteRequest {
        session: session.to_string(),
        request_options: None,
        mutation_groups: groups,
        exclude_txn_from_change_streams: false,
    };
    let request = routed_request(format!("session={session}"), database, body);
    let resp = client.batch_write(request).await?;
    let mut stream = resp.into_inner();
    let mut ok: HashSet<usize> = HashSet::new();
    let mut group_err = None;
    while let Some(r) = stream.message().await? {
        let code = r.status.as_ref().map(|s| s.code).unwrap_or(0);
        if code == 0 {
            ok.extend(r.indexes.iter().map(|i| *i as usize));
        } else if is_retryable_code(code) {
            // 一過性のグループ失敗（ABORTED 等）は、RPC エラーとして返して
            // 既存の全体リトライ（冪等な上書き再送）に載せる。黙って捨てない。
            let msg = r.status.map(|s| s.message).unwrap_or_default();
            return Err(google_cloud_gax::grpc::Status::new(
                google_cloud_gax::grpc::Code::from(code),
                msg,
            ));
        } else if group_err.is_none() {
            group_err = r.status.map(|s| format!("group 失敗: {}", s.message));
        }
    }
    Ok((ok, group_err))
}

/// 一過性（リトライ可）の gRPC ステータスか。
fn is_retryable(status: &google_cloud_gax::grpc::Status) -> bool {
    use google_cloud_gax::grpc::Code;
    matches!(
        status.code(),
        Code::Unavailable | Code::Aborted | Code::DeadlineExceeded | Code::ResourceExhausted
    )
}

/// 一過性（リトライ可）の gRPC コード（数値）か。BatchWrite のグループ別 status は
/// google.rpc.Status の i32 コードで返るため、こちらで判定する。
/// DEADLINE_EXCEEDED=4, RESOURCE_EXHAUSTED=8, ABORTED=10, UNAVAILABLE=14。
fn is_retryable_code(code: i32) -> bool {
    matches!(code, 4 | 8 | 10 | 14)
}

/// 指定時間だけ待つが、abort/cancel が立ったら即座に返る。
/// リトライのバックオフ待機中でも中断要求に素早く応答するため。
async fn cancellable_sleep(dur: std::time::Duration, abort: &AtomicBool, cancel: &AtomicBool) {
    let step = std::time::Duration::from_millis(100);
    let mut left = dur;
    while left > std::time::Duration::ZERO {
        if abort.load(Ordering::Relaxed) || cancel.load(Ordering::Relaxed) {
            return;
        }
        let s = step.min(left);
        tokio::time::sleep(s).await;
        left = left.saturating_sub(s);
    }
}

/// リトライ回数の上限と、attempt(1始まり)・ワーカー名からのバックオフ待ち時間。
const RETRY_MAX: usize = 5;

fn retry_delay(attempt: usize, session: &str) -> std::time::Duration {
    use std::hash::{Hash, Hasher};
    // 100ms から指数増（上限 5s）。
    let base = 100u64;
    let exp = base.saturating_mul(1u64 << (attempt.saturating_sub(1)).min(6));
    let capped = exp.min(5000);
    // フルジッタ風: capped/2 + (0..capped/2)。種は (session, attempt)。
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (session, attempt).hash(&mut h);
    let frac = h.finish() % 1000;
    let jitter = (capped / 2) * frac / 1000;
    std::time::Duration::from_millis(capped / 2 + jitter)
}

/// セッションを破棄する（失敗しても無視可能）。
async fn delete_session(
    client: &mut SpannerClient<GaxChannel>,
    database: &str,
    name: &str,
) -> anyhow::Result<()> {
    let body = DeleteSessionRequest {
        name: name.to_string(),
    };
    let request = routed_request(format!("name={name}"), database, body);
    client.delete_session(request).await?;
    Ok(())
}

/// CSV の 1 行を、列の型に合わせた 1 ミューテーションに変換する（req.mode を使う）。
/// 行の列数が期待値（ヘッダ幅 / 先頭データ行の列数）と一致するか検証する。
/// expected==0（未確定）なら検査しない。区切り/引用符のズレで列がずれた壊れた行を
/// 黙って NULL 埋め・切り捨てせずに弾くためのもの。
fn check_row_width(rec: &[String], expected: usize) -> Result<(), String> {
    if expected != 0 && rec.len() != expected {
        Err(format!(
            "列数が一致しません（期待 {} 列, 実際 {} 列）。区切り文字や引用符のズレの可能性があります",
            expected,
            rec.len()
        ))
    } else {
        Ok(())
    }
}

fn build_mutation(
    req: &ImportRequest,
    row: &[String],
) -> Result<google_cloud_googleapis::spanner::v1::Mutation, String> {
    build_mutation_with_mode(req, row, req.mode)
}

/// 書き込み方式を明示してミューテーションを作る（リトライ時は冪等な上書き挿入を指定）。
fn build_mutation_with_mode(
    req: &ImportRequest,
    row: &[String],
    mode: ImportMode,
) -> Result<google_cloud_googleapis::spanner::v1::Mutation, String> {
    let mut boxed: Vec<Box<dyn ToKind>> = Vec::with_capacity(req.columns.len());
    for col in req.columns.iter() {
        // 列ごとに対応する CSV 列インデックスから値を取る。
        let raw = row.get(col.src_index).map(String::as_str).unwrap_or("");
        let v = convert_cell(raw, &col.ty, req.empty_as_null, req.null_token.as_deref())
            .map_err(|e| format!("列 '{}': {e}", col.name))?;
        boxed.push(v);
    }
    let names: Vec<&str> = req.columns.iter().map(|c| c.name.as_str()).collect();
    let refs: Vec<&dyn ToKind> = boxed.iter().map(|b| b.as_ref()).collect();
    Ok(match mode {
        ImportMode::Insert => insert(&req.table, &names, &refs),
        ImportMode::InsertOrUpdate => insert_or_update(&req.table, &names, &refs),
    })
}

/// 既定で NULL とみなすプレースホルダか（DataGrip/DBeaver 等の `<null>` / `(null)`）。
/// 大文字小文字は区別しない。`null_token` 設定とは独立に常に効く。
fn is_default_null_token(value: &str) -> bool {
    value.eq_ignore_ascii_case("<null>") || value.eq_ignore_ascii_case("(null)")
}

/// 文字列セルを、Spanner の型に合わせた値（ToKind）へ変換する。
/// 数値・真偽はパースして型付きで送り、それ以外は文字列のまま送る
/// （NUMERIC / TIMESTAMP / DATE / BYTES(base64) / JSON などは文字列表現で受理される）。
fn convert_cell(
    value: &str,
    ty: &str,
    empty_as_null: bool,
    null_token: Option<&str>,
) -> Result<Box<dyn ToKind>, String> {
    let t = ty.trim().to_uppercase();
    // 配列・構造体は本ツールでは未対応。
    if t.starts_with("ARRAY") || t.starts_with("STRUCT") {
        return Err(format!("未対応の型です: {ty}"));
    }
    // NULL トークン一致 / 既定プレースホルダ(<null>,(null)) / 空欄 を NULL 扱い。
    if null_token.is_some_and(|tok| value == tok)
        || is_default_null_token(value)
        || (empty_as_null && value.is_empty())
    {
        return Ok(Box::new(None::<String>));
    }
    if t.starts_with("BOOL") {
        let b = match value.trim().to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" | "y" => true,
            "false" | "0" | "f" | "no" | "n" => false,
            _ => return Err(format!("BOOL に変換できません: '{value}'")),
        };
        Ok(Box::new(b))
    } else if t.starts_with("INT64") {
        let i: i64 = value
            .trim()
            .parse()
            .map_err(|_| format!("INT64 に変換できません: '{value}'"))?;
        Ok(Box::new(i))
    } else if t.starts_with("FLOAT") {
        let f: f64 = value
            .trim()
            .parse()
            .map_err(|_| format!("FLOAT に変換できません: '{value}'"))?;
        Ok(Box::new(f))
    } else {
        // STRING / BYTES / NUMERIC / TIMESTAMP / DATE / JSON など。
        Ok(Box::new(value.to_string()))
    }
}

/// パニックしたタスクの JoinError から、UI 表示用のメッセージを作る。
fn panic_message(err: tokio::task::JoinError) -> String {
    if err.is_cancelled() {
        return "処理がキャンセルされました".into();
    }
    let payload = err.into_panic();
    let detail = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    if detail.is_empty() {
        "処理が異常終了しました（パニック）".into()
    } else {
        format!("処理が異常終了しました: {detail}")
    }
}

/// クライアントを遅延生成して借用を返す。接続先 env が変わっていれば作り直す。
async fn ensure_client<'a>(
    cache: &'a mut Option<(SpannerEnv, Client)>,
    env: &SpannerEnv,
) -> Result<&'a Client, String> {
    let stale = cache.as_ref().map(|(e, _)| e != env).unwrap_or(true);
    if stale {
        match build_client(env).await {
            Ok(c) => *cache = Some((env.clone(), c)),
            Err(e) => return Err(format!("接続/認証に失敗: {e}")),
        }
    }
    Ok(&cache.as_ref().unwrap().1)
}

async fn ensure_and_run(
    cache: &mut Option<(SpannerEnv, Client)>,
    env: &SpannerEnv,
    sql: &str,
) -> QueryOutcome {
    // 文の種類で実行経路を分ける。Spanner では DDL は Admin API、DML は読み書き
    // トランザクション、それ以外（SELECT 等）は読み取りスナップショットで実行する。
    match statement_kind(sql) {
        // DDL は Admin API（UpdateDatabaseDdl）。データ用クライアントでは実行できない。
        StatementKind::Ddl => run_ddl(env, sql).await,
        StatementKind::Dml => match ensure_client(cache, env).await {
            Ok(c) => run_dml(c, sql).await,
            Err(e) => QueryOutcome {
                error: Some(e),
                ..Default::default()
            },
        },
        StatementKind::Query => match ensure_client(cache, env).await {
            Ok(c) => run_query(c, sql).await,
            Err(e) => QueryOutcome {
                error: Some(e),
                ..Default::default()
            },
        },
    }
}

/// SQL 文の種類（実行経路の振り分け用）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementKind {
    /// SELECT / WITH など読み取り。
    Query,
    /// CREATE / ALTER / DROP / RENAME / GRANT / REVOKE など。
    Ddl,
    /// INSERT / UPDATE / DELETE。
    Dml,
}

/// 先頭キーワードから文の種類を判定する（簡易）。
fn statement_kind(sql: &str) -> StatementKind {
    // 先頭の語を取り出して大文字化（記号・空白・( で区切る）。
    let word: String = sql
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase();
    match word.as_str() {
        "CREATE" | "ALTER" | "DROP" | "RENAME" | "GRANT" | "REVOKE" | "ANALYZE" => {
            StatementKind::Ddl
        }
        "INSERT" | "UPDATE" | "DELETE" => StatementKind::Dml,
        _ => StatementKind::Query,
    }
}

/// 複数 DDL を `;` で分割する（空文・末尾セミコロンは無視）。
fn split_ddl_statements(sql: &str) -> Vec<String> {
    sql.split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// DDL を Admin API で実行する（CREATE/ALTER/DROP …）。複数文（;区切り）対応。
async fn run_ddl(env: &SpannerEnv, sql: &str) -> QueryOutcome {
    let start = Instant::now();
    let statements = split_ddl_statements(sql);
    if statements.is_empty() {
        return QueryOutcome {
            error: Some("実行する DDL がありません".into()),
            ..Default::default()
        };
    }
    let n = statements.len();
    let mut out = match try_run_ddl(env, statements).await {
        Ok(()) => QueryOutcome {
            columns: vec!["結果".into()],
            rows: vec![vec![format!("DDL を適用しました（{n} 文）。スキーマタブで「更新」すると反映されます。")]],
            ..Default::default()
        },
        Err(e) => QueryOutcome {
            error: Some(format!("DDL 実行エラー: {e}")),
            ..Default::default()
        },
    };
    out.elapsed_ms = start.elapsed().as_millis();
    out
}

async fn try_run_ddl(env: &SpannerEnv, statements: Vec<String>) -> anyhow::Result<()> {
    use gcloud_spanner::admin::client::Client as AdminClient;
    use gcloud_spanner::admin::AdminClientConfig;
    use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;

    let cfg = AdminClientConfig::default().with_auth().await?;
    let admin = AdminClient::new(cfg).await?;
    let req = UpdateDatabaseDdlRequest {
        database: database_path(env),
        statements,
        ..Default::default()
    };
    let mut op = admin.database().update_database_ddl(req, None).await?;
    op.wait(None).await?;
    Ok(())
}

/// DML を読み書きトランザクションで実行する（INSERT/UPDATE/DELETE）。影響行数を返す。
async fn run_dml(client: &Client, sql: &str) -> QueryOutcome {
    let start = Instant::now();
    let sql_owned = sql.to_string();
    let result = client
        .read_write_transaction(move |tx| {
            let stmt = Statement::new(sql_owned.clone());
            Box::pin(async move {
                let n = tx.update(stmt).await?;
                Ok::<i64, gcloud_spanner::client::Error>(n)
            })
        })
        .await;
    let mut out = match result {
        Ok((_commit, affected)) => QueryOutcome {
            columns: vec!["影響行数".into()],
            rows: vec![vec![affected.to_string()]],
            ..Default::default()
        },
        Err(e) => QueryOutcome {
            error: Some(format!("DML 実行エラー: {e}")),
            ..Default::default()
        },
    };
    out.elapsed_ms = start.elapsed().as_millis();
    out
}

/// INFORMATION_SCHEMA からテーブル・カラム・依存関係を集めてグラフを作る。
async fn fetch_schema(client: &Client) -> SchemaGraph {
    match try_fetch_schema(client).await {
        Ok(g) => g,
        Err(e) => SchemaGraph {
            error: Some(e.to_string()),
            ..Default::default()
        },
    }
}

async fn try_fetch_schema(client: &Client) -> anyhow::Result<SchemaGraph> {
    // 主キー構成カラム
    let (_, pk_rows, _) = try_query(client, PK_SQL).await?;
    let pk_set: std::collections::HashSet<(String, String)> = pk_rows
        .into_iter()
        .filter_map(|r| Some((r.first()?.clone(), r.get(1)?.clone())))
        .collect();

    // ノード（テーブル + カラム）
    let (_, col_rows, _) = try_query(client, COLUMNS_SQL).await?;
    let mut order: Vec<String> = Vec::new();
    let mut cols: HashMap<String, Vec<Column>> = HashMap::new();
    for r in col_rows {
        let table = r.first().cloned().unwrap_or_default();
        let name = r.get(1).cloned().unwrap_or_default();
        let ty = r.get(2).cloned().unwrap_or_default();
        if !cols.contains_key(&table) {
            order.push(table.clone());
        }
        let pk = pk_set.contains(&(table.clone(), name.clone()));
        cols.entry(table).or_default().push(Column { name, ty, pk });
    }
    // インデックス（(table, index) ごとにカラムを集約）
    let (_, idx_rows, _) = try_query(client, INDEXES_SQL).await?;
    let mut idx_order: HashMap<String, Vec<String>> = HashMap::new(); // table -> [index_name...]
    let mut idx_cols: HashMap<(String, String), (bool, Vec<String>)> = HashMap::new();
    for r in idx_rows {
        let table = r.first().cloned().unwrap_or_default();
        let index = r.get(1).cloned().unwrap_or_default();
        let unique = r.get(2).map(|s| s == "true").unwrap_or(false);
        let col = r.get(3).cloned().unwrap_or_default();
        let key = (table.clone(), index.clone());
        if !idx_cols.contains_key(&key) {
            idx_order.entry(table).or_default().push(index);
        }
        let entry = idx_cols.entry(key).or_insert((unique, Vec::new()));
        entry.0 = unique;
        entry.1.push(col);
    }
    let index_strings = |table: &str| -> Vec<String> {
        idx_order
            .get(table)
            .map(|names| {
                names
                    .iter()
                    .map(|name| {
                        let (unique, c) = idx_cols
                            .get(&(table.to_string(), name.clone()))
                            .cloned()
                            .unwrap_or((false, Vec::new()));
                        let u = if unique { "  UNIQUE" } else { "" };
                        format!("{name} ({}){u}", c.join(", "))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let nodes = order
        .into_iter()
        .map(|name| {
            let columns = cols.remove(&name).unwrap_or_default();
            let indexes = index_strings(&name);
            TableNode {
                name,
                columns,
                indexes,
            }
        })
        .collect();

    // エッジ（依存関係）
    let (_, dep_rows, _) = try_query(client, DEPENDENCY_SQL).await?;
    let edges = dep_rows
        .into_iter()
        .filter_map(|r| {
            if r.len() < 3 {
                return None;
            }
            let kind = if r[1].contains("インターリーブ") {
                EdgeKind::Interleave
            } else {
                EdgeKind::ForeignKey
            };
            Some(Edge {
                from: r[0].clone(),
                to: r[2].clone(),
                kind,
                label: r.get(3).cloned().unwrap_or_default(),
            })
        })
        .collect();

    Ok(SchemaGraph {
        nodes,
        edges,
        error: None,
        ddl: HashMap::new(),
    })
}

/// GetDatabaseDdl で実 DDL を取得し、テーブル名 → 連結 DDL のマップにする
/// （ベストエフォート。失敗時は空マップ → UI 側で近似 DDL にフォールバック）。
async fn fetch_ddl(env: &SpannerEnv) -> HashMap<String, String> {
    (try_fetch_ddl(env).await).unwrap_or_default()
}

async fn try_fetch_ddl(env: &SpannerEnv) -> anyhow::Result<HashMap<String, String>> {
    use gcloud_spanner::admin::client::Client as AdminClient;
    use gcloud_spanner::admin::AdminClientConfig;
    use google_cloud_googleapis::spanner::admin::database::v1::GetDatabaseDdlRequest;

    let cfg = AdminClientConfig::default().with_auth().await?;
    let admin = AdminClient::new(cfg).await?;
    let req = GetDatabaseDdlRequest {
        database: database_path(env),
    };
    let resp = admin.database().get_database_ddl(req, None).await?;
    Ok(group_ddl_by_table(&resp.into_inner().statements))
}

/// DDL 文の一覧を、対象テーブルごとにまとめる。CREATE TABLE / CREATE INDEX … ON /
/// ALTER TABLE を対象テーブルに割り当て、出現順を保って ";\n\n" で連結する。
fn group_ddl_by_table(statements: &[String]) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    for stmt in statements {
        let Some(table) = ddl_target_table(stmt) else {
            continue;
        };
        let entry = map.entry(table).or_default();
        if !entry.is_empty() {
            entry.push_str(";\n\n");
        }
        entry.push_str(stmt.trim());
    }
    for v in map.values_mut() {
        v.push(';');
    }
    map
}

/// DDL 文が対象とするテーブル名を返す（既定スキーマ前提）。
fn ddl_target_table(stmt: &str) -> Option<String> {
    let s = stmt.trim();
    // ASCII 大文字化はバイト長を変えないので、得たオフセットを s にそのまま使える
    // （to_uppercase は ß→SS 等で長さが変わりオフセットがずれる）。
    let upper = s.to_ascii_uppercase();
    if upper.starts_with("CREATE TABLE") {
        return Some(ddl_name_after(s, "CREATE TABLE".len()));
    }
    if upper.starts_with("ALTER TABLE") {
        return Some(ddl_name_after(s, "ALTER TABLE".len()));
    }
    // CREATE [UNIQUE] [NULL_FILTERED] INDEX <idx> ON <table> (...)
    if upper.starts_with("CREATE") && upper.contains(" INDEX ") {
        if let Some(pos) = upper.find(" ON ") {
            return Some(strip_table_token(&s[pos + 4..]));
        }
    }
    None
}

/// キーワード長 `kw_len` の後ろからテーブル名トークンを取り出す（IF NOT EXISTS を読み飛ばす）。
fn ddl_name_after(s: &str, kw_len: usize) -> String {
    let rest = s.get(kw_len..).unwrap_or("").trim_start();
    // char 境界を割らないよう get(..13) で安全に判定する。
    let rest = if rest
        .get(..13)
        .is_some_and(|h| h.eq_ignore_ascii_case("IF NOT EXISTS"))
    {
        rest[13..].trim_start()
    } else {
        rest
    };
    strip_table_token(rest)
}

/// 先頭のテーブル識別子を取り出す（`バッククォート` 付き / 素のどちらにも対応）。
fn strip_table_token(s: &str) -> String {
    let s = s.trim_start();
    if let Some(rest) = s.strip_prefix('`') {
        if let Some(end) = rest.find('`') {
            return rest[..end].to_string();
        }
    }
    s.chars()
        .take_while(|c| !c.is_whitespace() && *c != '(')
        .collect()
}

/// テーブルの行数を COUNT(*) で取得する（証跡レポートの「元件数/新規/更新」用、
/// ベストエフォート。権限不足や接続失敗なら None）。
async fn count_rows(env: &SpannerEnv, table: &str) -> Option<i64> {
    let client = build_client(env).await.ok()?;
    let (_, rows, _) = try_query(&client, &format!("SELECT COUNT(*) FROM `{table}`"))
        .await
        .ok()?;
    rows.first()?.first()?.parse::<i64>().ok()
}

async fn build_client(env: &SpannerEnv) -> anyhow::Result<Client> {
    let db = format!(
        "projects/{}/instances/{}/databases/{}",
        env.project, env.instance, env.database
    );
    let config = ClientConfig::default().with_auth().await?;
    Ok(Client::new(&db, config).await?)
}

async fn run_query(client: &Client, sql: &str) -> QueryOutcome {
    match try_query(client, sql).await {
        Ok((columns, rows, truncated)) => QueryOutcome {
            columns,
            rows,
            truncated,
            error: None,
            ..Default::default()
        },
        Err(e) => QueryOutcome {
            error: Some(e.to_string()),
            ..Default::default()
        },
    }
}

/// クエリの実行計画（PLAN モード）を取得して表示用ツリーに整形する。
async fn run_plan(client: &Client, sql: &str) -> PlanOutcome {
    match try_plan(client, sql).await {
        Ok(lines) if lines.is_empty() => {
            // エミュレータは EXPLAIN/実行計画に未対応で常に空が返る。
            let msg = if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
                "エミュレータは実行計画(EXPLAIN)に未対応です。実 Spanner では取得できます。"
            } else {
                "実行計画が空でした（SELECT 等のクエリで試してください）"
            };
            PlanOutcome {
                error: Some(msg.into()),
                ..Default::default()
            }
        }
        Ok(lines) => PlanOutcome {
            lines,
            ..Default::default()
        },
        Err(e) => PlanOutcome {
            error: Some(e.to_string()),
            ..Default::default()
        },
    }
}

async fn try_plan(client: &Client, sql: &str) -> anyhow::Result<Vec<PlanLine>> {
    use gcloud_spanner::transaction::QueryOptions;
    use google_cloud_googleapis::spanner::v1::execute_sql_request::QueryMode;

    let mut tx = client.single().await?;
    let opt = QueryOptions {
        mode: QueryMode::Plan,
        ..Default::default()
    };
    let mut iter = tx.query_with_option(Statement::new(sql), opt).await?;
    // PLAN モードは行を返さないが、ストリームを最後まで進めて stats を受け取る。
    while iter.next().await?.is_some() {}
    let plan = iter.stats().and_then(|s| s.query_plan.clone());
    Ok(plan.map(|p| build_plan_lines(&p.plan_nodes)).unwrap_or_default())
}

/// 実行計画ノード（pre-order）を child_links でたどって表示用ツリーにする。
fn build_plan_lines(
    nodes: &[google_cloud_googleapis::spanner::v1::PlanNode],
) -> Vec<PlanLine> {
    let mut lines = Vec::new();
    if !nodes.is_empty() {
        walk_plan(nodes, 0, 0, &mut lines);
    }
    lines
}

fn walk_plan(
    nodes: &[google_cloud_googleapis::spanner::v1::PlanNode],
    idx: usize,
    depth: usize,
    lines: &mut Vec<PlanLine>,
) {
    const MAX_LINES: usize = 4000;
    const MAX_DEPTH: usize = 60;
    if lines.len() >= MAX_LINES || depth > MAX_DEPTH {
        return;
    }
    let Some(node) = nodes.get(idx) else {
        return;
    };
    // Kind: Relational=1, Scalar=2。
    let scalar = node.kind == 2;
    lines.push(PlanLine {
        depth,
        name: if node.display_name.is_empty() {
            "(node)".to_string()
        } else {
            node.display_name.clone()
        },
        detail: plan_node_detail(node),
        scalar,
    });
    for cl in &node.child_links {
        let ci = cl.child_index;
        // 子は常に親より後（pre-order）。これを満たさないものは循環防止のため無視。
        if ci > node.index && (ci as usize) < nodes.len() {
            walk_plan(nodes, ci as usize, depth + 1, lines);
        }
    }
}

/// ノードの補足説明（short_representation と metadata の要約）。
fn plan_node_detail(node: &google_cloud_googleapis::spanner::v1::PlanNode) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(sr) = &node.short_representation {
        if !sr.description.is_empty() {
            parts.push(sr.description.clone());
        }
    }
    if let Some(meta) = &node.metadata {
        // scan_target / scan_type などを "key=value" で要約（文字列・数値のみ）。
        let mut keys: Vec<&String> = meta.fields.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(v) = meta.fields.get(k) {
                if let Some(s) = struct_value_string(v) {
                    parts.push(format!("{k}={s}"));
                }
            }
        }
    }
    parts.join("  ·  ")
}

fn struct_value_string(v: &prost_types::Value) -> Option<String> {
    use prost_types::value::Kind;
    match v.kind.as_ref()? {
        Kind::StringValue(s) if !s.is_empty() => Some(s.clone()),
        Kind::NumberValue(n) => Some(format!("{n}")),
        Kind::BoolValue(b) => Some(b.to_string()),
        _ => None,
    }
}

async fn try_query(
    client: &Client,
    sql: &str,
) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>, bool)> {
    let mut tx = client.single().await?;
    let mut iter = tx.query(Statement::new(sql)).await?;

    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut truncated = false;

    while let Some(row) = iter.next().await? {
        if rows.len() >= MAX_ROWS {
            truncated = true;
            break;
        }
        let mut r = Vec::new();
        let mut i = 0;
        // 列数は InvalidColumnIndex で終端検出（型不明でも走査できる）
        while let Some(cell) = stringify_cell(&row, i) {
            r.push(cell);
            i += 1;
        }
        rows.push(r);
    }

    // 列名はメタデータから取得（行が空でも取れる）
    let columns: Vec<String> = iter
        .columns_metadata()
        .iter()
        .map(|f| f.name.clone())
        .collect();

    Ok((columns, rows, truncated))
}

/// 1 セルを型を問わず文字列化する。None は「列の終端」を表す。
fn stringify_cell(row: &Row, i: usize) -> Option<String> {
    match row.column::<Option<String>>(i) {
        // STRING / INT64 / NUMERIC / TIMESTAMP / DATE / BYTES はすべて StringValue
        Ok(Some(s)) => Some(s),
        Ok(None) => Some("NULL".to_string()),
        Err(RowError::InvalidColumnIndex(_, _)) => None, // 終端
        Err(_) => {
            // FLOAT64 / BOOL など。配列・構造体は未対応として表示。
            let v = row
                .column::<Option<f64>>(i)
                .ok()
                .flatten()
                .map(|f| f.to_string())
                .or_else(|| {
                    row.column::<Option<bool>>(i)
                        .ok()
                        .flatten()
                        .map(|b| b.to_string())
                })
                .unwrap_or_else(|| "<unsupported>".to_string());
            Some(v)
        }
    }
}

/// モックモード用のデータ結果。
fn mock_data(sql: &str) -> QueryOutcome {
    // モックは SQL を実行しないが、末尾の LIMIT n だけは尊重して紛らわしさを減らす。
    let limit = parse_limit(sql).unwrap_or(20);
    QueryOutcome {
        columns: vec!["Id".into(), "Payload".into(), "Seq".into()],
        rows: (0..limit)
            .map(|i| {
                vec![
                    format!("00000000-0000-0000-0000-{:012}", i),
                    format!("payload-{i}"),
                    i.to_string(),
                ]
            })
            .collect(),
        ..Default::default()
    }
}

/// SQL 末尾の `LIMIT n` を取り出す（モック用の簡易パース）。
fn parse_limit(sql: &str) -> Option<usize> {
    // 小文字化した文字列側でオフセットを取り、同じ小文字側を切る。
    // 元の sql を切ると、非 ASCII を含む SQL で小文字化により長さが変わったとき
    // バイト境界がずれて panic することがある（数字は ASCII なので lower で十分）。
    let lower = sql.to_lowercase();
    let pos = lower.rfind("limit")?;
    lower[pos + "limit".len()..]
        .split_whitespace()
        .next()?
        .parse::<usize>()
        .ok()
}

/// モックモード用のスキーマグラフ（Singers→Albums→Songs のインターリーブ + FK）。
fn mock_graph() -> SchemaGraph {
    // (name, type, pk)
    let node = |name: &str, cols: &[(&str, &str, bool)], idx: &[&str]| TableNode {
        name: name.into(),
        columns: cols
            .iter()
            .map(|(n, t, pk)| Column {
                name: n.to_string(),
                ty: t.to_string(),
                pk: *pk,
            })
            .collect(),
        indexes: idx.iter().map(|c| c.to_string()).collect(),
    };
    SchemaGraph {
        nodes: vec![
            node(
                "Singers",
                &[("SingerId", "INT64", true), ("Name", "STRING(MAX)", false)],
                &["IDX_Singers_Name (Name)"],
            ),
            node(
                "Albums",
                &[
                    ("SingerId", "INT64", true),
                    ("AlbumId", "INT64", true),
                    ("Title", "STRING(MAX)", false),
                ],
                &["IDX_Albums_Title (Title)"],
            ),
            node(
                "Songs",
                &[
                    ("SingerId", "INT64", true),
                    ("AlbumId", "INT64", true),
                    ("TrackId", "INT64", true),
                    ("Title", "STRING(MAX)", false),
                ],
                &[],
            ),
            node(
                "Customers",
                &[
                    ("CustomerId", "INT64", true),
                    ("Name", "STRING(MAX)", false),
                ],
                &["IDX_Customers_Name (Name)  UNIQUE"],
            ),
            node(
                "Orders",
                &[
                    ("OrderId", "INT64", true),
                    ("CustomerId", "INT64", false),
                    ("Amount", "NUMERIC", false),
                ],
                &["IDX_Orders_Customer (CustomerId)"],
            ),
        ],
        edges: vec![
            Edge {
                from: "Albums".into(),
                to: "Singers".into(),
                kind: EdgeKind::Interleave,
                label: "CASCADE".into(),
            },
            Edge {
                from: "Songs".into(),
                to: "Albums".into(),
                kind: EdgeKind::Interleave,
                label: "CASCADE".into(),
            },
            Edge {
                from: "Orders".into(),
                to: "Customers".into(),
                kind: EdgeKind::ForeignKey,
                label: "FK_Orders_Customers".into(),
            },
        ],
        error: None,
        ddl: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// DDL 文をテーブル単位にまとめる（CREATE TABLE + INDEX + FK をその表へ）。
    #[test]
    fn group_ddl_collects_table_index_and_fk() {
        let stmts = vec![
            "CREATE TABLE Users (\n  Id INT64 NOT NULL,\n  Name STRING(MAX),\n) PRIMARY KEY(Id)".to_string(),
            "CREATE TABLE Orders (\n  OrderId INT64 NOT NULL,\n  UserId INT64,\n) PRIMARY KEY(OrderId)".to_string(),
            "CREATE INDEX IDX_Orders_User ON Orders(UserId)".to_string(),
            "ALTER TABLE Orders ADD CONSTRAINT FK_Orders_Users FOREIGN KEY(UserId) REFERENCES Users(Id)".to_string(),
        ];
        let map = group_ddl_by_table(&stmts);
        // Users は CREATE TABLE のみ。
        let users = map.get("Users").unwrap();
        assert!(users.starts_with("CREATE TABLE Users"));
        assert!(users.ends_with(';'));
        // Orders は CREATE TABLE + INDEX + ALTER を連結。
        let orders = map.get("Orders").unwrap();
        assert!(orders.contains("CREATE TABLE Orders"));
        assert!(orders.contains("CREATE INDEX IDX_Orders_User ON Orders"));
        assert!(orders.contains("ALTER TABLE Orders ADD CONSTRAINT FK_Orders_Users"));
        assert_eq!(orders.matches(";\n\n").count(), 2); // 3 文 → 区切り 2 個
    }

    /// バッククォート付き / IF NOT EXISTS のテーブル名抽出。
    #[test]
    fn ddl_target_table_parsing() {
        assert_eq!(
            ddl_target_table("CREATE TABLE `My Tbl` (Id INT64) PRIMARY KEY(Id)").as_deref(),
            Some("My Tbl")
        );
        assert_eq!(
            ddl_target_table("CREATE TABLE IF NOT EXISTS Foo (Id INT64) PRIMARY KEY(Id)").as_deref(),
            Some("Foo")
        );
        assert_eq!(
            ddl_target_table("CREATE UNIQUE NULL_FILTERED INDEX Ix ON Bar(Col)").as_deref(),
            Some("Bar")
        );
        assert_eq!(ddl_target_table("SELECT 1").as_deref(), None);
        // 非 ASCII（マルチバイト）を含んでもパニックせず正しく抜き出す。
        assert_eq!(
            ddl_target_table("CREATE TABLE `テーブル名` (Id INT64) PRIMARY KEY(Id)").as_deref(),
            Some("テーブル名")
        );
        assert_eq!(
            ddl_target_table("CREATE INDEX Ix ON `テーブル名`(列)").as_deref(),
            Some("テーブル名")
        );
        assert_eq!(
            ddl_target_table("ALTER TABLE `日本語表` ADD CONSTRAINT Fk FOREIGN KEY(X) REFERENCES Y(Z)")
                .as_deref(),
            Some("日本語表")
        );
    }

    /// 実行計画ツリーの組み立て（pre-order の child_links を深さ付きで展開）。
    #[test]
    fn build_plan_lines_tree() {
        use google_cloud_googleapis::spanner::v1::plan_node::ChildLink;
        use google_cloud_googleapis::spanner::v1::PlanNode;
        let node = |idx: i32, name: &str, kind: i32, children: &[i32]| PlanNode {
            index: idx,
            kind,
            display_name: name.into(),
            child_links: children
                .iter()
                .map(|c| ChildLink {
                    child_index: *c,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        // Union → Serialize → Scan の 3 段ツリー。
        let nodes = vec![
            node(0, "Distributed Union", 1, &[1]),
            node(1, "Serialize Result", 1, &[2]),
            node(2, "Scan", 1, &[]),
        ];
        let lines = build_plan_lines(&nodes);
        assert_eq!(lines.len(), 3);
        assert_eq!((lines[0].depth, lines[0].name.as_str()), (0, "Distributed Union"));
        assert_eq!((lines[1].depth, lines[1].name.as_str()), (1, "Serialize Result"));
        assert_eq!((lines[2].depth, lines[2].name.as_str()), (2, "Scan"));
        // 親より小さい/不正な child_index は循環防止で無視（パニックしない）。
        let bad = vec![node(0, "Root", 1, &[0, 9])];
        assert_eq!(build_plan_lines(&bad).len(), 1);
    }

    /// 実行計画を実際に取得できる（emulator 前提・実行はしない）。
    #[tokio::test]
    async fn plan_for_select() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        reseed_loadtest(20).await;
        let client = client().await;
        let out = run_plan(&client, "SELECT Id, Payload FROM LoadTest LIMIT 10").await;
        // エミュレータは EXPLAIN 未対応で空を返す（その旨のメッセージ）。RPC レベルの
        // 失敗（接続/SQL エラー）でないこと、計画が返れば depth 0 のルートがあることを確認。
        if out.lines.is_empty() {
            assert!(
                out.error.as_deref().unwrap_or("").contains("エミュレータ"),
                "想定外のエラー: {:?}",
                out.error
            );
        } else {
            assert_eq!(out.error, None);
            assert_eq!(out.lines[0].depth, 0);
        }
    }

    /// SQL 文の種類判定（DDL/DML/Query の振り分け）。
    #[test]
    fn statement_kind_classifies() {
        assert_eq!(statement_kind("SELECT * FROM T"), StatementKind::Query);
        assert_eq!(statement_kind("  with x as (select 1) select * from x"), StatementKind::Query);
        assert_eq!(
            statement_kind("CREATE TABLE Foo (Id INT64) PRIMARY KEY(Id)"),
            StatementKind::Ddl
        );
        assert_eq!(statement_kind("create index Ix on Foo(Id)"), StatementKind::Ddl);
        assert_eq!(statement_kind("ALTER TABLE Foo ADD COLUMN X INT64"), StatementKind::Ddl);
        assert_eq!(statement_kind("DROP TABLE Foo"), StatementKind::Ddl);
        assert_eq!(statement_kind("INSERT INTO T (Id) VALUES (1)"), StatementKind::Dml);
        assert_eq!(statement_kind("update T set x=1"), StatementKind::Dml);
        assert_eq!(statement_kind("DELETE FROM T WHERE Id=1"), StatementKind::Dml);
        // 複数 DDL を ; で分割。
        assert_eq!(
            split_ddl_statements("CREATE TABLE A (Id INT64) PRIMARY KEY(Id);\n CREATE INDEX I ON A(Id); "),
            vec![
                "CREATE TABLE A (Id INT64) PRIMARY KEY(Id)".to_string(),
                "CREATE INDEX I ON A(Id)".to_string()
            ]
        );
    }

    /// gs:// URI のパース（ネットワーク不要）。
    #[test]
    fn parse_gs_uri_ok_and_err() {
        let (b, o) = parse_gs_uri("gs://my-bucket/dir/data.csv").unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(o, "dir/data.csv");
        // 前後空白は許容する。
        assert!(parse_gs_uri("  gs://b/o  ").is_ok());
        // スキーム無し・オブジェクト無し・空はエラー。
        assert!(parse_gs_uri("my-bucket/data.csv").is_err());
        assert!(parse_gs_uri("gs://only-bucket").is_err());
        assert!(parse_gs_uri("gs:///data.csv").is_err());
    }

    /// gs://bucket/prefix の分解（prefix 省略可）。
    #[test]
    fn split_gs_location_cases() {
        assert_eq!(
            split_gs_location("gs://b/dir/").unwrap(),
            ("b".into(), "dir/".into())
        );
        // prefix 省略はバケット直下（空 prefix）。
        assert_eq!(split_gs_location("gs://b").unwrap(), ("b".into(), "".into()));
        assert_eq!(split_gs_location("gs://b/").unwrap(), ("b".into(), "".into()));
        assert!(split_gs_location("b/dir/").is_err());
        assert!(split_gs_location("gs:///dir/").is_err());
    }

    /// オブジェクト名の percent-encode（スラッシュもエスケープ）。
    #[test]
    fn encode_object_escapes() {
        assert_eq!(encode_object("dir/sub/a b.csv"), "dir%2Fsub%2Fa%20b.csv");
        // 非予約文字はそのまま。
        assert_eq!(encode_object("a-b_c.d~e"), "a-b_c.d~e");
    }

    /// ストリーミング CSV パーサ: チャンク境界をまたいでも正しく分解できるか。
    #[test]
    fn csv_streamer_handles_chunks_and_quotes() {
        let input = "a,b,c\r\n1,\"x,y\",\"he said \"\"hi\"\"\"\n2,,z\n\nlast,row,!";
        // 様々なチャンク境界で同じ結果になることを確認。
        for size in [1usize, 2, 3, 5, 7, 1000] {
            let mut s = CsvStreamer::default();
            let mut out = Vec::new();
            for chunk in input.as_bytes().chunks(size) {
                s.push(chunk, &mut out);
            }
            s.finish(&mut out);
            assert_eq!(
                out,
                vec![
                    vec!["a", "b", "c"],
                    vec!["1", "x,y", "he said \"hi\""],
                    vec!["2", "", "z"],
                    // 空行は捨てられる
                    vec!["last", "row", "!"],
                ],
                "chunk size {size}"
            );
        }
    }

    /// ストリーミング CSV パーサのその他のケース（ネットワーク不要）。
    fn stream_all(input: &[u8]) -> Vec<Vec<String>> {
        let mut s = CsvStreamer::default();
        let mut out = Vec::new();
        s.push(input, &mut out);
        s.finish(&mut out);
        out
    }

    #[test]
    fn csv_streamer_edge_cases() {
        // 空入力。
        assert!(stream_all(b"").is_empty());
        // ヘッダのみ・末尾改行なし。
        assert_eq!(stream_all(b"a,b"), vec![vec!["a", "b"]]);
        // 末尾改行なしの複数行。
        assert_eq!(
            stream_all(b"a,b\nc,d"),
            vec![vec!["a", "b"], vec!["c", "d"]]
        );
        // 末尾カンマ → 空フィールド。
        assert_eq!(stream_all(b"a,b,\n"), vec![vec!["a", "b", ""]]);
        // クォート内の改行・カンマ。
        assert_eq!(
            stream_all(b"x,\"line1\nline2\",y\n"),
            vec![vec!["x", "line1\nline2", "y"]]
        );
        // CRLF と LF の混在、空行は捨てる。
        assert_eq!(
            stream_all(b"a\r\n\r\nb\n"),
            vec![vec!["a"], vec!["b"]]
        );
        // フィールド途中の " はデータ（5'10"）。以降のカンマ・改行を飲み込まない。
        assert_eq!(
            stream_all(b"O'Brien,5'10\" tall,z\nnext,row,!\n"),
            vec![vec!["O'Brien", "5'10\" tall", "z"], vec!["next", "row", "!"]]
        );
        // 先頭が " のフィールドは従来どおり引用（カンマを保持・外側 " を除去）。
        assert_eq!(stream_all(b"\"a,b\",c\n"), vec![vec!["a,b", "c"]]);
    }

    /// push_tracked がレコードごとの終端バイト位置を 1:1 で返す（再開シーク用）。
    #[test]
    fn streamer_tracks_byte_offsets() {
        let input = b"a,b\nc,d\nlast"; // 末尾改行なし
        let mut s = CsvStreamer::default();
        let mut out = Vec::new();
        let mut offs = Vec::new();
        // チャンク境界をまたいでも累積位置が正しいことも見る（3 バイトずつ）。
        for ch in input.chunks(3) {
            s.push_tracked(ch, &mut out, &mut offs);
        }
        s.finish_tracked(&mut out, &mut offs);
        assert_eq!(out, vec![vec!["a", "b"], vec!["c", "d"], vec!["last"]]);
        // "a,b\n"=4, "c,d\n"=8(累積), "last"=12(EOF)。
        assert_eq!(offs, vec![4, 8, 12]);
    }

    /// 先頭の UTF-8 BOM は、1 バイトずつ供給して境界をまたいでも先頭フィールドから除かれる。
    #[test]
    fn csv_streamer_strips_bom_across_chunks() {
        let mut input = vec![0xEF, 0xBB, 0xBF]; // UTF-8 BOM
        input.extend_from_slice(b"Id,Name\n1,a\n");
        let mut s = CsvStreamer::default();
        let mut out = Vec::new();
        for b in &input {
            s.push(&[*b], &mut out); // 生 strip_bom を通さず 1 バイトずつ
        }
        s.finish(&mut out);
        assert_eq!(out[0][0], "Id", "先頭フィールドに BOM が残らない");
        assert_eq!(out[0], vec!["Id", "Name"]);
        assert_eq!(out[1], vec!["1", "a"]);
    }

    /// Shift-JIS デコードとタブ区切りに対応する。
    #[test]
    fn csv_streamer_shift_jis_and_tab() {
        // "名前\t値\nあ\t1\n" を Shift-JIS でエンコードした生バイト。
        let (sjis, _, _) = encoding_rs::SHIFT_JIS.encode("名前\t値\nあ\t1\n");
        let mut s = CsvStreamer::new(Encoding::ShiftJis, b'\t');
        let mut out = Vec::new();
        // 1 バイトずつ供給してもマルチバイト境界で壊れない。
        for b in sjis.iter() {
            s.push(&[*b], &mut out);
        }
        s.finish(&mut out);
        assert_eq!(out, vec![vec!["名前", "値"], vec!["あ", "1"]]);
    }

    /// parse_preview は文字コード・区切り・行数上限を反映する。
    #[test]
    fn parse_preview_respects_encoding_delimiter() {
        let (sjis, _, _) = encoding_rs::SHIFT_JIS.encode("氏名;住所\nあ;東京\nい;大阪\n");
        let rows = parse_preview(&sjis, Encoding::ShiftJis, b';', 2);
        assert_eq!(rows, vec![vec!["氏名", "住所"], vec!["あ", "東京"]]);
    }

    /// マルチバイト UTF-8 が 1 バイト境界でも壊れない。
    #[test]
    fn csv_streamer_utf8_across_chunks() {
        let input = "名前,値\nあいう,123\n".as_bytes();
        let mut s = CsvStreamer::default();
        let mut out = Vec::new();
        for b in input {
            s.push(&[*b], &mut out); // 1 バイトずつ供給
        }
        s.finish(&mut out);
        assert_eq!(out, vec![vec!["名前", "値"], vec!["あいう", "123"]]);
    }

    /// BOM 除去は先頭だけ。
    #[test]
    fn strip_bom_only_leading() {
        let mut b = vec![0xEF, 0xBB, 0xBF, b'a', b',', b'b'];
        strip_bom(&mut b);
        assert_eq!(b, b"a,b");
        let mut c = b"a,b".to_vec();
        strip_bom(&mut c);
        assert_eq!(c, b"a,b");
    }

    /// build_mutation: 列マッピング（src_index）・モード・型エラーの列名（ネットワーク不要）。
    #[test]
    fn build_mutation_maps_columns_and_mode() {
        use google_cloud_googleapis::spanner::v1::mutation::Operation;
        // テーブル列順 [Name, Id] に対し、CSV は [Id, Name] 並び（src_index で逆引き）。
        let req = ImportRequest {
            id: 0,
            table: "T".into(),
            columns: vec![
                ImportColumn { name: "Name".into(), ty: "STRING(MAX)".into(), src_index: 1 },
                ImportColumn { name: "Id".into(), ty: "INT64".into(), src_index: 0 },
            ],
            source: ImportSource::File("/dev/null".into()),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let row = vec!["7".to_string(), "bob".to_string()];
        let m = build_mutation(&req, &row).unwrap();
        match m.operation.unwrap() {
            Operation::Insert(w) => {
                assert_eq!(w.table, "T");
                let names: Vec<&str> = w.columns.iter().map(|s| s.as_str()).collect();
                assert_eq!(names, ["Name", "Id"]);
                assert_eq!(w.values.len(), 1);
                assert_eq!(w.values[0].values.len(), 2);
            }
            _ => panic!("expected Insert operation"),
        }
        // 上書き挿入モード。
        let mut up = req.clone();
        up.mode = ImportMode::InsertOrUpdate;
        assert!(matches!(
            build_mutation(&up, &row).unwrap().operation.unwrap(),
            Operation::InsertOrUpdate(_)
        ));
        // マッピング先（Id=INT64）が不正 → 列名付きエラー。
        let bad = vec!["abc".to_string(), "bob".to_string()];
        let err = build_mutation(&req, &bad).unwrap_err();
        assert!(err.contains("Id"), "err should name column Id: {err}");
    }

    /// チェックポイントの書き込み・読み出し往復（ネットワーク不要）。
    #[test]
    fn checkpoint_roundtrip() {
        let path = std::env::temp_dir().join("sv_ckpt_roundtrip.txt");
        let _ = std::fs::remove_file(&path);
        let sig = "v1\tsig-A";
        // 新規作成 → committed は空。
        let w = CheckpointWriter::open(&path, sig, true);
        assert!(load_checkpoint(&path, sig).is_empty());
        w.mark(2, 400, 30);
        w.mark(5, 1000, 60);
        let map = load_checkpoint(&path, sig);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&2), Some(&(400u64, 30usize)));
        assert_eq!(map.get(&5), Some(&(1000u64, 60usize)));
        // シグネチャ不一致なら読まない。
        assert!(load_checkpoint(&path, "v1\tsig-B").is_empty());
        // 追記オープンで継続できる。
        let w2 = CheckpointWriter::open(&path, sig, false);
        w2.mark(9, 1800, 90);
        assert_eq!(load_checkpoint(&path, sig).get(&9), Some(&(1800u64, 90usize)));
        let _ = std::fs::remove_file(&path);

        // 連続コミット先頭部分: 0,1,2 が連続 → (3, 終端400, 累積30)。
        let mut cm: HashMap<usize, (u64, usize)> = HashMap::new();
        cm.insert(0, (100, 10));
        cm.insert(1, (250, 20));
        cm.insert(2, (400, 30));
        cm.insert(5, (900, 60)); // 飛び石
        assert_eq!(contiguous_resume_point(&cm), (3, 400, 30));
        // 0 が無ければ最初から。
        let mut cm2: HashMap<usize, (u64, usize)> = HashMap::new();
        cm2.insert(1, (250, 20));
        assert_eq!(contiguous_resume_point(&cm2), (0, 0, 0));
    }

    /// parse_limit: 非 ASCII を含む SQL でも panic せず LIMIT を取れる。
    #[test]
    fn parse_limit_handles_non_ascii() {
        assert_eq!(parse_limit("SELECT * FROM T LIMIT 5"), Some(5));
        assert_eq!(parse_limit("select * from t limit 12"), Some(12));
        // 日本語コメント/リテラルが前にあっても境界ずれで panic しない。
        assert_eq!(parse_limit("SELECT * FROM T -- 表 LIMIT 7"), Some(7));
        assert_eq!(parse_limit("SELECT 'İstanbul' FROM T LIMIT 3"), Some(3));
        assert_eq!(parse_limit("SELECT * FROM T"), None);
    }

    /// シグネチャは列/ per_request / ソースで変わり、mode では変わらない。
    #[test]
    fn import_signature_distinguishes() {
        let base = ImportRequest {
            id: 0,
            table: "T".into(),
            columns: cols(&[("Id", "INT64")]),
            source: ImportSource::Gcs("gs://b/o".into()),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let s1 = import_signature(&base, 100);
        // 列が増えれば別物。
        let mut b2 = base.clone();
        b2.columns = cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]);
        assert_ne!(s1, import_signature(&b2, 100));
        // per_request が変われば別物。
        assert_ne!(s1, import_signature(&base, 50));
        // mode は含めない（Insert で落ちても上書き挿入で再開できるように）。
        let mut b3 = base.clone();
        b3.mode = ImportMode::InsertOrUpdate;
        assert_eq!(s1, import_signature(&b3, 100));
    }

    /// ドライラン: Spanner に繋がず、有効行数と不正行数を返す（ネットワーク不要）。
    #[tokio::test]
    async fn dry_run_validates_without_writing() {
        // 良い行 2 + 不正行 1（Id が数値でない）。
        let csv = "Id,Name\n1,a\n2,b\nx,c\n";
        let path = write_temp_csv("dryrun", csv);
        let req = ImportRequest {
            id: 0,
            table: "T".into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: true,
            dry_run: true,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (tx, _rx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&SpannerEnv::default(), &req, false, &tx).await;
        assert!(out.dry_run);
        assert_eq!(out.total, 3);
        assert_eq!(out.written, 2, "2 valid rows");
        assert_eq!(out.skipped, 1, "1 bad row (x not INT64)");
        assert_eq!(out.error, None, "skip_bad_rows なのでエラーにしない");

        // skip=false なら最初の不正行でエラーを返す。
        let mut strict = req.clone();
        strict.skip_bad_rows = false;
        let (tx2, _rx2) = std::sync::mpsc::channel::<ImportProgress>();
        let out2 = run_streaming_import(&SpannerEnv::default(), &strict, false, &tx2).await;
        assert!(out2.error.is_some(), "strict dry-run errors on bad row");
    }

    /// Resource Manager projects 応答の解析（削除予約は除外、ページトークン）。
    #[test]
    fn parse_projects_page_filters_and_paginates() {
        let body = r#"{
            "projects": [
                {"projectId": "alpha", "lifecycleState": "ACTIVE"},
                {"projectId": "gone", "lifecycleState": "DELETE_REQUESTED"},
                {"projectId": "beta"}
            ],
            "nextPageToken": "tok2"
        }"#;
        let (ids, next) = parse_projects_page(body).unwrap();
        assert_eq!(ids, vec!["alpha", "beta"]); // gone は除外
        assert_eq!(next.as_deref(), Some("tok2"));
        // v3 形式（state フィールド）も解釈する。
        let v3 = r#"{"projects": [
            {"projectId": "p1", "state": "ACTIVE"},
            {"projectId": "p2", "state": "DELETE_REQUESTED"}
        ]}"#;
        let (ids, _) = parse_projects_page(v3).unwrap();
        assert_eq!(ids, vec!["p1"]); // DELETE_REQUESTED は除外
        // 空応答。
        let (ids, next) = parse_projects_page("{}").unwrap();
        assert!(ids.is_empty() && next.is_none());
    }

    /// Spanner instances/databases 応答の末尾セグメント抽出（ソート済み）。
    #[test]
    fn parse_resource_names_last_segment() {
        let body = r#"{"instances": [
            {"name": "projects/p/instances/zeta"},
            {"name": "projects/p/instances/alpha"}
        ], "nextPageToken": "tok"}"#;
        // parse は文書順（ソートは list_paged 側で全ページ集約後に行う）。
        let (names, next) = parse_resource_names(body, "instances").unwrap();
        assert_eq!(names, vec!["zeta", "alpha"]);
        assert_eq!(next.as_deref(), Some("tok"));
        // フィールド不一致・空は空ベクタ・トークン無し。
        assert!(parse_resource_names(body, "databases").unwrap().0.is_empty());
        let (n2, t2) = parse_resource_names("{}", "instances").unwrap();
        assert!(n2.is_empty() && t2.is_none());
    }

    /// リトライ対象の判定: 一過性コードだけ true、恒久エラーは false。
    #[test]
    fn is_retryable_codes() {
        use google_cloud_gax::grpc::{Code, Status};
        for c in [
            Code::Unavailable,
            Code::Aborted,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
        ] {
            assert!(is_retryable(&Status::new(c, "x")), "{c:?} should retry");
        }
        for c in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::PermissionDenied,
            Code::FailedPrecondition,
        ] {
            assert!(!is_retryable(&Status::new(c, "x")), "{c:?} must not retry");
        }
        // グループ別 status の i32 コード判定が、Status 版と一致すること。
        for c in [
            Code::Unavailable,
            Code::Aborted,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
        ] {
            let code_i32 = Status::new(c, "x").code() as i32;
            assert_eq!(
                is_retryable_code(code_i32),
                is_retryable(&Status::new(c, "x")),
                "{c:?} の i32 判定が一致するべき"
            );
        }
    }

    /// バックオフ: 指数増・上限 5s・[capped/2, capped] の範囲・決定的。
    #[test]
    fn retry_delay_bounds() {
        // attempt1: base=100 → capped=100 → [50,100] ms。
        let d1 = retry_delay(1, "s").as_millis();
        assert!((50..=100).contains(&d1), "d1={d1}");
        // 大きい attempt は 5s で頭打ち → [2500,5000] ms。
        let d9 = retry_delay(9, "s").as_millis();
        assert!((2500..=5000).contains(&d9), "d9={d9}");
        // 同じ入力なら決定的。
        assert_eq!(retry_delay(3, "abc"), retry_delay(3, "abc"));
    }

    /// NULL トークン一致は NULL になり、空欄扱いとは独立に効く。
    #[test]
    fn null_token_maps_to_null() {
        // "NULL" を NULL 扱い（empty_as_null=false でも効く）。
        assert!(convert_cell("NULL", "STRING(MAX)", false, Some("NULL")).is_ok());
        // INT64 列で "NULL" トークンなら NULL（パースエラーにならない）。
        assert!(convert_cell("NULL", "INT64", false, Some("NULL")).is_ok());
        // トークン不一致の不正値はエラー。
        assert!(convert_cell("NA", "INT64", false, Some("NULL")).is_err());
    }

    /// セル変換: 型ごとのパース成否（ネットワーク不要）。
    #[test]
    fn convert_cell_types() {
        assert!(convert_cell("123", "INT64", true, None).is_ok());
        assert!(convert_cell("abc", "INT64", true, None).is_err());
        assert!(convert_cell("3.5", "FLOAT64", true, None).is_ok());
        assert!(convert_cell("x", "FLOAT64", true, None).is_err());
        assert!(convert_cell("true", "BOOL", true, None).is_ok());
        assert!(convert_cell("1", "BOOL", true, None).is_ok());
        assert!(convert_cell("maybe", "BOOL", true, None).is_err());
        // 文字列系はそのまま通る
        assert!(convert_cell("hello", "STRING(MAX)", true, None).is_ok());
        assert!(convert_cell("2024-01-01", "DATE", true, None).is_ok());
        // 配列・構造体は未対応
        assert!(convert_cell("[1,2]", "ARRAY<INT64>", true, None).is_err());
    }

    /// <null> / (null) は既定（トークン未指定）で NULL 扱い。大小無視。
    #[test]
    fn default_null_placeholders() {
        assert!(is_default_null_token("<null>"));
        assert!(is_default_null_token("(null)"));
        assert!(is_default_null_token("<NULL>")); // 大小無視
        assert!(is_default_null_token("(Null)"));
        assert!(!is_default_null_token("null")); // 括弧なしは対象外
        assert!(!is_default_null_token("<nullable>"));
        assert!(!is_default_null_token(""));
        // INT64 列でも <null>/(null) はパースエラーにならない＝NULL 化されている証拠。
        assert!(convert_cell("<null>", "INT64", false, None).is_ok());
        assert!(convert_cell("(null)", "INT64", false, None).is_ok());
        // 通常文字列は影響なし（STRING はそのまま、INT64 はエラー）。
        assert!(convert_cell("hello", "STRING(MAX)", false, None).is_ok());
        assert!(convert_cell("abc", "INT64", false, None).is_err());
        // 照合の正規化も同じ規則。
        assert_eq!(canon_cell("<null>", false, None), "NULL");
        assert_eq!(canon_cell("(NULL)", false, None), "NULL");
        assert_eq!(canon_cell("keep", false, None), "keep");
    }

    /// 空欄は empty_as_null=true のとき、型に関わらず NULL として通る。
    #[test]
    fn convert_cell_empty_null() {
        // INT64 でも空欄は NULL 扱いでパースエラーにならない
        assert!(convert_cell("", "INT64", true, None).is_ok());
        // empty_as_null=false なら空文字列として扱い、INT64 ではパース失敗
        assert!(convert_cell("", "INT64", false, None).is_err());
        // 文字列なら空文字列のまま OK
        assert!(convert_cell("", "STRING(MAX)", false, None).is_ok());
    }

    // SPANNER_EMULATOR_HOST が設定され、`setup`/`loadgen` 済みのエミュレータが前提。
    // 未設定なら自動スキップ（CI でも安全）。
    fn emulator_db() -> Option<String> {
        std::env::var("SPANNER_EMULATOR_HOST").ok()?;
        Some(format!(
            "projects/{}/instances/{}/databases/{}",
            std::env::var("SPANNER_PROJECT").ok()?,
            std::env::var("SPANNER_INSTANCE").ok()?,
            std::env::var("SPANNER_DATABASE").ok()?,
        ))
    }

    async fn client() -> Client {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let config = ClientConfig::default().with_auth().await.unwrap();
        Client::new(&emulator_db().unwrap(), config).await.unwrap()
    }

    /// 汎用型の文字列化を検証（STRING/INT64/FLOAT64/BOOL/NULL の各分岐）
    #[tokio::test]
    async fn stringify_mixed_types() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let client = client().await;
        let (cols, rows, truncated) = try_query(
            &client,
            "SELECT 'abc' AS s, 123 AS i, 3.5 AS f, true AS b, CAST(NULL AS STRING) AS n",
        )
        .await
        .unwrap();

        assert_eq!(cols, vec!["s", "i", "f", "b", "n"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["abc", "123", "3.5", "true", "NULL"]);
        assert!(!truncated);
    }

    /// LoadTest に UUID キーの行を投入する（既存は全削除）。外部の loadgen に依存しない。
    async fn reseed_loadtest(n: usize) {
        let c = client().await;
        let _ = c
            .apply(vec![gcloud_spanner::mutation::delete(
                "LoadTest",
                gcloud_spanner::key::all_keys(),
            )])
            .await;
        let mut muts = Vec::new();
        for i in 0..n {
            let id = uuid::Uuid::new_v4().to_string();
            let payload = format!("payload-{i}");
            muts.push(gcloud_spanner::mutation::insert_or_update(
                "LoadTest",
                &["Id", "Payload"],
                &[&id, &payload],
            ));
            if muts.len() >= 1000 {
                c.apply(std::mem::take(&mut muts)).await.unwrap();
            }
        }
        if !muts.is_empty() {
            c.apply(muts).await.unwrap();
        }
    }

    /// 実テーブルの読み取りと列名取得を検証
    #[tokio::test]
    async fn read_loadtest_rows() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await; // 書き込みを直列化
        reseed_loadtest(MAX_ROWS + 50).await;
        let client = client().await;
        let (cols, rows, _) = try_query(&client, "SELECT Id, Payload FROM LoadTest LIMIT 5")
            .await
            .unwrap();

        assert_eq!(cols, vec!["Id", "Payload"]);
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|r| r.len() == 2));
        // Id は UUID 文字列
        assert_eq!(rows[0][0].len(), 36);
    }

    /// MAX_ROWS 打ち切りフラグの検証
    #[tokio::test]
    async fn truncates_at_limit() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await; // 書き込みを直列化
        reseed_loadtest(MAX_ROWS + 50).await;
        let client = client().await;
        let (_, rows, truncated) = try_query(&client, "SELECT Id FROM LoadTest LIMIT 5000")
            .await
            .unwrap();
        assert_eq!(rows.len(), MAX_ROWS);
        assert!(truncated);
    }

    /// 依存関係クエリ: インターリーブの親子と外部キーを検出できるか
    #[tokio::test]
    async fn schema_dependency_query() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await;
        create_dep_schema().await;

        let client = client().await;
        let (cols, rows, _) = try_query(&client, DEPENDENCY_SQL).await.unwrap();

        assert_eq!(cols, vec!["テーブル", "種別", "依存先", "詳細"]);
        // インターリーブ: DepChild → DepParent
        assert!(
            rows.iter()
                .any(|r| r[0] == "DepChild" && r[1] == "インターリーブ" && r[2] == "DepParent"),
            "interleave 行が見つからない: {rows:?}"
        );
        // 外部キー: DepOrders → DepParent
        assert!(
            rows.iter()
                .any(|r| r[0] == "DepOrders" && r[1] == "外部キー" && r[2] == "DepParent"),
            "foreign key 行が見つからない: {rows:?}"
        );
    }

    /// スキーマグラフ: 全カラムとセカンダリインデックスが取得できるか
    #[tokio::test]
    async fn schema_graph_has_columns_and_indexes() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await;
        create_dep_schema().await;

        let client = client().await;
        let graph = try_fetch_schema(&client).await.unwrap();

        let orders = graph
            .nodes
            .iter()
            .find(|n| n.name == "DepOrders")
            .expect("DepOrders ノードがない");
        // 全カラム（OrderId, ParentId）が含まれる
        assert!(orders.columns.iter().any(|c| c.name == "OrderId" && c.pk));
        assert!(orders.columns.iter().any(|c| c.name == "ParentId"));
        // セカンダリインデックスが含まれる
        assert!(
            orders
                .indexes
                .iter()
                .any(|i| i.contains("IDX_DepOrders_Parent")),
            "インデックスが見つからない: {:?}",
            orders.indexes
        );
    }

    /// テスト用の SpannerEnv（emulator の環境変数から）。
    fn test_env() -> SpannerEnv {
        SpannerEnv {
            project: std::env::var("SPANNER_PROJECT").unwrap(),
            instance: std::env::var("SPANNER_INSTANCE").unwrap(),
            database: std::env::var("SPANNER_DATABASE").unwrap(),
        }
    }

    /// テーブル列名と型から、CSV 列順どおりの ImportColumn を作る。
    fn cols(specs: &[(&str, &str)]) -> Vec<ImportColumn> {
        specs
            .iter()
            .enumerate()
            .map(|(i, (n, t))| ImportColumn {
                name: (*n).into(),
                ty: (*t).into(),
                src_index: i,
            })
            .collect()
    }

    /// 一時 CSV を書いてパスを返す（テストごとにユニーク名）。
    fn write_temp_csv(name: &str, body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("spanner_viewer_test_{name}.csv"));
        std::fs::write(&path, body).unwrap();
        path
    }

    /// ストリーミング取り込み: ローカル CSV から型付きの行を並列 BatchWrite で投入（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_typed_rows() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        create_import_table().await;
        let client = client().await;

        let csv = "Id,Name,Score,Active,Note\n\
                   1,alice,1.5,true,\n\
                   2,bob,2.0,false,hi\n";
        let path = write_temp_csv("typed", csv);
        let req = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[
                ("Id", "INT64"),
                ("Name", "STRING(MAX)"),
                ("Score", "FLOAT64"),
                ("Active", "BOOL"),
                ("Note", "STRING(MAX)"),
            ]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "import error: {:?}", out.error);
        assert_eq!(out.written, 2);
        assert_eq!(out.total, 2);
        // 進捗イベントが少なくとも 1 件は流れる。
        let progress_events = prx.try_iter().count();
        assert!(progress_events >= 1, "no progress events");

        let (_, rows, _) = try_query(
            &client,
            "SELECT Id, Name, Score, Active, Note FROM ImportTest ORDER BY Id",
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], "alice");
        assert_eq!(rows[0][4], "NULL"); // 空欄 → NULL
        assert_eq!(rows[1][1], "bob");

        // 上書き挿入で既存行を更新できる。
        let csv2 = "Id,Name\n1,alice2\n";
        let path2 = write_temp_csv("upsert", csv2);
        let upsert = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path2),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx2, _prx2) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &upsert, false, &ptx2).await;
        assert_eq!(out.error, None);
        assert_eq!(out.written, 1);
        let (_, rows, _) = try_query(&client, "SELECT Name FROM ImportTest WHERE Id = 1")
            .await
            .unwrap();
        assert_eq!(rows[0][0], "alice2");
    }

    /// 実 DDL 取得: GetDatabaseDdl から CREATE TABLE が引ける（emulator 前提）。
    #[tokio::test]
    async fn fetch_ddl_returns_create_table() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        create_import_table().await;

        let ddl = fetch_ddl(&test_env()).await;
        let create = ddl.get("ImportTest").expect("ImportTest の DDL が無い");
        assert!(create.contains("CREATE TABLE ImportTest"), "DDL: {create}");
        assert!(create.contains("PRIMARY KEY"), "DDL: {create}");
        assert!(create.trim_end().ends_with(';'), "末尾に ; が無い: {create}");
    }

    /// SQL コンソール経由で DDL(CREATE/DROP)・DML(INSERT)・Query(SELECT) が
    /// それぞれ正しい経路で実行できる（emulator 前提）。
    #[tokio::test]
    async fn sql_console_ddl_dml_query() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        let env = test_env();
        let tbl = "SqlConsoleTest";

        // 既存なら掃除（無ければエラーは無視）。
        let _ = run_ddl(&env, &format!("DROP TABLE {tbl}")).await;

        // CREATE（DDL → Admin API）。
        let out = run_ddl(
            &env,
            &format!("CREATE TABLE {tbl} (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(Id)"),
        )
        .await;
        assert_eq!(out.error, None, "CREATE error: {:?}", out.error);

        let client = client().await;
        // INSERT（DML → 読み書きトランザクション）。
        let out = run_dml(
            &client,
            &format!("INSERT INTO {tbl} (Id, Name) VALUES (1, 'alice'), (2, 'bob')"),
        )
        .await;
        assert_eq!(out.error, None, "INSERT error: {:?}", out.error);
        assert_eq!(out.rows[0][0], "2", "影響行数=2");

        // SELECT（Query → 読み取りスナップショット）。
        let out = run_query(&client, &format!("SELECT Id, Name FROM {tbl} ORDER BY Id")).await;
        assert_eq!(out.error, None);
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.rows[0][1], "alice");
        assert_eq!(out.rows[1][1], "bob");

        // 後始末。
        let _ = run_ddl(&env, &format!("DROP TABLE {tbl}")).await;
    }

    /// STRING(5) 列に <null>(6文字) が来ても NULL 化されるので桁エラーにならない（emulator 前提）。
    #[tokio::test]
    async fn import_null_placeholder_into_fixed_size_string() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        let env = test_env();
        let tbl = "NullSizeTest";
        let _ = run_ddl(&env, &format!("DROP TABLE {tbl}")).await;
        let out = run_ddl(
            &env,
            &format!("CREATE TABLE {tbl} (Id INT64 NOT NULL, Code STRING(5)) PRIMARY KEY(Id)"),
        )
        .await;
        assert_eq!(out.error, None, "create: {:?}", out.error);

        let csv = "Id,Code\n1,<null>\n2,abcde\n";
        let path = write_temp_csv("nullsize", csv);
        let req = ImportRequest {
            id: 0,
            table: tbl.into(),
            columns: cols(&[("Id", "INT64"), ("Code", "STRING(5)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "import error: {:?}", out.error);

        let client = client().await;
        let (_, rows, _) = try_query(&client, &format!("SELECT Id, Code FROM {tbl} ORDER BY Id"))
            .await
            .unwrap();
        assert_eq!(rows[0][1], "NULL", "<null> は NULL になる（桁エラーにならない）");
        assert_eq!(rows[1][1], "abcde");
        let _ = run_ddl(&env, &format!("DROP TABLE {tbl}")).await;
    }

    /// <null> / (null) がトークン未指定でも DB に NULL として入る（emulator 前提）。
    #[tokio::test]
    async fn import_default_null_placeholders_to_db() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        create_import_table().await;
        let client = client().await;

        let csv = "Id,Name,Note\n\
                   1,<null>,(null)\n\
                   2,bob,keep\n\
                   3,<NULL>,Keep<null>Inside\n";
        let path = write_temp_csv("default_null", csv);
        let req = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)"), ("Note", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None, // 未指定でも <null>/(null) は NULL になるはず
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "import error: {:?}", out.error);

        let (_, rows, _) = try_query(&client, "SELECT Id, Name, Note FROM ImportTest ORDER BY Id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        // 1: <null>→NULL, (null)→NULL
        assert_eq!(rows[0][1], "NULL");
        assert_eq!(rows[0][2], "NULL");
        // 2: 通常値はそのまま
        assert_eq!(rows[1][1], "bob");
        assert_eq!(rows[1][2], "keep");
        // 3: 大小無視で <NULL>→NULL。途中に <null> を含む文字列は完全一致でないので保持。
        assert_eq!(rows[2][1], "NULL");
        assert_eq!(rows[2][2], "Keep<null>Inside");
    }

    /// クォート/カンマ/改行/途中のクォートを含む実データが、欠落・結合・余計な
    /// クォート無しで正しく取り込まれる（emulator 前提）。
    #[tokio::test]
    async fn import_messy_quoted_csv_roundtrip() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        create_import_table().await;
        let client = client().await;

        // 1: クォート内カンマ / 2: エスケープ "" / 3: フィールド途中の " (5'10") / 4: クォート内改行
        let csv = "Id,Name,Note\n\
                   1,\"Smith, John\",plain\n\
                   2,\"she said \"\"hi\"\"\",ok\n\
                   3,O'Brien,5'10\" tall\n\
                   4,\"multi\nline\",last\n";
        let path = write_temp_csv("messy_quoted", csv);
        let req = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)"), ("Note", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "import error: {:?}", out.error);
        assert_eq!(out.written, 4, "4 行すべて取り込まれるはず");

        let (_, rows, _) = try_query(&client, "SELECT Id, Name, Note FROM ImportTest ORDER BY Id")
            .await
            .unwrap();
        assert_eq!(rows.len(), 4);
        // クォート内カンマは 1 フィールドのまま、外側クォートは除去。
        assert_eq!(rows[0][1], "Smith, John");
        assert_eq!(rows[0][2], "plain");
        // エスケープ "" は " 1 個に。
        assert_eq!(rows[1][1], "she said \"hi\"");
        // フィールド途中の " はデータとして保持（行結合・欠落を起こさない）。
        assert_eq!(rows[2][1], "O'Brien");
        assert_eq!(rows[2][2], "5'10\" tall");
        // クォート内改行は 1 フィールド内に保持。
        assert_eq!(rows[3][1], "multi\nline");
        assert_eq!(rows[3][2], "last");
    }

    /// 照合: PK 列だけマッピングすれば「DB に存在するか（存在確認）」ができる。
    /// 値差異は常に 0、CSVのみ = 不足行、になる（emulator 前提）。
    #[tokio::test]
    async fn verify_pk_only_existence_check() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        create_import_table().await;

        // DB に Id 1,2,3 を投入（Name は適当）。
        let seed = "Id,Name,Score,Active,Note\n1,a,1,true,\n2,b,2,true,\n3,c,3,true,\n";
        let import = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[
                ("Id", "INT64"),
                ("Name", "STRING(MAX)"),
                ("Score", "FLOAT64"),
                ("Active", "BOOL"),
                ("Note", "STRING(MAX)"),
            ]),
            source: ImportSource::File(write_temp_csv("pkonly_seed", seed)),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _p) = std::sync::mpsc::channel::<ImportProgress>();
        assert_eq!(
            run_streaming_import(&test_env(), &import, false, &ptx).await.error,
            None
        );

        // スプレッドシート相当（Id だけの列・名前は何でもよい）。Id 1,2,4。
        let csv = "Key\n1\n2\n4\n";
        let vreq = VerifyRequest {
            table: "ImportTest".into(),
            // PK 列(Id)だけをマッピング。他列はマッピングしない。
            columns: vec![VerifyColumn {
                name: "Id".into(),
                pk: true,
                src_index: 0,
            }],
            source: ImportSource::File(write_temp_csv("pkonly_target", csv)),
            has_header: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            empty_as_null: true,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (vtx, _v) = std::sync::mpsc::channel::<VerifyProgress>();
        let r = run_verify(&test_env(), &vreq, false, &vtx).await;
        assert_eq!(r.error, None, "verify error: {:?}", r.error);
        assert_eq!(r.matched, 2, "1,2 は存在");
        assert_eq!(r.value_mismatch, 0, "PK のみなので値差異は常に 0");
        assert_eq!(r.csv_only, 1, "4 は DB に無い（不足行）");
        assert_eq!(r.db_only, 1, "3 はスプレッドシートに無い");
    }

    /// CSV↔DB 照合: 一致 / 値差異 / CSVのみ / DBのみ を正しく数える（emulator 前提）。
    #[tokio::test]
    async fn verify_counts_match_mismatch_csvonly_dbonly() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _guard = EMU_LOCK.lock().await;
        create_import_table().await;

        // DB へ 3 行投入: Id 1/2/3。
        let seed = "Id,Name,Score,Active,Note\n\
                    1,alice,1.5,true,\n\
                    2,bob,2.0,false,hi\n\
                    3,carol,3.0,true,x\n";
        let seed_path = write_temp_csv("verify_seed", seed);
        let import = ImportRequest {
            id: 0,
            table: "ImportTest".into(),
            columns: cols(&[
                ("Id", "INT64"),
                ("Name", "STRING(MAX)"),
                ("Score", "FLOAT64"),
                ("Active", "BOOL"),
                ("Note", "STRING(MAX)"),
            ]),
            source: ImportSource::File(seed_path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &import, false, &ptx).await;
        assert_eq!(out.error, None, "seed import error: {:?}", out.error);

        // 照合用 CSV: Id1=完全一致, Id2=Name差異, Id4=CSVのみ。Id3 は DB のみ。
        let csv = "Id,Name,Score,Active,Note\n\
                   1,alice,1.5,true,\n\
                   2,bobX,2.0,false,hi\n\
                   4,dave,9.0,true,new\n";
        let csv_path = write_temp_csv("verify_target", csv);
        let vreq = VerifyRequest {
            table: "ImportTest".into(),
            columns: vec![
                VerifyColumn { name: "Id".into(), pk: true, src_index: 0 },
                VerifyColumn { name: "Name".into(), pk: false, src_index: 1 },
                VerifyColumn { name: "Score".into(), pk: false, src_index: 2 },
                VerifyColumn { name: "Active".into(), pk: false, src_index: 3 },
                VerifyColumn { name: "Note".into(), pk: false, src_index: 4 },
            ],
            source: ImportSource::File(csv_path),
            has_header: true,
            encoding: Encoding::Utf8,
            delimiter: b',',
            empty_as_null: true,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (vtx, _vrx) = std::sync::mpsc::channel::<VerifyProgress>();
        let r = run_verify(&test_env(), &vreq, false, &vtx).await;
        assert_eq!(r.error, None, "verify error: {:?}", r.error);
        assert_eq!(r.csv_rows, 3, "csv rows");
        assert_eq!(r.db_rows, 3, "db rows");
        assert_eq!(r.matched, 1, "matched (Id1)");
        assert_eq!(r.value_mismatch, 1, "value mismatch (Id2 Name)");
        assert_eq!(r.csv_only, 1, "csv only (Id4)");
        assert_eq!(r.db_only, 1, "db only (Id3)");
        // 値差異サンプルに Name の差分が含まれる。
        assert!(
            r.samples
                .iter()
                .any(|s| s.kind == VerifyKind::ValueMismatch && s.detail.contains("Name")),
            "Name 差分サンプルがない: {:?}",
            r.samples
        );
    }

    /// ヘッダ無し CSV + 列の並べ替え（src_index）でも正しく書き込めるか（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_no_header_reordered() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportReorder";
        create_import_table_named(table).await;
        let client = client().await;

        // CSV は [Name, Id] 並び・ヘッダ無し。テーブル列は Id, Name に逆引き。
        let csv = "alice,1\nbob,2\n";
        let path = write_temp_csv("reorder", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: vec![
                ImportColumn { name: "Id".into(), ty: "INT64".into(), src_index: 1 },
                ImportColumn { name: "Name".into(), ty: "STRING(MAX)".into(), src_index: 0 },
            ],
            source: ImportSource::File(path),
            has_header: false,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "err: {:?}", out.error);
        assert_eq!(out.written, 2);

        let (_, rows, _) = try_query(
            &client,
            &format!("SELECT Id, Name FROM {table} ORDER BY Id"),
        )
        .await
        .unwrap();
        assert_eq!(rows, vec![vec!["1", "alice"], vec!["2", "bob"]]);
    }

    /// 証跡レポート用の取込前後 COUNT(*) が記録され、新規挿入/更新の推定に使える
    /// （emulator 前提）。
    #[tokio::test]
    async fn import_records_before_after_counts() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await;
        let table = "ImportCounts";
        create_import_table_named(table).await; // 既存行は掃除される

        let run = |csv: &str, tag: &str| {
            let path = write_temp_csv(tag, csv);
            ImportRequest {
                id: 0,
                table: table.into(),
                columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
                source: ImportSource::File(path),
                has_header: true,
                mode: ImportMode::InsertOrUpdate,
                empty_as_null: true,
                fresh: false,
                encoding: Encoding::Utf8,
                delimiter: b',',
                skip_bad_rows: false,
                dry_run: false,
                null_token: None,
                cancel: Arc::new(AtomicBool::new(false)),
            }
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();

        // 1 回目: 空テーブルに 3 行（全部新規）。
        let out1 = run_streaming_import(&test_env(), &run("Id,Name\n1,a\n2,b\n3,c\n", "cnt1"), false, &ptx).await;
        assert_eq!(out1.before_count, Some(0));
        assert_eq!(out1.after_count, Some(3));

        // 2 回目: id 2,3 を更新 + id 4 を新規。
        let out2 = run_streaming_import(&test_env(), &run("Id,Name\n2,b2\n3,c2\n4,d\n", "cnt2"), false, &ptx).await;
        assert_eq!(out2.before_count, Some(3), "取込前は 3 行");
        assert_eq!(out2.after_count, Some(4), "取込後は 4 行（新規 1）");
        // 新規 = after-before = 1、更新 = written-新規 = 3-1 = 2。
        let inserted = out2.after_count.unwrap() - out2.before_count.unwrap();
        assert_eq!(inserted, 1);
        assert_eq!(out2.written as i64 - inserted, 2);
    }

    /// strict（skip_bad_rows=false）では、列数が一致しない行でエラーになり
    /// 書き込まれない（黙って NULL 埋めしない）（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_rejects_mismatched_cols_strict() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await;
        let table = "ImportMismatchStrict";
        create_import_table_named(table).await;
        let client = client().await;

        // 3 列（Id,Name,Score）にマップ。2 行目はフィールドが 1 個だけの短い行。
        let csv = "Id,Name,Score\n1,alice,9.5\n2\n";
        let path = write_temp_csv("mismatch_strict", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)"), ("Score", "FLOAT64")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert!(
            out.error.as_deref().is_some_and(|e| e.contains("列数")),
            "列数不一致でエラーになるべき: {:?}",
            out.error
        );
        // 不正行を含むバッチは書かれない（黙って NULL 埋めしない）。
        let (_, rows, _) = try_query(&client, &format!("SELECT Id FROM {table} ORDER BY Id"))
            .await
            .unwrap();
        assert!(rows.is_empty(), "不正バッチは書き込まれないはず: {rows:?}");
    }

    /// skip_bad_rows=true では、列数が一致しない行（短い/長い）はスキップされ、
    /// 正しい行だけが書き込まれる（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_skips_mismatched_cols() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _g = EMU_LOCK.lock().await;
        let table = "ImportMismatchSkip";
        create_import_table_named(table).await;
        let client = client().await;

        // 行2=短い, 行3=長すぎる, 行1/行4=正しい。
        let csv = "Id,Name,Score\n1,alice,9.5\n2\n3,carol,7.0,EXTRA\n4,dave,8.0\n";
        let path = write_temp_csv("mismatch_skip", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)"), ("Score", "FLOAT64")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: true,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "skip なのでエラーにしない: {:?}", out.error);
        assert_eq!(out.written, 2, "正しい 2 行だけ書き込む");
        assert_eq!(out.skipped, 2, "不正な 2 行はスキップ");
        let (_, rows, _) = try_query(&client, &format!("SELECT Id FROM {table} ORDER BY Id"))
            .await
            .unwrap();
        assert_eq!(rows, vec![vec!["1"], vec!["4"]], "正しい行のみ残る");
    }

    /// 空欄を NULL にしない設定では空文字列として書き込む（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_empty_as_string() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportEmptyStr";
        create_import_table_named(table).await;
        let client = client().await;

        let csv = "Id,Note\n1,\n";
        let path = write_temp_csv("emptystr", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Note", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: false, // 空欄 → 空文字列
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "err: {:?}", out.error);
        let (_, rows, _) = try_query(
            &client,
            &format!("SELECT Note, Note IS NULL FROM {table} WHERE Id = 1"),
        )
        .await
        .unwrap();
        assert_eq!(rows[0][0], ""); // 空文字列
        assert_eq!(rows[0][1], "false"); // NULL ではない
    }

    /// 主キー重複で部分適用になり、written < total・エラーありで返る（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_partial_on_duplicate() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportPartial";
        create_import_table_named(table).await;
        let client = client().await;

        // 先に Id=1 を入れておく。
        let p0 = write_temp_csv("partial_pre", "Id,Name\n1,pre\n");
        let seed = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(p0),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (s_tx, _s_rx) = std::sync::mpsc::channel::<ImportProgress>();
        assert_eq!(run_streaming_import(&test_env(), &seed, false, &s_tx).await.written, 1);

        // Id=1（重複）と Id=2（新規）を Insert。1 は失敗、2 は成功 → 部分適用。
        let csv = "Id,Name\n1,dup\n2,ok\n";
        let path = write_temp_csv("partial", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::Insert,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, _prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert!(out.error.is_some(), "duplicate PK should error");
        assert_eq!(out.total, 2);
        assert_eq!(out.written, 1, "only the new row commits");
        // Id=2 は書き込まれ、Id=1 は元のまま。
        let (_, rows, _) = try_query(
            &client,
            &format!("SELECT Name FROM {table} ORDER BY Id"),
        )
        .await
        .unwrap();
        assert_eq!(rows, vec![vec!["pre"], vec!["ok"]]);
    }

    /// 複数バッチにまたがる量でも全行入る（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_multi_batch() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportMulti";
        create_import_table_named(table).await;
        let client = client().await;

        // 2 列なら per_request = 20000/2 = 10000 行。25000 行で 3 バッチに分割される。
        let n = 25_000usize;
        let mut csv = String::from("Id,Name\n");
        for i in 0..n {
            csv.push_str(&format!("{i},n{i}\n"));
        }
        let path = write_temp_csv("multi", &csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let (ptx, prx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &ptx).await;
        assert_eq!(out.error, None, "err: {:?}", out.error);
        assert_eq!(out.written, n);
        // 進捗は複数回流れ、最後は全体サイズに到達する。
        let mut last_bytes = (0u64, None);
        let mut count = 0;
        for ev in prx.try_iter() {
            if let ImportProgress::Progress { bytes_done, bytes_total, .. } = ev {
                last_bytes = (bytes_done, bytes_total);
                count += 1;
            }
        }
        assert!(count >= 2, "expected several progress events, got {count}");
        if let (done, Some(total)) = last_bytes {
            assert_eq!(done, total, "final progress reaches full size");
        }
        let (_, rows, _) = try_query(&client, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        assert_eq!(rows[0][0], n.to_string());
    }

    /// チェックポイントに記録済みのバッチは再実行でスキップされる（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_resumes_from_checkpoint() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportResume";
        create_import_table_named(table).await;
        let client = client().await;

        // 3 行 → per_request(=10000) 未満なのでバッチは index 0 のみ。
        let csv = "Id,Name\n1,a\n2,b\n3,c\n";
        let path = write_temp_csv("resume", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let per_request = (BATCH_CELLS_PER_REQUEST / 2).max(1);
        let sig = import_signature(&req, per_request);
        let ckpt = checkpoint_path(&sig);
        // バッチ 0 を「コミット済み」として用意（実際にはまだ未書き込み）。
        // CSV は "Id,Name\n1,a\n2,b\n3,c\n" = ヘッダ8 + データ12 = 20 バイト、データ 3 行。
        // バッチ 0 の終端は 20 バイト目・累積 3 行。
        if let Some(p) = ckpt.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        std::fs::write(&ckpt, format!("{sig}\n0 20 3\n")).unwrap();

        // 再開: バッチ 0 まではシークで読み飛ばす → 1 件も書き込まれない。
        let (tx, _rx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &tx).await;
        assert_eq!(out.error, None, "err: {:?}", out.error);
        assert_eq!(out.resumed, 3, "skipped batch 0 = 3 rows");
        let (_, rows, _) = try_query(&client, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        assert_eq!(rows[0][0], "0", "skipped batch must not be re-written");
        // 成功でチェックポイントは消える。
        assert!(!ckpt.exists(), "checkpoint removed after success");

        // チェックポイントが無い状態で再実行 → 今度は全行書き込む。
        let (tx2, _rx2) = std::sync::mpsc::channel::<ImportProgress>();
        let out2 = run_streaming_import(&test_env(), &req, false, &tx2).await;
        assert_eq!(out2.error, None);
        assert_eq!(out2.resumed, 0);
        let (_, rows, _) = try_query(&client, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap();
        assert_eq!(rows[0][0], "3");
    }

    /// バイトオフセット再開: チェックポイントの終端位置までシークし、それ以降だけ書く
    /// （手前の行は読み直さず・再書き込みもしない）（emulator 前提）。
    #[tokio::test]
    async fn streaming_import_seek_resume_midfile() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _import_guard = EMU_LOCK.lock().await;
        let table = "ImportSeek";
        create_import_table_named(table).await;
        let client = client().await;

        // ヘッダ8B + "1,a\n".."5,e\n"(各4B)。2 行目の終端は 8+8=16 バイト目。
        let csv = "Id,Name\n1,a\n2,b\n3,c\n4,d\n5,e\n";
        let path = write_temp_csv("seek", csv);
        let req = ImportRequest {
            id: 0,
            table: table.into(),
            columns: cols(&[("Id", "INT64"), ("Name", "STRING(MAX)")]),
            source: ImportSource::File(path),
            has_header: true,
            mode: ImportMode::InsertOrUpdate,
            empty_as_null: true,
            fresh: false,
            encoding: Encoding::Utf8,
            delimiter: b',',
            skip_bad_rows: false,
            dry_run: false,
            null_token: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        let per_request = (BATCH_CELLS_PER_REQUEST / 2).max(1);
        let sig = import_signature(&req, per_request);
        let ckpt = checkpoint_path(&sig);
        if let Some(p) = ckpt.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        // バッチ 0 = 先頭 2 行を「コミット済み」（終端 16 バイト・累積 2 行）として用意。
        std::fs::write(&ckpt, format!("{sig}\n0 16 2\n")).unwrap();

        let (tx, _rx) = std::sync::mpsc::channel::<ImportProgress>();
        let out = run_streaming_import(&test_env(), &req, false, &tx).await;
        assert_eq!(out.error, None, "err: {:?}", out.error);
        assert_eq!(out.resumed, 2, "先頭 2 行はシークで読み飛ばす");
        assert_eq!(out.written, 3, "3 行目以降の 3 行だけ書く");

        // DB には Id 3,4,5 だけが入る（1,2 はシークで飛ばし未書き込み）。
        let (_, rows, _) = try_query(&client, &format!("SELECT Id FROM {table} ORDER BY Id"))
            .await
            .unwrap();
        let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
        assert_eq!(ids, vec!["3", "4", "5"]);
    }

    /// インポート検証用の標準テーブル ImportTest を作成（冪等・中身を消す）。
    async fn create_import_table() {
        create_import_table_named("ImportTest").await;
    }

    /// 指定名のインポート検証用テーブルを作成（冪等・中身を消す）。
    /// 並列テストでの干渉を避けるため、テストごとに別名を使える。
    async fn create_import_table_named(table: &str) {
        use gcloud_spanner::admin::client::Client as AdminClient;
        use gcloud_spanner::admin::AdminClientConfig;
        use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cfg = AdminClientConfig::default().with_auth().await.unwrap();
        let admin = AdminClient::new(cfg).await.unwrap();
        let req = UpdateDatabaseDdlRequest {
            database: emulator_db().unwrap(),
            statements: vec![format!(
                "CREATE TABLE IF NOT EXISTS {table} (Id INT64 NOT NULL, Name STRING(MAX), \
                 Score FLOAT64, Active BOOL, Note STRING(MAX)) PRIMARY KEY (Id)"
            )],
            ..Default::default()
        };
        let mut op = admin
            .database()
            .update_database_ddl(req, None)
            .await
            .unwrap();
        op.wait(None).await.unwrap();
        // 前回データを掃除（全キー削除のミューテーション）。
        let c = client().await;
        let _ = c
            .apply(vec![gcloud_spanner::mutation::delete(
                table,
                gcloud_spanner::key::all_keys(),
            )])
            .await;
    }

    // エミュレータは「同時に 1 トランザクション/スキーマ変更のみ」対応。DDL と
    // 読み書きトランザクションは互いに排他なので、エミュレータに触れる全テストを
    // この 1 つのロックで直列化する（並列実行でも衝突しない）。
    static EMU_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// テスト用に親子（インターリーブ）と外部キーを持つテーブルを作成（冪等）
    async fn create_dep_schema() {
        use gcloud_spanner::admin::client::Client as AdminClient;
        use gcloud_spanner::admin::AdminClientConfig;
        use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cfg = AdminClientConfig::default().with_auth().await.unwrap();
        let admin = AdminClient::new(cfg).await.unwrap();

        let req = UpdateDatabaseDdlRequest {
            database: emulator_db().unwrap(),
            statements: vec![
                "CREATE TABLE IF NOT EXISTS DepParent (Id INT64 NOT NULL) PRIMARY KEY (Id)".into(),
                "CREATE TABLE IF NOT EXISTS DepChild (Id INT64 NOT NULL, ChildId INT64 NOT NULL) \
                 PRIMARY KEY (Id, ChildId), INTERLEAVE IN PARENT DepParent ON DELETE CASCADE"
                    .into(),
                "CREATE TABLE IF NOT EXISTS DepOrders (OrderId INT64 NOT NULL, ParentId INT64, \
                 CONSTRAINT FK_DepOrders FOREIGN KEY (ParentId) REFERENCES DepParent (Id)) \
                 PRIMARY KEY (OrderId)"
                    .into(),
                "CREATE INDEX IF NOT EXISTS IDX_DepOrders_Parent ON DepOrders (ParentId)".into(),
            ],
            ..Default::default()
        };
        let mut op = admin
            .database()
            .update_database_ddl(req, None)
            .await
            .unwrap();
        op.wait(None).await.unwrap();
    }
}
