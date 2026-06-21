//! テーブルデータビューア用のクエリワーカー。
//! UI から SQL を受け取り、Spanner で実行して結果（列名 + 文字列化した行）を返す。
//! 監視側とは別系統で、オンデマンド（実行ボタン）で動く。

use std::collections::HashMap;
use std::time::Instant;

use gcloud_spanner::client::{Client, ClientConfig};
use gcloud_spanner::row::{Error as RowError, Row};
use gcloud_spanner::statement::Statement;
use tokio::sync::mpsc::UnboundedReceiver;

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
}

pub struct Config {
    pub project: String,
    pub instance: String,
    pub database: String,
    pub mock: bool,
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
}

const NO_CONFIG: &str = "SPANNER_PROJECT / SPANNER_INSTANCE / SPANNER_DATABASE を設定してください";

/// UI からのリクエストを順次処理する。req 側が閉じたら終了。
/// データクエリは `data_tx`、スキーマ図は `schema_tx` に結果を返す。
pub async fn query_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<(Target, String)>,
    data_tx: std::sync::mpsc::Sender<QueryOutcome>,
    schema_tx: std::sync::mpsc::Sender<SchemaGraph>,
) {
    let cfg = std::sync::Arc::new(cfg);
    // クライアントはタスク間で共有する。tokio の Mutex は poison しないので、
    // 1 リクエストがパニックしてもロックは解放され次のリクエストを処理できる。
    let client: std::sync::Arc<tokio::sync::Mutex<Option<Client>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(None));
    let configured =
        !(cfg.project.is_empty() || cfg.instance.is_empty() || cfg.database.is_empty());

    while let Some((target, sql)) = req_rx.recv().await {
        let cfg = cfg.clone();
        let client = client.clone();
        // タスク用とパニック時のエラー通知用に別々のクローンを用意する。
        let data_tx_task = data_tx.clone();
        let schema_tx_task = schema_tx.clone();
        let data_tx_err = data_tx.clone();
        let schema_tx_err = schema_tx.clone();

        // 各リクエストは独立タスクで実行する。Spanner クライアント側で万一
        // パニックが起きても、この query_loop 自体は生き続け、UI からの次の
        // 実行を受け付けられるようにする（実行ボタンが無反応になるのを防ぐ）。
        let handle = tokio::spawn(async move {
            match target {
                Target::Data => {
                    let start = Instant::now();
                    let mut guard = client.lock().await;
                    let mut out = if cfg.mock {
                        mock_data(&sql)
                    } else if !configured {
                        QueryOutcome {
                            error: Some(NO_CONFIG.into()),
                            ..Default::default()
                        }
                    } else {
                        ensure_and_run(&mut guard, &cfg, &sql).await
                    };
                    out.target = Target::Data;
                    out.elapsed_ms = start.elapsed().as_millis();
                    let _ = data_tx_task.send(out);
                }
                Target::Schema => {
                    let mut guard = client.lock().await;
                    let graph = if cfg.mock {
                        mock_graph()
                    } else if !configured {
                        SchemaGraph {
                            error: Some(NO_CONFIG.into()),
                            ..Default::default()
                        }
                    } else {
                        match ensure_client(&mut guard, &cfg).await {
                            Ok(c) => fetch_schema(c).await,
                            Err(e) => SchemaGraph {
                                error: Some(e),
                                ..Default::default()
                            },
                        }
                    };
                    let _ = schema_tx_task.send(graph);
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
            }
        }
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

/// クライアントを遅延生成して借用を返す。
async fn ensure_client<'a>(
    client: &'a mut Option<Client>,
    cfg: &Config,
) -> Result<&'a Client, String> {
    if client.is_none() {
        match build_client(cfg).await {
            Ok(c) => *client = Some(c),
            Err(e) => return Err(format!("接続/認証に失敗: {e}")),
        }
    }
    Ok(client.as_ref().unwrap())
}

async fn ensure_and_run(client: &mut Option<Client>, cfg: &Config, sql: &str) -> QueryOutcome {
    match ensure_client(client, cfg).await {
        Ok(c) => run_query(c, sql).await,
        Err(e) => QueryOutcome {
            error: Some(e),
            ..Default::default()
        },
    }
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
    })
}

async fn build_client(cfg: &Config) -> anyhow::Result<Client> {
    let db = format!(
        "projects/{}/instances/{}/databases/{}",
        cfg.project, cfg.instance, cfg.database
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
    let lower = sql.to_lowercase();
    let pos = lower.rfind("limit")?;
    sql[pos + "limit".len()..]
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// 実テーブルの読み取りと列名取得を検証
    #[tokio::test]
    async fn read_loadtest_rows() {
        let Some(_) = emulator_db() else {
            eprintln!("skip: SPANNER_EMULATOR_HOST 未設定");
            return;
        };
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
        let client = client().await;
        // LoadTest には十分な行がある前提（loadgen 実行済み）
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

    // エミュレータは同時スキーマ変更を拒否するため、DDL をプロセス内で直列化する。
    static DDL_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// テスト用に親子（インターリーブ）と外部キーを持つテーブルを作成（冪等）
    async fn create_dep_schema() {
        use gcloud_spanner::admin::client::Client as AdminClient;
        use gcloud_spanner::admin::AdminClientConfig;
        use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;

        let _guard = DDL_LOCK.lock().await;
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
