//! Kubernetes データ取得（kubectl 経由）。
//! - 監視: Docker Desktop 風のコンテナ一覧（Pod→コンテナ）+ ノード/namespace 集計
//! - 図: クラスタ構成（Pod→ノード / Pod→オーナー）
//!
//! kubectl 不在・クラスタ未接続でも mock で動作する。

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Config {
    pub mock: bool,
}

/// 選択中の kubectl コンテキスト（None なら current-context）。
static CONTEXT: Mutex<Option<String>> = Mutex::new(None);

/// UI から選択コンテキストを設定する。
pub fn set_context(ctx: Option<String>) {
    *CONTEXT.lock().unwrap() = ctx;
}

fn context_args() -> Vec<String> {
    match CONTEXT.lock().unwrap().clone() {
        Some(c) if !c.is_empty() => vec!["--context".into(), c],
        _ => Vec::new(),
    }
}

/// GUI（Finder 起動）では PATH が `/usr/bin:/bin` 程度しか無く、Homebrew や
/// gcloud SDK にある kubectl / gke-gcloud-auth-plugin を見つけられない。
/// k9s 等のターミナル起動では繋がるのにアプリだと繋がらない典型原因なので、
/// よくある bin ディレクトリを PATH に補う。
fn augmented_path() -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    let mut dirs: Vec<String> = std::env::var("PATH")
        .map(|p| p.split(':').map(String::from).collect())
        .unwrap_or_default();
    let extra = [
        "/opt/homebrew/bin".to_string(),
        "/usr/local/bin".to_string(),
        "/usr/bin".to_string(),
        "/bin".to_string(),
        format!("{home}/google-cloud-sdk/bin"),
        "/opt/homebrew/share/google-cloud-sdk/bin".to_string(),
        "/usr/local/share/google-cloud-sdk/bin".to_string(),
        format!("{home}/.rd/bin"), // Rancher Desktop
        format!("{home}/.krew/bin"), // kubectl krew プラグイン
    ];
    for d in extra {
        if !d.is_empty() && !dirs.iter().any(|x| x == &d) {
            dirs.push(d);
        }
    }
    dirs.join(":")
}

/// kubectl の実体を探す（PATH に無くても一般的な場所から見つける）。
fn kubectl_bin() -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates = [
        "/opt/homebrew/bin/kubectl".to_string(),
        "/usr/local/bin/kubectl".to_string(),
        "/usr/bin/kubectl".to_string(),
        format!("{home}/google-cloud-sdk/bin/kubectl"),
        format!("{home}/.rd/bin/kubectl"),
    ];
    for p in &candidates {
        if std::path::Path::new(p).exists() {
            return p.clone();
        }
    }
    "kubectl".to_string()
}

/// PATH を補った kubectl コマンド（非同期）。これを使って kubectl を起動する。
fn kubectl_cmd() -> tokio::process::Command {
    let mut c = tokio::process::Command::new(kubectl_bin());
    c.env("PATH", augmented_path());
    c
}

/// PATH を補った kubectl コマンド（同期。UI スレッドからのコンテキスト取得用）。
fn kubectl_cmd_sync() -> std::process::Command {
    let mut c = std::process::Command::new(kubectl_bin());
    c.env("PATH", augmented_path());
    c
}

/// 利用可能なコンテキスト一覧と現在のコンテキスト（同期・UI から呼ぶ）。
pub fn list_contexts_blocking() -> (Vec<String>, Option<String>) {
    let names = kubectl_cmd_sync()
        .args(["config", "get-contexts", "-o", "name"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let current = kubectl_cmd_sync()
        .args(["config", "current-context"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty());
    (names, current)
}

#[derive(Clone, Debug, Default)]
pub struct NodeUsage {
    pub name: String,
    pub cpu_pct: f64,
    pub mem_pct: f64,
    pub pods: usize,
    pub containers: usize,
}

/// 1 コンテナ（Docker Desktop の行に相当）
#[derive(Clone, Debug, Default)]
pub struct ContainerInfo {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub restarts: i64,
    pub state: String, // Running / CrashLoopBackOff / Completed など
    pub init: bool,
    pub cpu_milli: f64,
    pub mem_mib: f64,
}

/// Pod（展開すると containers が見える）
#[derive(Clone, Debug, Default)]
pub struct PodInfo {
    pub ns: String,
    pub name: String,
    pub phase: String,
    pub node: String,
    pub age: String,
    pub restarts: i64,
    pub cpu_milli: f64,
    pub mem_mib: f64,
    pub containers: Vec<ContainerInfo>,
}

/// namespace 別の集計
#[derive(Clone, Debug, Default)]
pub struct NsAgg {
    pub name: String,
    pub pods: usize,
    pub containers: usize,
}

#[derive(Clone, Debug, Default)]
pub struct KubeMetrics {
    pub nodes: Vec<NodeUsage>,
    pub pods: Vec<PodInfo>,
    pub namespaces: Vec<NsAgg>,
    pub error: Option<String>,
}

/// ログ取得リクエスト
#[derive(Clone, Debug)]
pub enum LogReq {
    /// 追従開始。直前のストリームが動いていれば中断してから開始する。
    Follow {
        ns: String,
        pod: String,
        container: String,
    },
    /// 追従停止。in-flight の `kubectl logs -f` を中断する（新規ストリームは開始しない）。
    Stop,
}

/// ログのストリーミングイベント。
#[derive(Clone, Debug)]
pub enum LogEvent {
    Start(String), // タイトル（新規ストリーム開始 → バッファクリア）
    Line(String),
    Error(String),
}

/// k8s 操作リクエスト。
/// Scale / RolloutRestart はバックエンド実装済み（UI 露出は今後）。
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum ActionReq {
    DeletePod {
        ns: String,
        pod: String,
    },
    Scale {
        ns: String,
        deploy: String,
        replicas: i32,
    },
    RolloutRestart {
        ns: String,
        deploy: String,
    },
    Describe {
        ns: String,
        kind: String,
        name: String,
    },
    /// 任意種別の削除（リソースブラウザ用、ns は省略可）。
    DeleteAny {
        kind: String,
        ns: Option<String>,
        name: String,
    },
    /// 任意の scale 可能リソースのスケール。
    ScaleAny {
        kind: String,
        ns: Option<String>,
        name: String,
        replicas: i32,
    },
    /// 任意の rollout restart 可能リソースの再起動。
    RestartAny {
        kind: String,
        ns: Option<String>,
        name: String,
    },
    /// 編集した YAML を適用する（kubectl apply -f -）。
    Apply {
        yaml: String,
    },
    /// コンテナ内でコマンドを実行する（kubectl exec -- sh -c）。出力はログ窓へ。
    Exec {
        ns: String,
        pod: String,
        container: String,
        command: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct ActionResult {
    pub message: String,
    pub describe: Option<(String, String)>, // (title, text) → ログ窓に表示
}

/// クラスタイベント（1件）
#[derive(Clone, Debug, Default)]
pub struct KubeEvent {
    pub warning: bool,
    pub reason: String,
    pub object: String,
    pub message: String,
    pub count: i64,
    pub age: String,
}

#[derive(Clone, Debug, Default)]
pub struct EventsResult {
    pub events: Vec<KubeEvent>,
    pub error: Option<String>,
}


// ── アーキテクチャ図（通信フロー型: Ingress → Service → Workload → Pod） ──

/// アーキ図のノード種別。左→右の列順でもある。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchKind {
    Ingress,
    Service,
    Workload,
    Pod,
}

impl ArchKind {
    /// 左→右の列インデックス。
    pub fn column(self) -> usize {
        match self {
            ArchKind::Ingress => 0,
            ArchKind::Service => 1,
            ArchKind::Workload => 2,
            ArchKind::Pod => 3,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ArchNode {
    pub id: String, // 一意キー "ns\u{1}kind\u{1}name"
    pub ns: String,
    pub kind: ArchKind,
    pub name: String,
    pub sub: String, // 補足（Workload 種別 / Pod phase / Service type 等）
}

/// 通信フロー型アーキテクチャ図のデータ。namespace ごとに描画側でグループ化する。
#[derive(Clone, Debug, Default)]
pub struct ArchGraph {
    pub nodes: Vec<ArchNode>,
    pub edges: Vec<(String, String)>, // (from_id, to_id) 左→右
    pub error: Option<String>,
}

fn arch_id(ns: &str, kind: &str, name: &str) -> String {
    format!("{ns}\u{1}{kind}\u{1}{name}")
}

/// selector の (k,v) がすべて labels に含まれるか（空 selector は不一致扱い）。
fn selector_matches(
    selector: &serde_json::Map<String, Value>,
    labels: &serde_json::Map<String, Value>,
) -> bool {
    !selector.is_empty() && selector.iter().all(|(k, v)| labels.get(k) == Some(v))
}

fn as_label_map(v: &Value) -> serde_json::Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

/// kubectl の各 JSON から通信フロー型グラフを組み立てる（純ロジック・テスト可能）。
fn build_arch(
    pods_json: &str,
    svc_json: &str,
    ing_json: &str,
    workload_jsons: &[(&str, &str)], // (kind表示名, json)
) -> ArchGraph {
    let items = |s: &str| -> Vec<Value> {
        serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v["items"].as_array().cloned())
            .unwrap_or_default()
    };
    let meta = |it: &Value| {
        (
            it["metadata"]["namespace"].as_str().unwrap_or("").to_string(),
            it["metadata"]["name"].as_str().unwrap_or("").to_string(),
        )
    };

    let mut nodes: Vec<ArchNode> = Vec::new();
    let ids: std::cell::RefCell<std::collections::HashSet<String>> = Default::default();
    let mut edges: Vec<(String, String)> = Vec::new();

    // Pod（ラベル保持）
    struct Pod {
        id: String,
        ns: String,
        labels: serde_json::Map<String, Value>,
    }
    let mut pods: Vec<Pod> = Vec::new();
    for it in items(pods_json) {
        let (ns, name) = meta(&it);
        if name.is_empty() {
            continue;
        }
        let id = arch_id(&ns, "Pod", &name);
        let phase = it["status"]["phase"].as_str().unwrap_or("").to_string();
        ids.borrow_mut().insert(id.clone());
        nodes.push(ArchNode {
            id: id.clone(),
            ns: ns.clone(),
            kind: ArchKind::Pod,
            name,
            sub: phase,
        });
        pods.push(Pod {
            id,
            ns,
            labels: as_label_map(&it["metadata"]["labels"]),
        });
    }

    // Workload（Deployment/StatefulSet/DaemonSet）
    struct Wl {
        id: String,
        ns: String,
        match_labels: serde_json::Map<String, Value>,
    }
    let mut workloads: Vec<Wl> = Vec::new();
    for (kind, json) in workload_jsons {
        for it in items(json) {
            let (ns, name) = meta(&it);
            if name.is_empty() {
                continue;
            }
            let id = arch_id(&ns, kind, &name);
            let ml = as_label_map(&it["spec"]["selector"]["matchLabels"]);
            ids.borrow_mut().insert(id.clone());
            nodes.push(ArchNode {
                id: id.clone(),
                ns: ns.clone(),
                kind: ArchKind::Workload,
                name,
                sub: (*kind).to_string(),
            });
            // Workload → Pod（selector がラベルに一致）
            for p in &pods {
                if p.ns == ns && selector_matches(&ml, &p.labels) {
                    edges.push((id.clone(), p.id.clone()));
                }
            }
            workloads.push(Wl {
                id,
                ns,
                match_labels: ml,
            });
        }
    }

    // Service（selector で Workload / Pod に接続）
    for it in items(svc_json) {
        let (ns, name) = meta(&it);
        if name.is_empty() {
            continue;
        }
        let id = arch_id(&ns, "Service", &name);
        let sel = as_label_map(&it["spec"]["selector"]);
        let svc_type = it["spec"]["type"].as_str().unwrap_or("ClusterIP").to_string();
        ids.borrow_mut().insert(id.clone());
        nodes.push(ArchNode {
            id: id.clone(),
            ns: ns.clone(),
            kind: ArchKind::Service,
            name,
            sub: svc_type,
        });
        // Service → Workload（あれば）。無ければ Service → Pod に直結。
        let mut linked_wl = false;
        for w in &workloads {
            if w.ns == ns && !sel.is_empty() && selector_matches(&sel, &w.match_labels) {
                edges.push((id.clone(), w.id.clone()));
                linked_wl = true;
            }
        }
        if !linked_wl {
            for p in &pods {
                if p.ns == ns && selector_matches(&sel, &p.labels) {
                    edges.push((id.clone(), p.id.clone()));
                }
            }
        }
    }

    // Ingress → Service（backend.service.name）
    for it in items(ing_json) {
        let (ns, name) = meta(&it);
        if name.is_empty() {
            continue;
        }
        let id = arch_id(&ns, "Ingress", &name);
        ids.borrow_mut().insert(id.clone());
        nodes.push(ArchNode {
            id: id.clone(),
            ns: ns.clone(),
            kind: ArchKind::Ingress,
            name,
            sub: "Ingress".into(),
        });
        let mut backends: Vec<String> = Vec::new();
        if let Some(b) = it["spec"]["defaultBackend"]["service"]["name"].as_str() {
            backends.push(b.to_string());
        }
        for rule in it["spec"]["rules"].as_array().into_iter().flatten() {
            for path in rule["http"]["paths"].as_array().into_iter().flatten() {
                if let Some(b) = path["backend"]["service"]["name"].as_str() {
                    backends.push(b.to_string());
                }
            }
        }
        for b in backends {
            let target = arch_id(&ns, "Service", &b);
            if ids.borrow().contains(&target) {
                edges.push((id.clone(), target));
            }
        }
    }

    edges.sort();
    edges.dedup();
    ArchGraph {
        nodes,
        edges,
        error: None,
    }
}

async fn fetch_arch(ns: &Option<String>) -> ArchGraph {
    async fn getj(sc: &[String], kind: &str) -> Result<String, String> {
        let mut argv: Vec<String> = vec!["get".into(), kind.into()];
        argv.extend(sc.iter().cloned());
        argv.push("-o".into());
        argv.push("json".into());
        run(&argv.iter().map(|s| s.as_str()).collect::<Vec<_>>()).await
    }
    let sc = scope_args(ns);
    // Pod 取得失敗＝接続不可として扱う（他は失敗しても線が出ないだけ）。
    let pods_json = match getj(&sc, "pods").await {
        Ok(o) => o,
        Err(e) => {
            return ArchGraph {
                error: Some(e),
                ..Default::default()
            }
        }
    };
    let svc_json = getj(&sc, "services").await.unwrap_or_default();
    let ing_json = getj(&sc, "ingresses").await.unwrap_or_default();
    let dep_json = getj(&sc, "deployments").await.unwrap_or_default();
    let sts_json = getj(&sc, "statefulsets").await.unwrap_or_default();
    let ds_json = getj(&sc, "daemonsets").await.unwrap_or_default();
    build_arch(
        &pods_json,
        &svc_json,
        &ing_json,
        &[
            ("Deployment", &dep_json),
            ("StatefulSet", &sts_json),
            ("DaemonSet", &ds_json),
        ],
    )
}

/// テスト用の通信フロー型グラフ（描画の視覚確認に使う）。
#[cfg(test)]
pub fn sample_arch_graph() -> ArchGraph {
    mock_arch()
}

/// モック用の通信フロー型グラフ（Ingress→Service→Deployment→Pod×2）。
fn mock_arch() -> ArchGraph {
    let ns = "app";
    let n = |kind: ArchKind, name: &str, sub: &str, k: &str| ArchNode {
        id: arch_id(ns, k, name),
        ns: ns.into(),
        kind,
        name: name.into(),
        sub: sub.into(),
    };
    let nodes = vec![
        n(ArchKind::Ingress, "web-ingress", "Ingress", "Ingress"),
        n(ArchKind::Service, "web", "ClusterIP", "Service"),
        n(ArchKind::Service, "db", "ClusterIP", "Service"),
        n(ArchKind::Workload, "web", "Deployment", "Deployment"),
        n(ArchKind::Workload, "db", "StatefulSet", "StatefulSet"),
        n(ArchKind::Pod, "web-7d9c-aaaaa", "Running", "Pod"),
        n(ArchKind::Pod, "web-7d9c-bbbbb", "Running", "Pod"),
        n(ArchKind::Pod, "db-0", "Running", "Pod"),
    ];
    let e = |a: (&str, &str), b: (&str, &str)| {
        (arch_id(ns, a.0, a.1), arch_id(ns, b.0, b.1))
    };
    let edges = vec![
        e(("Ingress", "web-ingress"), ("Service", "web")),
        e(("Service", "web"), ("Deployment", "web")),
        e(("Deployment", "web"), ("Pod", "web-7d9c-aaaaa")),
        e(("Deployment", "web"), ("Pod", "web-7d9c-bbbbb")),
        e(("Service", "db"), ("StatefulSet", "db")),
        e(("StatefulSet", "db"), ("Pod", "db-0")),
    ];
    ArchGraph {
        nodes,
        edges,
        error: None,
    }
}

// ── 汎用リソースブラウザ ──

/// リソースブラウザへのリクエスト。
#[derive(Clone, Debug)]
pub enum ResourceReq {
    /// 指定種別の一覧。namespace=None は全 namespace（-A）。
    List {
        kind: String,
        namespace: Option<String>,
    },
    /// 1 リソースの YAML（-o yaml）。
    Yaml {
        kind: String,
        ns: Option<String>,
        name: String,
    },
    /// 1 リソースの describe。
    Describe {
        kind: String,
        ns: Option<String>,
        name: String,
    },
    /// 編集用の YAML（取得結果は YAML エディタに表示）。
    YamlForEdit {
        kind: String,
        ns: Option<String>,
        name: String,
    },
    /// namespace 一覧（セレクタ用）。
    Namespaces,
}

/// 一覧の 1 行。
#[derive(Clone, Debug, Default)]
pub struct ResourceRow {
    pub namespace: String, // 空なら非 namespaced
    pub name: String,
    pub cells: Vec<String>, // columns に対応
}

/// 汎用リソース一覧。
#[derive(Clone, Debug, Default)]
pub struct ResourceList {
    pub kind: String,
    pub columns: Vec<String>, // NAMESPACE 列は除いた表示用ヘッダ
    pub rows: Vec<ResourceRow>,
    pub namespaced: bool,
    pub error: Option<String>,
}

/// リソースブラウザの結果。
#[derive(Clone, Debug)]
pub enum ResourceResult {
    List(ResourceList),
    /// YAML / describe のテキスト（ログ窓に表示）。
    Text {
        title: String,
        body: String,
    },
    /// 編集用 YAML（YAML エディタに表示）。
    EditText {
        title: String,
        body: String,
    },
    /// namespace 一覧（セレクタ更新用）。
    Namespaces(Vec<String>),
}

/// リソースブラウザのループ。
pub async fn resource_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<ResourceReq>,
    tx: std::sync::mpsc::Sender<ResourceResult>,
) {
    while let Some(req) = req_rx.recv().await {
        let r = if cfg.mock {
            mock_resource(&req)
        } else {
            run_resource(req).await
        };
        if tx.send(r).is_err() {
            break;
        }
    }
}

fn ns_args(ns: &Option<String>) -> Vec<String> {
    match ns {
        Some(n) if !n.is_empty() => vec!["-n".into(), n.clone()],
        _ => vec![],
    }
}

async fn run_resource(req: ResourceReq) -> ResourceResult {
    match req {
        ResourceReq::List { kind, namespace } => {
            let mut args: Vec<String> = vec!["get".into(), kind.clone()];
            match &namespace {
                Some(n) if !n.is_empty() => {
                    args.push("-n".into());
                    args.push(n.clone());
                }
                _ => args.push("-A".into()),
            }
            let argv: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            match run(&argv).await {
                Ok(out) => {
                    let (columns, rows, namespaced) = parse_table(&out);
                    ResourceResult::List(ResourceList {
                        kind,
                        columns,
                        rows,
                        namespaced,
                        error: None,
                    })
                }
                Err(e) => ResourceResult::List(ResourceList {
                    kind,
                    error: Some(e),
                    ..Default::default()
                }),
            }
        }
        ResourceReq::Yaml { kind, ns, name } => {
            let na = ns_args(&ns);
            let mut argv: Vec<&str> = vec!["get", &kind, &name];
            argv.extend(na.iter().map(|s| s.as_str()));
            argv.extend(["-o", "yaml"]);
            let title = format!("{kind}/{name} · YAML");
            text_result(run(&argv).await, title)
        }
        ResourceReq::Describe { kind, ns, name } => {
            let na = ns_args(&ns);
            let mut argv: Vec<&str> = vec!["describe", &kind, &name];
            argv.extend(na.iter().map(|s| s.as_str()));
            let title = format!("{kind}/{name} · describe");
            text_result(run(&argv).await, title)
        }
        ResourceReq::YamlForEdit { kind, ns, name } => {
            let na = ns_args(&ns);
            let mut argv: Vec<&str> = vec!["get", &kind, &name];
            argv.extend(na.iter().map(|s| s.as_str()));
            argv.extend(["-o", "yaml"]);
            let title = format!("{kind}/{name} · 編集");
            match run(&argv).await {
                Ok(body) => ResourceResult::EditText { title, body },
                Err(e) => ResourceResult::Text {
                    title,
                    body: format!("取得に失敗しました:\n{e}"),
                },
            }
        }
        ResourceReq::Namespaces => {
            let list = run(&["get", "namespaces", "-o", "name"])
                .await
                .map(|o| {
                    o.lines()
                        .filter_map(|l| l.trim().strip_prefix("namespace/"))
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            ResourceResult::Namespaces(list)
        }
    }
}

fn text_result(res: Result<String, String>, title: String) -> ResourceResult {
    match res {
        Ok(body) => ResourceResult::Text { title, body },
        Err(e) => ResourceResult::Text {
            title,
            body: format!("取得に失敗しました:\n{e}"),
        },
    }
}

/// `kubectl get` の表形式テキストを汎用パースする。
/// 戻り値: (表示用カラム, 行, namespaced か)。
/// 先頭カラムが NAMESPACE のときは namespaced とみなし、その列を行の ns に振り分ける。
fn parse_table(out: &str) -> (Vec<String>, Vec<ResourceRow>, bool) {
    let mut lines = out.lines().filter(|l| !l.trim().is_empty());
    let Some(header) = lines.next() else {
        return (Vec::new(), Vec::new(), false);
    };
    let raw_cols: Vec<String> = header.split_whitespace().map(|s| s.to_string()).collect();
    let namespaced = raw_cols.first().map(|c| c == "NAMESPACE").unwrap_or(false);
    // 表示用カラム（NAMESPACE は別管理）と name の列位置
    let display_cols: Vec<String> = if namespaced {
        raw_cols[1..].to_vec()
    } else {
        raw_cols.clone()
    };
    let total = raw_cols.len();

    let mut rows = Vec::new();
    for line in lines {
        // 末尾カラムに空白を含む値があり得るので total 個に丸める。
        let mut parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        if parts.len() > total && total > 0 {
            let tail = parts.split_off(total - 1).join(" ");
            parts.push(tail);
        }
        while parts.len() < total {
            parts.push(String::new());
        }
        let (namespace, rest) = if namespaced {
            (parts[0].clone(), parts[1..].to_vec())
        } else {
            (String::new(), parts.clone())
        };
        let name = rest.first().cloned().unwrap_or_default();
        rows.push(ResourceRow {
            namespace,
            name,
            cells: rest,
        });
    }
    (display_cols, rows, namespaced)
}

fn mock_resource(req: &ResourceReq) -> ResourceResult {
    match req {
        ResourceReq::List { kind, .. } => {
            let columns = vec!["NAME".into(), "STATUS".into(), "AGE".into()];
            let rows = (0..6)
                .map(|i| ResourceRow {
                    namespace: "default".into(),
                    name: format!("{kind}-mock-{i}"),
                    cells: vec![
                        format!("{kind}-mock-{i}"),
                        if i % 3 == 0 { "Pending" } else { "Active" }.into(),
                        format!("{}m", i * 7 + 3),
                    ],
                })
                .collect();
            ResourceResult::List(ResourceList {
                kind: kind.clone(),
                columns,
                rows,
                namespaced: true,
                error: None,
            })
        }
        ResourceReq::Yaml { kind, ns, name } => ResourceResult::Text {
            title: format!("{kind}/{name} · YAML"),
            body: format!(
                "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {}\n(mock yaml)",
                ns.as_deref().unwrap_or("")
            ),
        },
        ResourceReq::Describe { kind, ns, name } => ResourceResult::Text {
            title: format!("{kind}/{name} · describe"),
            body: format!(
                "Name: {name}\nNamespace: {}\nKind: {kind}\n(mock describe)",
                ns.as_deref().unwrap_or("")
            ),
        },
        ResourceReq::YamlForEdit { kind, ns, name } => ResourceResult::EditText {
            title: format!("{kind}/{name} · 編集"),
            body: format!(
                "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {}\n  labels:\n    edited: \"true\"\n(mock yaml — 編集してApplyを試せます)",
                ns.as_deref().unwrap_or("")
            ),
        },
        ResourceReq::Namespaces => ResourceResult::Namespaces(vec![
            "default".into(),
            "kube-system".into(),
            "kube-public".into(),
            "app".into(),
        ]),
    }
}

// ── port-forward ──

/// port-forward の開始/停止リクエスト。
#[derive(Clone, Debug)]
pub enum PortForwardReq {
    Start {
        id: u64,
        ns: String,
        target: String, // 例: "pod/foo" / "svc/bar"
        local: u16,
        remote: u16,
    },
    Stop {
        id: u64,
    },
}

/// port-forward の状態イベント。
#[derive(Clone, Debug)]
pub enum PortForwardEvent {
    Started { id: u64, label: String },
    Line { id: u64, text: String },
    Error { id: u64, msg: String },
    Stopped { id: u64 },
}

/// port-forward 管理ループ。プロセスを id ごとに保持し、停止要求で abort する。
pub async fn pf_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<PortForwardReq>,
    tx: std::sync::mpsc::Sender<PortForwardEvent>,
) {
    let mut running: HashMap<u64, tokio::task::JoinHandle<()>> = HashMap::new();
    while let Some(req) = req_rx.recv().await {
        match req {
            PortForwardReq::Start {
                id,
                ns,
                target,
                local,
                remote,
            } => {
                let label = format!("{target} {local}→{remote} ({ns})");
                if cfg.mock {
                    let _ = tx.send(PortForwardEvent::Started { id, label });
                    let _ = tx.send(PortForwardEvent::Line {
                        id,
                        text: format!("(mock) Forwarding from 127.0.0.1:{local} -> {remote}"),
                    });
                    continue;
                }
                // 自然終了した port-forward のハンドルを掃除（リーク防止）。
                running.retain(|_, h| !h.is_finished());
                let handle = tokio::spawn(pf_run(id, ns, target, local, remote, label, tx.clone()));
                // id 再利用時は古いタスクを止めてから差し替える（旧タスクの取りこぼし防止）。
                if let Some(old) = running.insert(id, handle) {
                    old.abort();
                }
            }
            PortForwardReq::Stop { id } => {
                if let Some(h) = running.remove(&id) {
                    h.abort();
                }
                let _ = tx.send(PortForwardEvent::Stopped { id });
            }
        }
    }
    for (_, h) in running {
        h.abort();
    }
}

async fn pf_run(
    id: u64,
    ns: String,
    target: String,
    local: u16,
    remote: u16,
    label: String,
    tx: std::sync::mpsc::Sender<PortForwardEvent>,
) {
    let ports = format!("{local}:{remote}");
    let mut child = match kubectl_cmd()
        .args(context_args())
        .args(["port-forward", "-n", &ns, &target, &ports])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(PortForwardEvent::Error {
                id,
                msg: format!("kubectl 実行失敗: {e}"),
            });
            return;
        }
    };

    let _ = tx.send(PortForwardEvent::Started { id, label });

    // stderr を別タスクで並行排出。長時間転送で stderr のパイプバッファが埋まると
    // 子プロセスが書き込みでブロックし、stdout 読み出しがハングするのを防ぐ。
    let stderr_task = child.stderr.take().map(|err| {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(PortForwardEvent::Error { id, msg: line }).is_err() {
                    break;
                }
            }
        })
    });

    if let Some(out) = child.stdout.take() {
        let mut lines = tokio::io::BufReader::new(out).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if tx.send(PortForwardEvent::Line { id, text: line }).is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(PortForwardEvent::Error {
                        id,
                        msg: e.to_string(),
                    });
                    break;
                }
            }
        }
    }

    // 子プロセス終了を待つ。stderr タスクは子の終了で EOF に達して自然に終わる。
    let _ = child.wait().await;
    if let Some(h) = stderr_task {
        let _ = h.await;
    }
    let _ = tx.send(PortForwardEvent::Stopped { id });
}

/// 監視ループ。間隔は共有 Atomic から都度読む（設定パネルで変更可能）。
pub async fn monitor_loop(
    cfg: Config,
    interval: std::sync::Arc<std::sync::atomic::AtomicU64>,
    tx: std::sync::mpsc::Sender<KubeMetrics>,
) {
    loop {
        let m = if cfg.mock {
            mock_metrics()
        } else {
            fetch_metrics().await
        };
        if tx.send(m).is_err() {
            break;
        }
        let secs = interval.load(std::sync::atomic::Ordering::Relaxed).max(1);
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }
}

/// ログ追従ループ。新しいリクエストが来たら直前のストリームを中断する。
pub async fn logs_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<LogReq>,
    tx: std::sync::mpsc::Sender<LogEvent>,
) {
    let mut handle: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(req) = req_rx.recv().await {
        // 新規リクエストでもStopでも、まず直前の kubectl logs -f を停止（kill_on_drop）
        if let Some(h) = handle.take() {
            h.abort();
        }
        match req {
            LogReq::Follow { ns, pod, container } => {
                handle = Some(tokio::spawn(stream_logs(
                    ns,
                    pod,
                    container,
                    cfg.mock,
                    tx.clone(),
                )));
            }
            // 停止のみ。新規ストリームは開始しない。
            LogReq::Stop => {}
        }
    }
    if let Some(h) = handle.take() {
        h.abort();
    }
}

async fn stream_logs(
    ns: String,
    pod: String,
    container: String,
    mock: bool,
    tx: std::sync::mpsc::Sender<LogEvent>,
) {
    let title = format!("{ns}/{pod} · {container}");
    if tx.send(LogEvent::Start(title)).is_err() {
        return;
    }

    if mock {
        for i in 0..100000 {
            let line = format!(
                "2026-06-19T10:00:{:02}Z INFO  {} log line {}",
                i % 60,
                container,
                i
            );
            if tx.send(LogEvent::Line(line)).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(800)).await;
        }
        return;
    }

    let mut args: Vec<String> = vec![
        "logs".into(),
        "-f".into(),
        "-n".into(),
        ns.clone(),
        pod.clone(),
        "--tail=200".into(),
    ];
    if container.is_empty() {
        // コンテナ未指定（リソースブラウザからの起動）。全コンテナをまとめて追従。
        args.push("--all-containers=true".into());
        args.push("--prefix".into());
    } else {
        args.push("-c".into());
        args.push(container.clone());
    }
    let mut child = match kubectl_cmd()
        .args(context_args())
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(LogEvent::Error(format!("kubectl 実行失敗: {e}")));
            return;
        }
    };

    // stderr を別タスクで並行して読み出す。長時間追従で stderr の OS パイプバッファ
    // (~64KB) が埋まると子プロセスが書き込みでブロックし、stdout の供給も止まって
    // こちらの読み出しがハングするため、必ず stdout と同時に排出する。
    let stderr_task = child.stderr.take().map(|err| {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(err).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(LogEvent::Error(line)).is_err() {
                    break;
                }
            }
        })
    });

    if let Some(out) = child.stdout.take() {
        let mut lines = tokio::io::BufReader::new(out).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if tx.send(LogEvent::Line(line)).is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(LogEvent::Error(e.to_string()));
                    break;
                }
            }
        }
    }

    // 子プロセス終了を待つ。stderr タスクは子の終了で EOF に達して自然に終わる。
    let _ = child.wait().await;
    if let Some(h) = stderr_task {
        let _ = h.await;
    }
}

/// k8s 操作ループ。
pub async fn action_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<ActionReq>,
    tx: std::sync::mpsc::Sender<ActionResult>,
) {
    while let Some(a) = req_rx.recv().await {
        let r = if cfg.mock {
            ActionResult {
                message: format!("(mock) {}", action_label(&a)),
                describe: describe_mock(&a),
            }
        } else {
            run_action(a).await
        };
        if tx.send(r).is_err() {
            break;
        }
    }
}

fn action_label(a: &ActionReq) -> String {
    match a {
        ActionReq::DeletePod { pod, .. } => format!("Pod {pod} を削除"),
        ActionReq::Scale {
            deploy, replicas, ..
        } => format!("{deploy} を {replicas} にスケール"),
        ActionReq::RolloutRestart { deploy, .. } => format!("{deploy} を再起動"),
        ActionReq::Describe { kind, name, .. } => format!("describe {kind}/{name}"),
        ActionReq::DeleteAny { kind, name, .. } => format!("{kind}/{name} を削除"),
        ActionReq::ScaleAny {
            kind,
            name,
            replicas,
            ..
        } => format!("{kind}/{name} を {replicas} にスケール"),
        ActionReq::RestartAny { kind, name, .. } => format!("{kind}/{name} を再起動"),
        ActionReq::Apply { .. } => "YAML を適用".to_string(),
        ActionReq::Exec { pod, command, .. } => format!("exec {pod}: {command}"),
    }
}

fn describe_mock(a: &ActionReq) -> Option<(String, String)> {
    match a {
        ActionReq::Describe { kind, name, ns } => Some((
            format!("describe {kind}/{name}"),
            format!("Name: {name}\nNamespace: {ns}\nKind: {kind}\n(mock describe)"),
        )),
        ActionReq::Exec {
            ns, pod, command, ..
        } => Some((
            format!("{ns}/{pod} · exec"),
            format!("$ {command}\n(mock) コマンド出力の例\nhello from {pod}"),
        )),
        _ => None,
    }
}

async fn run_action(a: ActionReq) -> ActionResult {
    let label = action_label(&a);
    match a {
        ActionReq::DeletePod { ns, pod } => {
            simple(run(&["delete", "pod", "-n", &ns, &pod]).await, label)
        }
        ActionReq::Scale {
            ns,
            deploy,
            replicas,
        } => {
            let rep = format!("--replicas={replicas}");
            simple(
                run(&["scale", "deployment", "-n", &ns, &deploy, &rep]).await,
                label,
            )
        }
        ActionReq::RolloutRestart { ns, deploy } => simple(
            run(&["rollout", "restart", "deployment", "-n", &ns, &deploy]).await,
            label,
        ),
        ActionReq::Describe { ns, kind, name } => {
            match run(&["describe", &kind, "-n", &ns, &name]).await {
                Ok(o) => ActionResult {
                    message: label.clone(),
                    describe: Some((label, o)),
                },
                Err(e) => ActionResult {
                    message: format!("describe 失敗: {e}"),
                    describe: None,
                },
            }
        }
        ActionReq::DeleteAny { kind, ns, name } => {
            let na = ns_args(&ns);
            let mut argv: Vec<&str> = vec!["delete", &kind, &name];
            argv.extend(na.iter().map(|s| s.as_str()));
            simple(run(&argv).await, label)
        }
        ActionReq::ScaleAny {
            kind,
            ns,
            name,
            replicas,
        } => {
            let na = ns_args(&ns);
            let rep = format!("--replicas={replicas}");
            let target = format!("{kind}/{name}");
            let mut argv: Vec<&str> = vec!["scale", &target, &rep];
            argv.extend(na.iter().map(|s| s.as_str()));
            simple(run(&argv).await, label)
        }
        ActionReq::RestartAny { kind, ns, name } => {
            let na = ns_args(&ns);
            let target = format!("{kind}/{name}");
            let mut argv: Vec<&str> = vec!["rollout", "restart", &target];
            argv.extend(na.iter().map(|s| s.as_str()));
            simple(run(&argv).await, label)
        }
        ActionReq::Apply { yaml } => match run_stdin(&["apply", "-f", "-"], &yaml).await {
            Ok(o) => ActionResult {
                message: format!("適用しました: {}", o.trim().lines().last().unwrap_or("")),
                describe: None,
            },
            Err(e) => ActionResult {
                message: format!("apply 失敗: {e}"),
                describe: None,
            },
        },
        ActionReq::Exec {
            ns,
            pod,
            container,
            command,
        } => {
            let mut argv: Vec<&str> = vec!["exec", "-n", &ns, &pod];
            if !container.is_empty() {
                argv.extend(["-c", &container]);
            }
            argv.extend(["--", "sh", "-c", &command]);
            let title = format!("{ns}/{pod} · exec");
            let body = match run_combined(&argv).await {
                Ok(o) if o.is_empty() => "(出力なし)".to_string(),
                Ok(o) => o,
                Err(e) => format!("exec 失敗:\n{e}"),
            };
            ActionResult {
                message: format!("exec {pod}"),
                describe: Some((title, body)),
            }
        }
    }
}

/// stdin にデータを渡して kubectl を実行する（apply 用）。
async fn run_stdin(args: &[&str], input: &str) -> Result<String, String> {
    use tokio::io::AsyncWriteExt;
    let mut child = kubectl_cmd()
        .args(context_args())
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("kubectl 実行失敗: {e}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(input.as_bytes()).await;
        // drop で EOF を送る
    }
    let out = child
        .wait_with_output()
        .await
        .map_err(|e| format!("kubectl 実行失敗: {e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        return Err(err.trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// stdout/stderr を結合して返す（exec 用、非ゼロ終了でも出力を見せる）。
async fn run_combined(args: &[&str]) -> Result<String, String> {
    let out = kubectl_cmd()
        .args(context_args())
        .args(args)
        .output()
        .await
        .map_err(|e| format!("kubectl 実行失敗: {e}"))?;
    let mut s = String::from_utf8_lossy(&out.stdout).to_string();
    let err = String::from_utf8_lossy(&out.stderr);
    if !err.trim().is_empty() {
        if !s.is_empty() {
            s.push('\n');
        }
        s.push_str(err.trim());
    }
    Ok(s)
}

fn simple(res: Result<String, String>, ok_msg: String) -> ActionResult {
    match res {
        Ok(_) => ActionResult {
            message: ok_msg,
            describe: None,
        },
        Err(e) => ActionResult {
            message: format!("失敗: {e}"),
            describe: None,
        },
    }
}

/// namespace スコープの引数。None / 空なら全 namespace（-A）。
fn scope_args(ns: &Option<String>) -> Vec<String> {
    match ns {
        Some(n) if !n.is_empty() => vec!["-n".into(), n.clone()],
        _ => vec!["-A".into()],
    }
}

/// イベント取得ループ。要求は対象 namespace（None = 全て）。
pub async fn events_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<Option<String>>,
    tx: std::sync::mpsc::Sender<EventsResult>,
) {
    while let Some(ns) = req_rx.recv().await {
        let r = if cfg.mock {
            EventsResult {
                events: mock_events(),
                error: None,
            }
        } else {
            fetch_events(&ns).await
        };
        if tx.send(r).is_err() {
            break;
        }
    }
}

/// 構成図ループ。要求は対象 namespace（None = 全て）。
pub async fn topology_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<Option<String>>,
    tx: std::sync::mpsc::Sender<ArchGraph>,
) {
    while let Some(ns) = req_rx.recv().await {
        let g = if cfg.mock {
            mock_arch()
        } else {
            fetch_arch(&ns).await
        };
        if tx.send(g).is_err() {
            break;
        }
    }
}

async fn run(args: &[&str]) -> Result<String, String> {
    let out = kubectl_cmd()
        .args(context_args())
        .args(args)
        .output()
        .await
        .map_err(|e| format!("kubectl 実行失敗: {e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        // 接続不可（クラスタ未起動・コンテキスト誤り等）は分かりやすい文言にする。
        if err.contains("connection refused")
            || err.contains("The connection to the server")
            || err.contains("couldn't get current server API group list")
            || err.contains("did you specify the right host or port")
            || err.contains("no such host")
        {
            return Err(
                "クラスタに接続できません。Kubernetes が起動しているか、kubectl の\
                 コンテキストが正しいか確認してください（例: OrbStack/Docker Desktop の \
                 Kubernetes を有効化）。"
                    .into(),
            );
        }
        let line = err.lines().last().unwrap_or("").trim();
        return Err(if line.is_empty() {
            "kubectl エラー".into()
        } else {
            line.to_string()
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

// ── 監視 ──

async fn fetch_metrics() -> KubeMetrics {
    // ノード使用量は metrics-server が無いクラスタでは取れない。取れなくても
    // 致命的にせず空のまま続行する（接続可否は get pods で判定する）。
    let mut nodes = run(&["top", "nodes", "--no-headers"])
        .await
        .map(|o| parse_nodes(&o))
        .unwrap_or_default();

    // コンテナ単位の使用量（metrics-server がなければ空）
    let cusage = run(&["top", "pods", "-A", "--containers", "--no-headers"])
        .await
        .map(|o| parse_container_usage(&o))
        .unwrap_or_default();

    let pj = match run(&["get", "pods", "-A", "-o", "json"]).await {
        Ok(o) => o,
        Err(e) => {
            return KubeMetrics {
                nodes,
                error: Some(e),
                ..Default::default()
            }
        }
    };

    let pods = parse_pods(&pj, &cusage);

    // 集計（ノード別・namespace 別）。全体サマリは UI 側で算出する。
    let mut per_node: HashMap<String, (usize, usize)> = HashMap::new();
    let mut per_ns: HashMap<String, (usize, usize)> = HashMap::new();
    for p in &pods {
        if !p.node.is_empty() {
            let e = per_node.entry(p.node.clone()).or_insert((0, 0));
            e.0 += 1;
            e.1 += p.containers.len();
        }
        let e = per_ns.entry(p.ns.clone()).or_insert((0, 0));
        e.0 += 1;
        e.1 += p.containers.len();
    }
    for n in &mut nodes {
        if let Some((p, c)) = per_node.get(&n.name) {
            n.pods = *p;
            n.containers = *c;
        }
    }
    let mut namespaces: Vec<NsAgg> = per_ns
        .into_iter()
        .map(|(name, (pods, containers))| NsAgg {
            name,
            pods,
            containers,
        })
        .collect();
    namespaces.sort_by(|a, b| b.containers.cmp(&a.containers).then(a.name.cmp(&b.name)));

    KubeMetrics {
        nodes,
        pods,
        namespaces,
        error: None,
    }
}

fn parse_pct(s: &str) -> f64 {
    s.trim_end_matches('%').parse().unwrap_or(0.0)
}

fn parse_cpu_milli(s: &str) -> f64 {
    if let Some(m) = s.strip_suffix('m') {
        m.parse().unwrap_or(0.0)
    } else {
        s.parse::<f64>().unwrap_or(0.0) * 1000.0
    }
}

fn parse_mem_mib(s: &str) -> f64 {
    if let Some(v) = s.strip_suffix("Gi") {
        v.parse::<f64>().unwrap_or(0.0) * 1024.0
    } else if let Some(v) = s.strip_suffix("Mi") {
        v.parse().unwrap_or(0.0)
    } else if let Some(v) = s.strip_suffix("Ki") {
        v.parse::<f64>().unwrap_or(0.0) / 1024.0
    } else {
        s.parse::<f64>().unwrap_or(0.0) / (1024.0 * 1024.0)
    }
}

// NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
fn parse_nodes(s: &str) -> Vec<NodeUsage> {
    s.lines()
        .filter_map(|l| {
            let c: Vec<_> = l.split_whitespace().collect();
            (c.len() >= 5).then(|| NodeUsage {
                name: c[0].to_string(),
                cpu_pct: parse_pct(c[2]),
                mem_pct: parse_pct(c[4]),
                ..Default::default()
            })
        })
        .collect()
}

// NAMESPACE POD NAME(container) CPU MEMORY → (ns, pod, container) -> (milli, mib)
fn parse_container_usage(s: &str) -> HashMap<(String, String, String), (f64, f64)> {
    s.lines()
        .filter_map(|l| {
            let c: Vec<_> = l.split_whitespace().collect();
            (c.len() >= 5).then(|| {
                (
                    (c[0].to_string(), c[1].to_string(), c[2].to_string()),
                    (parse_cpu_milli(c[3]), parse_mem_mib(c[4])),
                )
            })
        })
        .collect()
}

fn age_from(ts: &str) -> String {
    let Ok(t) = chrono::DateTime::parse_from_rfc3339(ts) else {
        return String::new();
    };
    let secs = (Utc::now() - t.with_timezone(&Utc)).num_seconds().max(0);
    if secs >= 86400 {
        format!("{}d", secs / 86400)
    } else if secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn state_str(status: &Value) -> String {
    let state = &status["state"];
    if state.get("running").is_some() {
        "Running".into()
    } else if let Some(w) = state.get("waiting") {
        w["reason"].as_str().unwrap_or("Waiting").to_string()
    } else if let Some(t) = state.get("terminated") {
        t["reason"].as_str().unwrap_or("Terminated").to_string()
    } else {
        String::new()
    }
}

fn parse_pods(pj: &str, cusage: &HashMap<(String, String, String), (f64, f64)>) -> Vec<PodInfo> {
    let Ok(v) = serde_json::from_str::<Value>(pj) else {
        return Vec::new();
    };
    let mut pods = Vec::new();
    for item in v["items"].as_array().into_iter().flatten() {
        let name = item["metadata"]["name"].as_str().unwrap_or("?").to_string();
        let ns = item["metadata"]["namespace"]
            .as_str()
            .unwrap_or("default")
            .to_string();
        let phase = item["status"]["phase"].as_str().unwrap_or("").to_string();
        let node = item["spec"]["nodeName"].as_str().unwrap_or("").to_string();
        let age = item["metadata"]["creationTimestamp"]
            .as_str()
            .map(age_from)
            .unwrap_or_default();

        // status を name で引けるように
        let status_by_name = |arr: &str| -> HashMap<String, Value> {
            item["status"][arr]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|s| s["name"].as_str().map(|n| (n.to_string(), s.clone())))
                .collect()
        };
        let cstat = status_by_name("containerStatuses");
        let istat = status_by_name("initContainerStatuses");

        let mut containers = Vec::new();
        let mut build = |spec_key: &str, stat: &HashMap<String, Value>, init: bool| {
            for c in item["spec"][spec_key].as_array().into_iter().flatten() {
                let cname = c["name"].as_str().unwrap_or("").to_string();
                let st = stat.get(&cname);
                let image = st
                    .and_then(|s| s["image"].as_str())
                    .or_else(|| c["image"].as_str())
                    .unwrap_or("")
                    .to_string();
                let ready = st.and_then(|s| s["ready"].as_bool()).unwrap_or(false);
                let restarts = st.and_then(|s| s["restartCount"].as_i64()).unwrap_or(0);
                let state = st.map(state_str).unwrap_or_default();
                let (cpu_milli, mem_mib) = cusage
                    .get(&(ns.clone(), name.clone(), cname.clone()))
                    .copied()
                    .unwrap_or((0.0, 0.0));
                containers.push(ContainerInfo {
                    name: cname,
                    image,
                    ready,
                    restarts,
                    state,
                    init,
                    cpu_milli,
                    mem_mib,
                });
            }
        };
        build("initContainers", &istat, true);
        build("containers", &cstat, false);

        let restarts = containers.iter().map(|c| c.restarts).sum();
        let cpu_milli = containers.iter().map(|c| c.cpu_milli).sum();
        let mem_mib = containers.iter().map(|c| c.mem_mib).sum();

        pods.push(PodInfo {
            ns,
            name,
            phase,
            node,
            age,
            restarts,
            cpu_milli,
            mem_mib,
            containers,
        });
    }
    pods.sort_by(|a, b| a.ns.cmp(&b.ns).then(a.name.cmp(&b.name)));
    pods
}

// ── 構成図 ──

// ── イベント ──

async fn fetch_events(ns: &Option<String>) -> EventsResult {
    let sc = scope_args(ns);
    let mut argv: Vec<&str> = vec!["get", "events"];
    argv.extend(sc.iter().map(|s| s.as_str()));
    argv.extend(["-o", "json"]);
    match run(&argv).await {
        Ok(o) => EventsResult {
            events: parse_events(&o),
            error: None,
        },
        Err(e) => EventsResult {
            events: Vec::new(),
            error: Some(e),
        },
    }
}

fn parse_events(json: &str) -> Vec<KubeEvent> {
    let Ok(v) = serde_json::from_str::<Value>(json) else {
        return Vec::new();
    };
    let mut events: Vec<KubeEvent> = v["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|it| {
            let warning = it["type"].as_str() == Some("Warning");
            let obj = &it["involvedObject"];
            let object = format!(
                "{}/{}",
                obj["kind"].as_str().unwrap_or(""),
                obj["name"].as_str().unwrap_or("")
            );
            let ts = it["lastTimestamp"]
                .as_str()
                .or_else(|| it["eventTime"].as_str())
                .unwrap_or("");
            KubeEvent {
                warning,
                reason: it["reason"].as_str().unwrap_or("").to_string(),
                object,
                message: it["message"].as_str().unwrap_or("").to_string(),
                count: it["count"].as_i64().unwrap_or(1),
                age: age_from(ts),
            }
        })
        .collect();
    // Warning を先頭に
    events.sort_by_key(|e| std::cmp::Reverse(e.warning));
    events
}

fn mock_events() -> Vec<KubeEvent> {
    vec![
        KubeEvent {
            warning: true,
            reason: "BackOff".into(),
            object: "Pod/job-broken-q1".into(),
            message: "Back-off restarting failed container worker".into(),
            count: 6,
            age: "8m".into(),
        },
        KubeEvent {
            warning: false,
            reason: "Scheduled".into(),
            object: "Pod/api-7c9-abc".into(),
            message: "Successfully assigned default/api-7c9-abc to node-1".into(),
            count: 1,
            age: "3d".into(),
        },
        KubeEvent {
            warning: false,
            reason: "Pulled".into(),
            object: "Pod/web-5d-xyz".into(),
            message: "Container image \"nginx:1.27\" already present on machine".into(),
            count: 1,
            age: "5h".into(),
        },
    ]
}

// ── モック ──

fn mock_metrics() -> KubeMetrics {
    let ctr =
        |name: &str, image: &str, state: &str, restarts: i64, init: bool, cpu: f64, mem: f64| {
            ContainerInfo {
                name: name.into(),
                image: image.into(),
                ready: state == "Running",
                restarts,
                state: state.into(),
                init,
                cpu_milli: cpu,
                mem_mib: mem,
            }
        };
    let pods = vec![
        PodInfo {
            ns: "default".into(),
            name: "api-7c9-abc".into(),
            phase: "Running".into(),
            node: "node-1".into(),
            age: "3d".into(),
            restarts: 1,
            cpu_milli: 250.0,
            mem_mib: 180.0,
            containers: vec![
                ctr(
                    "init-migrate",
                    "migrate:1.2",
                    "Completed",
                    0,
                    true,
                    0.0,
                    0.0,
                ),
                ctr("api", "myorg/api:1.8.0", "Running", 1, false, 230.0, 160.0),
                ctr(
                    "sidecar",
                    "envoyproxy/envoy:v1.30",
                    "Running",
                    0,
                    false,
                    20.0,
                    20.0,
                ),
            ],
        },
        PodInfo {
            ns: "default".into(),
            name: "web-5d-xyz".into(),
            phase: "Running".into(),
            node: "node-2".into(),
            age: "5h".into(),
            restarts: 0,
            cpu_milli: 80.0,
            mem_mib: 90.0,
            containers: vec![ctr("web", "nginx:1.27", "Running", 0, false, 80.0, 90.0)],
        },
        PodInfo {
            ns: "monitoring".into(),
            name: "prometheus-0".into(),
            phase: "Running".into(),
            node: "node-3".into(),
            age: "12d".into(),
            restarts: 2,
            cpu_milli: 95.0,
            mem_mib: 512.0,
            containers: vec![ctr(
                "prometheus",
                "prom/prometheus:v2.53",
                "Running",
                2,
                false,
                95.0,
                512.0,
            )],
        },
        PodInfo {
            ns: "default".into(),
            name: "job-broken-q1".into(),
            phase: "Pending".into(),
            node: "node-1".into(),
            age: "8m".into(),
            restarts: 6,
            cpu_milli: 0.0,
            mem_mib: 0.0,
            containers: vec![ctr(
                "worker",
                "myorg/worker:0.3",
                "CrashLoopBackOff",
                6,
                false,
                0.0,
                0.0,
            )],
        },
    ];
    KubeMetrics {
        nodes: vec![
            NodeUsage {
                name: "node-1".into(),
                cpu_pct: 42.0,
                mem_pct: 55.0,
                pods: 8,
                containers: 11,
            },
            NodeUsage {
                name: "node-2".into(),
                cpu_pct: 18.0,
                mem_pct: 33.0,
                pods: 5,
                containers: 6,
            },
            NodeUsage {
                name: "node-3".into(),
                cpu_pct: 76.0,
                mem_pct: 61.0,
                pods: 12,
                containers: 18,
            },
        ],
        namespaces: vec![
            NsAgg {
                name: "default".into(),
                pods: 10,
                containers: 16,
            },
            NsAgg {
                name: "kube-system".into(),
                pods: 9,
                containers: 12,
            },
            NsAgg {
                name: "monitoring".into(),
                pods: 6,
                containers: 13,
            },
        ],
        pods,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// build_arch: Ingress→Service→Workload→Pod のエッジがラベル一致で張られる。
    #[test]
    fn build_arch_edges() {
        let pods = r#"{"items":[
            {"metadata":{"namespace":"app","name":"web-1","labels":{"app":"web"}},"status":{"phase":"Running"}},
            {"metadata":{"namespace":"app","name":"web-2","labels":{"app":"web"}},"status":{"phase":"Running"}}
        ]}"#;
        let svcs = r#"{"items":[
            {"metadata":{"namespace":"app","name":"web"},"spec":{"selector":{"app":"web"},"type":"ClusterIP"}}
        ]}"#;
        let ings = r#"{"items":[
            {"metadata":{"namespace":"app","name":"web-ing"},"spec":{"rules":[{"http":{"paths":[{"backend":{"service":{"name":"web"}}}]}}]}}
        ]}"#;
        let deps = r#"{"items":[
            {"metadata":{"namespace":"app","name":"web"},"spec":{"selector":{"matchLabels":{"app":"web"}}}}
        ]}"#;
        let g = build_arch(pods, svcs, ings, &[("Deployment", deps)]);
        let id = |k: &str, n: &str| arch_id("app", k, n);
        let has = |a: String, b: String| g.edges.iter().any(|(x, y)| *x == a && *y == b);
        assert!(has(id("Ingress", "web-ing"), id("Service", "web")), "ingress→service");
        assert!(has(id("Service", "web"), id("Deployment", "web")), "service→workload");
        assert!(has(id("Deployment", "web"), id("Pod", "web-1")), "workload→pod1");
        assert!(has(id("Deployment", "web"), id("Pod", "web-2")), "workload→pod2");
        // Service→Pod の直結は張らない（Workload があるので）。
        assert!(!has(id("Service", "web"), id("Pod", "web-1")), "service→pod は張らない");
        assert_eq!(g.nodes.len(), 5);
    }

    /// selector_matches: 部分集合一致、空 selector は不一致。
    #[test]
    fn selector_subset() {
        let m = |s: &str| serde_json::from_str::<serde_json::Map<String, Value>>(s).unwrap();
        assert!(selector_matches(&m(r#"{"app":"web"}"#), &m(r#"{"app":"web","x":"y"}"#)));
        assert!(!selector_matches(&m(r#"{"app":"web"}"#), &m(r#"{"app":"db"}"#)));
        assert!(!selector_matches(&m("{}"), &m(r#"{"app":"web"}"#)), "空 selector は不一致");
    }

    #[test]
    fn cpu_parsing() {
        assert_eq!(parse_cpu_milli("250m"), 250.0);
        assert_eq!(parse_cpu_milli("1500m"), 1500.0);
        assert_eq!(parse_cpu_milli("2"), 2000.0);
        assert_eq!(parse_pct("42%"), 42.0);
    }

    #[test]
    fn mem_parsing() {
        assert_eq!(parse_mem_mib("512Mi"), 512.0);
        assert_eq!(parse_mem_mib("1Gi"), 1024.0);
        assert_eq!(parse_mem_mib("1048576Ki"), 1024.0);
    }

    #[test]
    fn node_line_parsing() {
        let s = "node-1   250m   12%   1024Mi   30%\nnode-2  500m 25% 2048Mi 60%";
        let nodes = parse_nodes(s);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "node-1");
        assert_eq!(nodes[0].cpu_pct, 12.0);
        assert_eq!(nodes[0].mem_pct, 30.0);
    }

    #[test]
    fn container_usage_parsing() {
        let s = "default  api-abc  api  230m  160Mi\ndefault  api-abc  sidecar  20m  20Mi";
        let u = parse_container_usage(s);
        assert_eq!(
            u.get(&("default".into(), "api-abc".into(), "api".into())),
            Some(&(230.0, 160.0))
        );
    }

    #[test]
    fn age_far_past_is_days() {
        assert!(age_from("2020-01-01T00:00:00Z").ends_with('d'));
        assert!(age_from("bogus").is_empty());
    }

    #[test]
    fn events_parsing_sorts_warnings_first() {
        let json = r#"{"items":[
          {"type":"Normal","reason":"Pulled","count":1,"lastTimestamp":"2020-01-01T00:00:00Z",
           "involvedObject":{"kind":"Pod","name":"web"},"message":"image present"},
          {"type":"Warning","reason":"BackOff","count":6,"lastTimestamp":"2020-01-01T00:00:00Z",
           "involvedObject":{"kind":"Pod","name":"job"},"message":"Back-off"}
        ]}"#;
        let ev = parse_events(json);
        assert_eq!(ev.len(), 2);
        assert!(ev[0].warning); // Warning が先頭
        assert_eq!(ev[0].reason, "BackOff");
        assert_eq!(ev[0].object, "Pod/job");
        assert_eq!(ev[0].count, 6);
    }

    #[test]
    fn pod_json_parsing() {
        let json = r#"{
          "items": [{
            "metadata": {"name": "api-abc", "namespace": "default",
                         "creationTimestamp": "2020-01-01T00:00:00Z"},
            "spec": {
              "nodeName": "node-1",
              "initContainers": [{"name": "init-migrate", "image": "migrate:1.2"}],
              "containers": [
                {"name": "api", "image": "myorg/api:1.8"},
                {"name": "sidecar", "image": "envoy:v1.30"}
              ]
            },
            "status": {
              "phase": "Running",
              "initContainerStatuses": [
                {"name": "init-migrate", "ready": true, "restartCount": 0,
                 "state": {"terminated": {"reason": "Completed"}}}
              ],
              "containerStatuses": [
                {"name": "api", "image": "myorg/api:1.8", "ready": true, "restartCount": 3,
                 "state": {"running": {}}},
                {"name": "sidecar", "ready": false, "restartCount": 0,
                 "state": {"waiting": {"reason": "CrashLoopBackOff"}}}
              ]
            }
          }]
        }"#;
        let mut usage = HashMap::new();
        usage.insert(
            (
                "default".to_string(),
                "api-abc".to_string(),
                "api".to_string(),
            ),
            (230.0, 160.0),
        );
        let pods = parse_pods(json, &usage);
        assert_eq!(pods.len(), 1);
        let p = &pods[0];
        assert_eq!(p.name, "api-abc");
        assert_eq!(p.phase, "Running");
        assert_eq!(p.node, "node-1");
        assert_eq!(p.restarts, 3); // 合算
        assert!(p.age.ends_with('d'));
        // init が先頭、通常コンテナが後
        assert_eq!(p.containers.len(), 3);
        assert!(p.containers[0].init);
        assert_eq!(p.containers[0].state, "Completed");
        let api = p.containers.iter().find(|c| c.name == "api").unwrap();
        assert_eq!(api.restarts, 3);
        assert_eq!(api.image, "myorg/api:1.8");
        assert_eq!(api.cpu_milli, 230.0);
        let side = p.containers.iter().find(|c| c.name == "sidecar").unwrap();
        assert_eq!(side.state, "CrashLoopBackOff");
        assert!(!side.ready);
    }
}
