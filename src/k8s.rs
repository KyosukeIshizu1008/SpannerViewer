//! Kubernetes データ取得（kubectl 経由）。
//! - 監視: Docker Desktop 風のコンテナ一覧（Pod→コンテナ）+ ノード/namespace 集計
//! - 図: クラスタ構成（Pod→ノード / Pod→オーナー）
//!
//! kubectl 不在・クラスタ未接続でも mock で動作する。

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::query::{Column, Edge, EdgeKind, SchemaGraph, TableNode};

pub struct Config {
    pub mock: bool,
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
    pub pod_count: usize,
    pub container_count: usize, // 通常コンテナ
    pub init_count: usize,      // initContainers
    pub running_count: usize,
    pub namespaces: Vec<NsAgg>,
    pub error: Option<String>,
}

/// ログ取得リクエスト
#[derive(Clone, Debug)]
pub struct LogReq {
    pub ns: String,
    pub pod: String,
    pub container: String,
}

#[derive(Clone, Debug, Default)]
pub struct LogResult {
    pub title: String,
    pub text: String,
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

/// ログ取得ループ。
pub async fn logs_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<LogReq>,
    tx: std::sync::mpsc::Sender<LogResult>,
) {
    while let Some(r) = req_rx.recv().await {
        let title = format!("{}/{} · {}", r.ns, r.pod, r.container);
        let text = if cfg.mock {
            mock_logs(&r)
        } else {
            match run(&[
                "logs",
                "-n",
                r.ns.as_str(),
                r.pod.as_str(),
                "-c",
                r.container.as_str(),
                "--tail=300",
            ])
            .await
            {
                Ok(o) if o.trim().is_empty() => "(ログ出力なし)".into(),
                Ok(o) => o,
                Err(e) => format!("ログ取得失敗: {e}"),
            }
        };
        if tx.send(LogResult { title, text }).is_err() {
            break;
        }
    }
}

/// イベント取得ループ。
pub async fn events_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<()>,
    tx: std::sync::mpsc::Sender<EventsResult>,
) {
    while req_rx.recv().await.is_some() {
        let r = if cfg.mock {
            EventsResult {
                events: mock_events(),
                error: None,
            }
        } else {
            fetch_events().await
        };
        if tx.send(r).is_err() {
            break;
        }
    }
}

/// 構成図ループ。
pub async fn topology_loop(
    cfg: Config,
    mut req_rx: UnboundedReceiver<()>,
    tx: std::sync::mpsc::Sender<SchemaGraph>,
) {
    while req_rx.recv().await.is_some() {
        let g = if cfg.mock {
            mock_topology()
        } else {
            fetch_topology().await
        };
        if tx.send(g).is_err() {
            break;
        }
    }
}

async fn run(args: &[&str]) -> Result<String, String> {
    let out = tokio::process::Command::new("kubectl")
        .args(args)
        .output()
        .await
        .map_err(|e| format!("kubectl 実行失敗: {e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let line = err.lines().last().unwrap_or("").trim();
        return Err(if line.is_empty() {
            "kubectl エラー（クラスタに接続できません）".into()
        } else {
            line.to_string()
        });
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

// ── 監視 ──

async fn fetch_metrics() -> KubeMetrics {
    let mut nodes = match run(&["top", "nodes", "--no-headers"]).await {
        Ok(o) => parse_nodes(&o),
        Err(e) => {
            return KubeMetrics {
                error: Some(e),
                ..Default::default()
            }
        }
    };

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

    // 集計
    let mut pod_count = 0;
    let mut container_count = 0;
    let mut init_count = 0;
    let mut running_count = 0;
    let mut per_node: HashMap<String, (usize, usize)> = HashMap::new();
    let mut per_ns: HashMap<String, (usize, usize)> = HashMap::new();
    for p in &pods {
        pod_count += 1;
        let regular = p.containers.iter().filter(|c| !c.init).count();
        let inits = p.containers.len() - regular;
        container_count += regular;
        init_count += inits;
        if p.phase == "Running" {
            running_count += 1;
        }
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
        .map(|(name, (pods, containers))| NsAgg { name, pods, containers })
        .collect();
    namespaces.sort_by(|a, b| b.containers.cmp(&a.containers).then(a.name.cmp(&b.name)));

    KubeMetrics {
        nodes,
        pods,
        pod_count,
        container_count,
        init_count,
        running_count,
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

fn parse_pods(
    pj: &str,
    cusage: &HashMap<(String, String, String), (f64, f64)>,
) -> Vec<PodInfo> {
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

async fn fetch_topology() -> SchemaGraph {
    let nodes_json = match run(&["get", "nodes", "-o", "json"]).await {
        Ok(o) => o,
        Err(e) => return err_graph(e),
    };
    let pods_json = match run(&["get", "pods", "-A", "-o", "json"]).await {
        Ok(o) => o,
        Err(e) => return err_graph(e),
    };
    build_topology(&nodes_json, &pods_json)
}

// ── イベント ──

async fn fetch_events() -> EventsResult {
    match run(&["get", "events", "-A", "-o", "json"]).await {
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
    events.sort_by(|a, b| b.warning.cmp(&a.warning));
    events
}

fn mock_logs(r: &LogReq) -> String {
    format!(
        "2026-06-19T10:00:01Z INFO  starting {} in {}/{}\n\
         2026-06-19T10:00:02Z INFO  config loaded\n\
         2026-06-19T10:00:03Z INFO  listening on :8080\n\
         2026-06-19T10:00:05Z WARN  slow downstream (mock)\n\
         2026-06-19T10:00:06Z INFO  request handled status=200",
        r.container, r.ns, r.pod
    )
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

fn err_graph(e: String) -> SchemaGraph {
    SchemaGraph {
        error: Some(e),
        ..Default::default()
    }
}

fn line_col(text: &str) -> Column {
    Column {
        name: text.to_string(),
        ty: String::new(),
        pk: false,
    }
}

fn build_topology(nodes_json: &str, pods_json: &str) -> SchemaGraph {
    let mut nodes: Vec<TableNode> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut add_node = |nodes: &mut Vec<TableNode>, key: &str, cols: Vec<Column>| {
        if seen.insert(key.to_string()) {
            nodes.push(TableNode {
                name: key.to_string(),
                columns: cols,
                indexes: Vec::new(),
            });
        }
    };

    if let Ok(v) = serde_json::from_str::<Value>(nodes_json) {
        for item in v["items"].as_array().into_iter().flatten() {
            let name = item["metadata"]["name"].as_str().unwrap_or("?");
            add_node(
                &mut nodes,
                &format!("Node/{name}"),
                vec![line_col("Kubernetes Node")],
            );
        }
    }

    if let Ok(v) = serde_json::from_str::<Value>(pods_json) {
        for item in v["items"].as_array().into_iter().flatten() {
            let name = item["metadata"]["name"].as_str().unwrap_or("?");
            let ns = item["metadata"]["namespace"].as_str().unwrap_or("default");
            let phase = item["status"]["phase"].as_str().unwrap_or("");
            let node_name = item["spec"]["nodeName"].as_str();
            let owner = item["metadata"]["ownerReferences"]
                .as_array()
                .and_then(|a| a.first())
                .map(|o| {
                    (
                        o["kind"].as_str().unwrap_or("").to_string(),
                        o["name"].as_str().unwrap_or("").to_string(),
                    )
                });

            let pod_key = format!("{ns}/{name}");
            let mut cols = vec![line_col(&format!("Pod · {phase}"))];
            for c in item["spec"]["containers"].as_array().into_iter().flatten() {
                if let Some(cn) = c["name"].as_str() {
                    cols.push(line_col(&format!("- {cn}")));
                }
            }
            for c in item["spec"]["initContainers"].as_array().into_iter().flatten() {
                if let Some(cn) = c["name"].as_str() {
                    cols.push(line_col(&format!("- {cn} (init)")));
                }
            }
            if let Some((k, n)) = &owner {
                cols.push(line_col(&format!("by {k}/{n}")));
            }
            add_node(&mut nodes, &pod_key, cols);

            if let Some(nn) = node_name {
                let nk = format!("Node/{nn}");
                add_node(&mut nodes, &nk, vec![line_col("Kubernetes Node")]);
                edges.push(Edge {
                    from: pod_key.clone(),
                    to: nk,
                    kind: EdgeKind::ForeignKey,
                    label: "node".into(),
                });
            }
            if let Some((k, n)) = owner {
                if !n.is_empty() {
                    let ok = format!("{k}/{n}");
                    add_node(&mut nodes, &ok, vec![line_col(&k)]);
                    edges.push(Edge {
                        from: pod_key.clone(),
                        to: ok,
                        kind: EdgeKind::Interleave,
                        label: "owned".into(),
                    });
                }
            }
        }
    }

    SchemaGraph {
        nodes,
        edges,
        error: None,
    }
}

// ── モック ──

fn mock_metrics() -> KubeMetrics {
    let ctr = |name: &str, image: &str, state: &str, restarts: i64, init: bool, cpu: f64, mem: f64| {
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
                ctr("init-migrate", "migrate:1.2", "Completed", 0, true, 0.0, 0.0),
                ctr("api", "myorg/api:1.8.0", "Running", 1, false, 230.0, 160.0),
                ctr("sidecar", "envoyproxy/envoy:v1.30", "Running", 0, false, 20.0, 20.0),
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
            containers: vec![ctr("prometheus", "prom/prometheus:v2.53", "Running", 2, false, 95.0, 512.0)],
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
            containers: vec![ctr("worker", "myorg/worker:0.3", "CrashLoopBackOff", 6, false, 0.0, 0.0)],
        },
    ];
    KubeMetrics {
        nodes: vec![
            NodeUsage { name: "node-1".into(), cpu_pct: 42.0, mem_pct: 55.0, pods: 8, containers: 11 },
            NodeUsage { name: "node-2".into(), cpu_pct: 18.0, mem_pct: 33.0, pods: 5, containers: 6 },
            NodeUsage { name: "node-3".into(), cpu_pct: 76.0, mem_pct: 61.0, pods: 12, containers: 18 },
        ],
        pod_count: pods.len(),
        container_count: 5,
        init_count: 1,
        running_count: 3,
        namespaces: vec![
            NsAgg { name: "default".into(), pods: 10, containers: 16 },
            NsAgg { name: "kube-system".into(), pods: 9, containers: 12 },
            NsAgg { name: "monitoring".into(), pods: 6, containers: 13 },
        ],
        pods,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ("default".to_string(), "api-abc".to_string(), "api".to_string()),
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

fn mock_topology() -> SchemaGraph {
    let n = |key: &str, lines: &[&str]| TableNode {
        name: key.into(),
        columns: lines.iter().map(|l| line_col(l)).collect(),
        indexes: Vec::new(),
    };
    let e = |from: &str, to: &str, kind: EdgeKind, label: &str| Edge {
        from: from.into(),
        to: to.into(),
        kind,
        label: label.into(),
    };
    SchemaGraph {
        nodes: vec![
            n("Node/node-1", &["Kubernetes Node"]),
            n("Node/node-2", &["Kubernetes Node"]),
            n("Deployment/api", &["Deployment"]),
            n("default/api-7c9-abc", &["Pod · Running", "- api", "- sidecar", "- init-migrate (init)"]),
            n("default/web-5d-xyz", &["Pod · Running", "- web"]),
            n("kube-system/coredns-xyz", &["Pod · Running", "- coredns"]),
        ],
        edges: vec![
            e("default/api-7c9-abc", "Node/node-1", EdgeKind::ForeignKey, "node"),
            e("default/web-5d-xyz", "Node/node-2", EdgeKind::ForeignKey, "node"),
            e("kube-system/coredns-xyz", "Node/node-1", EdgeKind::ForeignKey, "node"),
            e("default/api-7c9-abc", "Deployment/api", EdgeKind::Interleave, "owned"),
            e("default/web-5d-xyz", "Deployment/api", EdgeKind::Interleave, "owned"),
        ],
        error: None,
    }
}
