use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use eframe::egui;
use egui_plot::{Legend, Line, Plot, PlotPoints};
use tokio::sync::mpsc::UnboundedSender;

use crate::k8s;
use crate::monitoring::Sample;
use crate::query::{EdgeKind, QueryOutcome, SchemaGraph, TableNode, Target};

// ── カラーパレット（モダンダーク） ──
const ACCENT: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const CPU_COLOR: egui::Color32 = egui::Color32::from_rgb(251, 146, 60); // amber/orange
const STORAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const TEXT: egui::Color32 = egui::Color32::from_rgb(226, 232, 240); // 明るいテキスト
const MUTED: egui::Color32 = egui::Color32::from_rgb(148, 163, 184); // 補助テキスト

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
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum KubeView {
    Monitor,
    Diagram,
    Events,
}

/// 背景スレッドとの全チャネル + 設定。
pub struct Channels {
    pub sample_rx: Receiver<Sample>,
    pub req_tx: UnboundedSender<(Target, String)>,
    pub res_rx: Receiver<QueryOutcome>,
    pub schema_rx: Receiver<SchemaGraph>,
    pub kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    pub kube_topo_req_tx: UnboundedSender<()>,
    pub kube_topo_rx: Receiver<SchemaGraph>,
    pub kube_log_req_tx: UnboundedSender<k8s::LogReq>,
    pub kube_log_rx: Receiver<k8s::LogResult>,
    pub kube_ev_req_tx: UnboundedSender<()>,
    pub kube_ev_rx: Receiver<k8s::EventsResult>,
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
    sql: String,
    data_result: Option<QueryOutcome>,
    data_pending: bool,
    schema_graph: Option<SchemaGraph>,
    schema_pending: bool,

    // スキーマ図のパン/ズーム・編集状態
    diagram_pan: egui::Vec2,
    diagram_zoom: f32,
    node_positions: HashMap<String, egui::Pos2>,
    selected: Option<String>,
    copy_note: Option<String>,

    // Kubernetes
    kube_metrics_rx: Receiver<k8s::KubeMetrics>,
    kube_metrics: Option<k8s::KubeMetrics>,
    kube_req_tx: UnboundedSender<()>,
    kube_graph_rx: Receiver<SchemaGraph>,
    kube_graph: Option<SchemaGraph>,
    kube_pending: bool,
    kube_positions: HashMap<String, egui::Pos2>,
    kube_selected: Option<String>,
    kube_pan: egui::Vec2,
    kube_zoom: f32,
    kube_view: KubeView,

    // k8s ログ
    kube_log_req_tx: UnboundedSender<k8s::LogReq>,
    kube_log_rx: Receiver<k8s::LogResult>,
    kube_log: Option<k8s::LogResult>,
    kube_log_open: bool,
    kube_log_pending: bool,

    // k8s イベント
    kube_ev_req_tx: UnboundedSender<()>,
    kube_ev_rx: Receiver<k8s::EventsResult>,
    kube_events: Option<k8s::EventsResult>,
    kube_ev_pending: bool,

    // 設定
    poll_interval: std::sync::Arc<std::sync::atomic::AtomicU64>,
    conn_info: String,
    settings_open: bool,

    section: Section,
    view: View,
}

impl MonitorApp {
    pub fn new(ch: Channels, cc: &eframe::CreationContext<'_>) -> Self {
        install_japanese_font(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);
        Self {
            sample_rx: ch.sample_rx,
            samples: VecDeque::new(),
            last_error: None,
            max_points: 480,
            req_tx: ch.req_tx,
            res_rx: ch.res_rx,
            schema_rx: ch.schema_rx,
            sql: "SELECT * FROM LoadTest LIMIT 100".to_string(),
            data_result: None,
            data_pending: false,
            schema_graph: None,
            schema_pending: false,
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
            kube_positions: HashMap::new(),
            kube_selected: None,
            kube_pan: egui::vec2(40.0, 40.0),
            kube_zoom: 1.0,
            kube_view: KubeView::Monitor,
            kube_log_req_tx: ch.kube_log_req_tx,
            kube_log_rx: ch.kube_log_rx,
            kube_log: None,
            kube_log_open: false,
            kube_log_pending: false,
            kube_ev_req_tx: ch.kube_ev_req_tx,
            kube_ev_rx: ch.kube_ev_rx,
            kube_events: None,
            kube_ev_pending: false,
            poll_interval: ch.poll_interval,
            conn_info: ch.conn_info,
            settings_open: false,
            section: Section::Spanner,
            view: View::Monitor,
        }
    }

    /// バックグラウンドスレッドから届いたデータを取り込む
    fn drain(&mut self) {
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
        while let Ok(m) = self.kube_metrics_rx.try_recv() {
            self.kube_metrics = Some(m);
        }
        while let Ok(g) = self.kube_graph_rx.try_recv() {
            self.kube_pending = false;
            self.kube_graph = Some(g);
        }
        while let Ok(l) = self.kube_log_rx.try_recv() {
            self.kube_log_pending = false;
            self.kube_log = Some(l);
        }
        while let Ok(e) = self.kube_ev_rx.try_recv() {
            self.kube_ev_pending = false;
            self.kube_events = Some(e);
        }
    }

    fn run_kube_topo(&mut self) {
        if self.kube_req_tx.send(()).is_ok() {
            self.kube_pending = true;
        }
    }

    fn run_kube_events(&mut self) {
        if self.kube_ev_req_tx.send(()).is_ok() {
            self.kube_ev_pending = true;
        }
    }

    fn open_logs(&mut self, ns: &str, pod: &str, container: &str) {
        let req = k8s::LogReq {
            ns: ns.to_string(),
            pod: pod.to_string(),
            container: container.to_string(),
        };
        if self.kube_log_req_tx.send(req).is_ok() {
            self.kube_log_open = true;
            self.kube_log_pending = true;
            self.kube_log = None;
        }
    }

    fn latest_ok(&self) -> Option<&Sample> {
        self.samples.iter().rev().find(|s| s.error.is_none())
    }

    fn run_query(&mut self) {
        let sql = self.sql.trim().to_string();
        if sql.is_empty() {
            return;
        }
        if self.req_tx.send((Target::Data, sql)).is_ok() {
            self.data_pending = true;
        }
    }

    fn run_schema(&mut self) {
        if self.req_tx.send((Target::Schema, String::new())).is_ok() {
            self.schema_pending = true;
        }
    }
}

impl eframe::App for MonitorApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain();
        ctx.request_repaint_after(Duration::from_secs(1));

        // VS Code 風の左アクティビティバー
        egui::SidePanel::left("activity")
            .exact_width(54.0)
            .resizable(false)
            .frame(egui::Frame::none().fill(egui::Color32::from_rgb(17, 19, 23)))
            .show(ctx, |ui| {
                self.activity_bar(ui);
            });

        // ビュー切替タブ（セクションごとに内容が変わる）
        egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
            ui.add_space(5.0);
            ui.horizontal(|ui| match self.section {
                Section::Spanner => {
                    ui.selectable_value(&mut self.view, View::Monitor, "  監視  ");
                    ui.selectable_value(&mut self.view, View::Data, "  データ  ");
                    ui.selectable_value(&mut self.view, View::Schema, "  スキーマ  ");
                }
                Section::Kube => {
                    ui.selectable_value(&mut self.kube_view, KubeView::Monitor, "  監視  ");
                    ui.selectable_value(&mut self.kube_view, KubeView::Diagram, "  図  ");
                    ui.selectable_value(&mut self.kube_view, KubeView::Events, "  イベント  ");
                }
            });
            ui.add_space(5.0);
        });

        // 図・イベントは初回表示時に自動取得
        if self.section == Section::Spanner
            && self.view == View::Schema
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

        match self.section {
            Section::Spanner => match self.view {
                View::Schema => self.schema_view(ctx),
                View::Monitor => self.monitor_view(ctx),
                View::Data => self.data_view(ctx),
            },
            Section::Kube => match self.kube_view {
                KubeView::Monitor => self.kube_monitor_view(ctx),
                KubeView::Diagram => self.kube_diagram_view(ctx),
                KubeView::Events => self.kube_events_view(ctx),
            },
        }

        self.settings_window(ctx);
        self.logs_window(ctx);
    }
}

impl MonitorApp {
    /// 左アクティビティバー: セクション切替（Spanner / Kubernetes）。
    fn activity_bar(&mut self, ui: &mut egui::Ui) {
        ui.add_space(12.0);
        if activity_item(ui, self.section == Section::Spanner, draw_db_icon, "Spanner") {
            self.section = Section::Spanner;
        }
        if activity_item(ui, self.section == Section::Kube, draw_k8s_icon, "Kubernetes") {
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
                } else {
                    ui.label(egui::RichText::new("データ取得待ち…").color(MUTED));
                }
            });
            if let Some(e) = &self.last_error {
                ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
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
                egui::RichText::new("CPU 使用率 (%) — 横軸: 計測開始からの経過 (分)")
                    .color(MUTED),
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

            ui.label(
                egui::RichText::new("ストレージ使用率 (%) — 横軸: 経過 (分)").color(MUTED),
            );
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
        egui::TopBottomPanel::top("query_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.label(egui::RichText::new("SQL").color(MUTED).small());
            ui.add(
                egui::TextEdit::multiline(&mut self.sql)
                    .desired_rows(3)
                    .desired_width(f32::INFINITY)
                    .code_editor(),
            );
            ui.horizontal(|ui| {
                let run = ui
                    .add_enabled(!self.data_pending, egui::Button::new("実行"))
                    .clicked();
                // Ctrl+Enter でも実行
                let shortcut = ui.input(|i| i.modifiers.command && i.key_pressed(egui::Key::Enter));
                if run || (shortcut && !self.data_pending) {
                    self.run_query();
                }
                result_status(ui, self.data_pending, self.data_result.as_ref());
            });
            ui.add_space(4.0);
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(result) = &self.data_result else {
                centered_hint(ui, "SQL を入力して「実行」を押してください");
                return;
            };
            if result.error.is_some() {
                return;
            }
            if result.columns.is_empty() {
                ui.label(egui::RichText::new("結果なし").color(MUTED));
                return;
            }
            result_grid(ui, "data_grid", result);
        });
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
        egui::CentralPanel::default().show(ctx, |ui| {
            let Some(m) = &self.kube_metrics else {
                centered_hint(ui, "kubectl から取得中…");
                return;
            };
            if m.error.is_some() {
                centered_hint(ui, "クラスタに接続できません（kubectl とクラスタ接続を確認）");
                return;
            }
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.add_space(4.0);
                // クラスタ全体のサマリ（チップ）
                ui.horizontal(|ui| {
                    chip(ui, "ノード", &m.nodes.len().to_string(), ACCENT);
                    chip(ui, "Pod", &m.pod_count.to_string(), ACCENT);
                    chip(
                        ui,
                        "コンテナ",
                        &(m.container_count + m.init_count).to_string(),
                        CPU_COLOR,
                    );
                    chip(ui, "うちinit", &m.init_count.to_string(), MUTED);
                    chip(ui, "Running", &m.running_count.to_string(), STORAGE_COLOR);
                });
                ui.add_space(8.0);

                // namespace 別の集計
                if !m.namespaces.is_empty() {
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
                for p in &m.pods {
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
                        ui.with_layout(
                            egui::Layout::right_to_left(egui::Align::Center),
                            |ui| {
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
                            },
                        );
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
                        ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
                    } else {
                        ui.label(egui::RichText::new(format!("{} 件", r.events.len())).color(MUTED));
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

    fn settings_window(&mut self, ctx: &egui::Context) {
        if !self.settings_open {
            return;
        }
        let mut open = self.settings_open;
        egui::Window::new("設定")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("接続").color(MUTED).small());
                ui.label(&self.conn_info);
                ui.separator();
                ui.label(egui::RichText::new("ポーリング間隔（秒）").color(MUTED).small());
                let mut secs = self.poll_interval.load(std::sync::atomic::Ordering::Relaxed);
                if ui.add(egui::Slider::new(&mut secs, 1..=120)).changed() {
                    self.poll_interval
                        .store(secs, std::sync::atomic::Ordering::Relaxed);
                }
                ui.label(
                    egui::RichText::new("監視・k8s メトリクスの取得間隔。\nCloud Monitoring は最小約60秒。")
                        .color(MUTED)
                        .small(),
                );
            });
        self.settings_open = open;
    }

    fn logs_window(&mut self, ctx: &egui::Context) {
        if !self.kube_log_open {
            return;
        }
        let mut open = self.kube_log_open;
        let title = self
            .kube_log
            .as_ref()
            .map(|l| l.title.clone())
            .unwrap_or_else(|| "ログ".into());
        egui::Window::new(format!("ログ · {title}"))
            .open(&mut open)
            .default_size([660.0, 420.0])
            .resizable(true)
            .show(ctx, |ui| {
                if self.kube_log_pending {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(egui::RichText::new("取得中…").color(MUTED));
                    });
                }
                if let Some(l) = &self.kube_log {
                    let mut text = l.text.clone(); // 編集は破棄（選択/コピー用）
                    egui::ScrollArea::both()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            ui.add_sized(
                                ui.available_size(),
                                egui::TextEdit::multiline(&mut text)
                                    .code_editor()
                                    .desired_width(f32::INFINITY),
                            );
                        });
                }
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
                                "{} オブジェクト / {} 関係",
                                g.nodes.len(),
                                g.edges.len()
                            ))
                            .color(MUTED),
                        );
                    }
                }
                ui.separator();
                legend(ui, CPU_COLOR, "ノード配置");
                legend(ui, ACCENT, "オーナー");
                if let Some(note) = &self.copy_note {
                    ui.label(egui::RichText::new(note).color(ACCENT).small());
                }
            });
            ui.add_space(6.0);
        });

        let Self {
            kube_graph,
            kube_positions,
            kube_selected,
            kube_pan,
            kube_zoom,
            copy_note,
            ..
        } = self;
        let g = kube_graph.as_ref();
        egui::CentralPanel::default().show(ctx, |ui| {
            Self::draw_graph(ui, g, kube_positions, kube_selected, kube_pan, kube_zoom, copy_note);
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
            ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
            return;
        }
        if graph.nodes.is_empty() {
            centered_hint(ui, "テーブルがありません");
            return;
        }

        let rect = ui.available_rect_before_wrap();
        // 背景（パン/ズーム/選択解除）。ノードより先に登録して下層に置く。
        let bg = ui.interact(rect, ui.id().with("schema_bg"), egui::Sense::click_and_drag());
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
        painter.rect_filled(rect, 0.0, egui::Color32::from_rgb(20, 22, 27));

        // ノード幅を内容（最長テキスト）に合わせて測定し、はみ出しを防ぐ
        let text_w = |text: &str, size: f32, mono: bool| -> f32 {
            let font = if mono {
                egui::FontId::monospace(size)
            } else {
                egui::FontId::proportional(size)
            };
            ui.fonts(|f| f.layout_no_wrap(text.to_owned(), font, egui::Color32::WHITE).size().x)
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
            (list.iter().position(|&x| x == i).unwrap_or(0), list.len().max(1))
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

            let base_color = match e.kind {
                EdgeKind::Interleave => ACCENT,
                EdgeKind::ForeignKey => CPU_COLOR,
            };
            let active = sel.as_deref().map_or(true, |s| e.from == s || e.to == s);
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
                    egui::FontId::proportional((10.0 * z).max(6.0)),
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

            let is_sel = sel.as_deref() == Some(node.name.as_str());
            let dimmed = related
                .as_ref()
                .map_or(false, |r| !r.contains(node.name.as_str()));
            let dim = |c: egui::Color32| if dimmed { c.gamma_multiply(0.35) } else { c };
            let fs = |s: f32| (s * z).max(6.0);
            // ノード内のテキストは枠外へはみ出さないようクリップ
            let pc = painter.with_clip_rect(screen.intersect(rect));

            // 背景 + 枠
            let rounding = egui::Rounding::same(7.0);
            painter.rect_filled(screen, rounding, dim(egui::Color32::from_rgb(33, 37, 45)));
            let border = if is_sel {
                egui::Stroke::new(2.0, ACCENT)
            } else {
                egui::Stroke::new(1.0, dim(egui::Color32::from_rgb(70, 76, 86)))
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
                if !idx_joined.is_empty() && ui.button("インデックス一覧をコピー").clicked() {
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
                let rid = ui.id().with(("col", node.name.as_str(), i));
                let label = format!("{}  {}", col.name, col.ty);
                let color = if col.pk { dim(PK_COLOR) } else { dim(TEXT) };
                if diagram_row(ui, &pc, rr, rid, &label, egui::FontId::monospace(fs(11.5)), color, z) {
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
                    egui::Stroke::new(1.0, dim(egui::Color32::from_rgb(70, 76, 86))),
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
                    let rid = ui.id().with(("idx", node.name.as_str(), i));
                    if diagram_row(ui, &pc, rr, rid, idx, egui::FontId::monospace(fs(11.0)), dim(ACCENT), z) {
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

fn phase_color(phase: &str) -> egui::Color32 {
    match phase {
        "Running" => egui::Color32::from_rgb(34, 197, 94),   // 緑
        "Pending" => egui::Color32::from_rgb(251, 191, 36),  // 黄
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
        p.rect_filled(rect, 3.0, egui::Color32::from_rgb(40, 44, 52));
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
const LAYOUT_FILE: &str = "schema_layout.json";

fn save_layout(positions: &HashMap<String, egui::Pos2>) -> std::io::Result<()> {
    let map: HashMap<&String, [f32; 2]> =
        positions.iter().map(|(k, p)| (k, [p.x, p.y])).collect();
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
fn layout_nodes(
    graph: &SchemaGraph,
    widths: &HashMap<String, f32>,
) -> HashMap<String, egui::Rect> {

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
    painter.add(egui::Shape::line(
        vec![from, p1, p2, to],
        stroke,
    ));

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
            ui.colored_label(egui::Color32::from_rgb(248, 113, 113), format!("エラー: {e}"));
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
fn result_grid(ui: &mut egui::Ui, id: &str, result: &QueryOutcome) {
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new(id)
            .striped(true)
            .spacing([18.0, 6.0])
            .show(ui, |ui| {
                for c in &result.columns {
                    let resp = ui
                        .add(
                            egui::Label::new(egui::RichText::new(c).color(ACCENT).strong())
                                .sense(egui::Sense::click()),
                        )
                        .on_hover_text("クリックでコピー");
                    if resp.clicked() {
                        ui.ctx().copy_text(c.clone());
                    }
                }
                ui.end_row();
                for row in &result.rows {
                    for cell in row {
                        let resp = ui
                            .add(egui::Label::new(cell).sense(egui::Sense::click()))
                            .on_hover_text("クリックでコピー");
                        if resp.clicked() {
                            ui.ctx().copy_text(cell.clone());
                        }
                    }
                    ui.end_row();
                }
            });
    });
}

/// 中央寄せの控えめなヒント表示。
fn centered_hint(ui: &mut egui::Ui, text: &str) {
    ui.add_space(20.0);
    ui.vertical_centered(|ui| {
        ui.label(egui::RichText::new(text).color(MUTED));
    });
}

/// モダンなダークテーマを適用する（配色・角丸・余白・フォントサイズ）。
fn setup_style(ctx: &egui::Context) {
    use egui::FontFamily::{Monospace, Proportional};
    use egui::{FontId, Rounding, Stroke, TextStyle};

    let mut v = egui::Visuals::dark();
    v.override_text_color = Some(TEXT);
    v.panel_fill = egui::Color32::from_rgb(24, 26, 31);
    v.window_fill = egui::Color32::from_rgb(30, 33, 39);
    v.faint_bg_color = egui::Color32::from_rgb(36, 40, 47); // 縞模様の行
    v.extreme_bg_color = egui::Color32::from_rgb(18, 20, 24); // 入力欄背景
    v.hyperlink_color = ACCENT;
    v.selection.bg_fill = egui::Color32::from_rgba_unmultiplied(56, 189, 248, 70);
    v.selection.stroke = Stroke::new(1.0, ACCENT);
    v.window_rounding = Rounding::same(10.0);
    v.window_shadow.color = egui::Color32::from_black_alpha(80);

    // ウィジェットを角丸に
    let r = Rounding::same(7.0);
    for w in [
        &mut v.widgets.noninteractive,
        &mut v.widgets.inactive,
        &mut v.widgets.hovered,
        &mut v.widgets.active,
        &mut v.widgets.open,
    ] {
        w.rounding = r;
    }
    // ホバー/アクティブ時にアクセントをほのめかす
    v.widgets.hovered.bg_stroke = Stroke::new(1.0, ACCENT.gamma_multiply(0.6));
    ctx.set_visuals(v);

    ctx.style_mut(|s| {
        s.spacing.item_spacing = egui::vec2(10.0, 8.0);
        s.spacing.button_padding = egui::vec2(12.0, 6.0);
        s.spacing.window_margin = egui::Margin::same(10.0);
        s.text_styles = [
            (TextStyle::Heading, FontId::new(22.0, Proportional)),
            (TextStyle::Body, FontId::new(15.0, Proportional)),
            (TextStyle::Button, FontId::new(15.0, Proportional)),
            (TextStyle::Monospace, FontId::new(14.0, Monospace)),
            (TextStyle::Small, FontId::new(12.0, Proportional)),
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
        fonts.families.entry(family).or_default().push("jp".to_owned());
    }
    ctx.set_fonts(fonts);
}

/// ステータスバー用の小さなカラーチップ（ラベル + 値）。
fn chip(ui: &mut egui::Ui, label: &str, value: &str, color: egui::Color32) {
    egui::Frame::none()
        .fill(egui::Color32::from_rgb(36, 40, 47))
        .rounding(egui::Rounding::same(7.0))
        .inner_margin(egui::Margin::symmetric(10.0, 4.0))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).color(MUTED).small());
            ui.label(egui::RichText::new(value).color(color).strong());
        });
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
