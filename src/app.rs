use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::time::Duration;

use eframe::egui;
use egui_plot::{Legend, Line, Plot, PlotPoints};
use tokio::sync::mpsc::UnboundedSender;

use crate::monitoring::Sample;
use crate::query::{EdgeKind, QueryOutcome, SchemaGraph, TableNode, Target};

// ── カラーパレット（モダンダーク） ──
const ACCENT: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const CPU_COLOR: egui::Color32 = egui::Color32::from_rgb(251, 146, 60); // amber/orange
const STORAGE_COLOR: egui::Color32 = egui::Color32::from_rgb(56, 189, 248); // sky
const TEXT: egui::Color32 = egui::Color32::from_rgb(226, 232, 240); // 明るいテキスト
const MUTED: egui::Color32 = egui::Color32::from_rgb(148, 163, 184); // 補助テキスト

#[derive(PartialEq, Eq, Clone, Copy)]
enum View {
    Monitor,
    Data,
    Schema,
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

    // スキーマ図のパン/ズーム
    diagram_pan: egui::Vec2,
    diagram_zoom: f32,

    view: View,
}

impl MonitorApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        sample_rx: Receiver<Sample>,
        req_tx: UnboundedSender<(Target, String)>,
        res_rx: Receiver<QueryOutcome>,
        schema_rx: Receiver<SchemaGraph>,
    ) -> Self {
        install_japanese_font(&cc.egui_ctx);
        setup_style(&cc.egui_ctx);
        Self {
            sample_rx,
            samples: VecDeque::new(),
            last_error: None,
            max_points: 480, // 例: 30秒間隔で約4時間分
            req_tx,
            res_rx,
            schema_rx,
            sql: "SELECT * FROM LoadTest LIMIT 100".to_string(),
            data_result: None,
            data_pending: false,
            schema_graph: None,
            schema_pending: false,
            diagram_pan: egui::vec2(40.0, 40.0),
            diagram_zoom: 1.0,
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

        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new("◆")
                        .color(ACCENT)
                        .size(20.0),
                );
                ui.heading(egui::RichText::new("Spanner Viewer").color(TEXT));
                ui.add_space(12.0);
                ui.selectable_value(&mut self.view, View::Monitor, "  監視  ");
                ui.selectable_value(&mut self.view, View::Data, "  データ  ");
                ui.selectable_value(&mut self.view, View::Schema, "  スキーマ  ");
            });
            ui.add_space(6.0);
        });

        // スキーマタブを初めて開いたら自動取得
        if self.view == View::Schema && self.schema_graph.is_none() && !self.schema_pending {
            self.run_schema();
        }

        match self.view {
            View::Schema => self.schema_view(ctx),
            View::Monitor => self.monitor_view(ctx),
            View::Data => self.data_view(ctx),
        }
    }
}

impl MonitorApp {
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
                .include_y(105.0)
                .set_margin_fraction(egui::vec2(0.02, 0.0)) // 縦余白0 → 0%より下を描かない
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
                .include_y(105.0)
                .set_margin_fraction(egui::vec2(0.02, 0.0)) // 縦余白0 → 0%より下を描かない
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

    fn schema_view(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("schema_bar").show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("スキーマ図").strong());
                if ui
                    .add_enabled(!self.schema_pending, egui::Button::new("更新"))
                    .clicked()
                {
                    self.run_schema();
                }
                if ui.button("表示リセット").clicked() {
                    self.diagram_pan = egui::vec2(40.0, 40.0);
                    self.diagram_zoom = 1.0;
                }
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
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        egui::RichText::new("ドラッグで移動 / スクロールで拡大縮小")
                            .color(MUTED)
                            .small(),
                    );
                    legend(ui, ACCENT, "インターリーブ");
                    legend(ui, CPU_COLOR, "外部キー");
                });
            });
            ui.add_space(6.0);
        });

        // self を分割借用して、グラフ(不変)とパン/ズーム(可変)を同時に渡す
        let Self {
            schema_graph,
            diagram_pan,
            diagram_zoom,
            ..
        } = self;
        let graph = schema_graph.as_ref();

        egui::CentralPanel::default().show(ctx, |ui| {
            draw_diagram(ui, graph, diagram_pan, diagram_zoom);
        });
    }
}

/// 凡例の色サンプル + ラベル。
fn legend(ui: &mut egui::Ui, color: egui::Color32, text: &str) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(14.0, 3.0), egui::Sense::hover());
    ui.painter().rect_filled(rect, 1.0, color);
    ui.label(egui::RichText::new(text).color(MUTED).small());
}

// ── スキーマダイアグラム描画 ──

const NODE_W: f32 = 220.0;
const HEADER_H: f32 = 30.0;
const ROW_H: f32 = 18.0;
const MAX_COLS: usize = 12;
const H_GAP: f32 = 56.0;
const V_GAP: f32 = 70.0;

fn draw_diagram(
    ui: &mut egui::Ui,
    graph: Option<&SchemaGraph>,
    pan: &mut egui::Vec2,
    zoom: &mut f32,
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
    let resp = ui.allocate_rect(rect, egui::Sense::click_and_drag());
    if resp.dragged() {
        *pan += resp.drag_delta();
    }
    if resp.hovered() {
        let scroll = ui.input(|i| i.raw_scroll_delta.y);
        if scroll != 0.0 {
            let factor = (1.0 + scroll * 0.0015).clamp(0.85, 1.18);
            *zoom = (*zoom * factor).clamp(0.3, 3.0);
        }
    }

    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, egui::Color32::from_rgb(20, 22, 27)); // キャンバス背景

    let layout = layout_nodes(graph);
    let origin = rect.min + *pan;
    let z = *zoom;
    let tf = |p: egui::Pos2| origin + (p.to_vec2() * z);
    let tr = |r: egui::Rect| egui::Rect::from_min_max(tf(r.min), tf(r.max));

    // エッジ（ノードの背面）
    for e in &graph.edges {
        if let (Some(a), Some(b)) = (layout.get(&e.from), layout.get(&e.to)) {
            let from = tf(egui::pos2(a.center().x, a.top())); // 子の上端
            let to = tf(egui::pos2(b.center().x, b.bottom())); // 親の下端
            let color = match e.kind {
                EdgeKind::Interleave => ACCENT,
                EdgeKind::ForeignKey => CPU_COLOR,
            };
            draw_arrow(&painter, from, to, color, z);
            if !e.label.is_empty() {
                let mid = from + (to - from) * 0.5;
                painter.text(
                    mid,
                    egui::Align2::CENTER_BOTTOM,
                    &e.label,
                    egui::FontId::proportional((10.0 * z).max(6.0)),
                    color,
                );
            }
        }
    }

    // ノード
    for node in &graph.nodes {
        if let Some(r) = layout.get(&node.name) {
            draw_node(&painter, tr(*r), node, z);
        }
    }
}

/// 依存の深さでレベル分けし、各ノードの矩形（ワールド座標）を返す。
fn layout_nodes(graph: &SchemaGraph) -> std::collections::HashMap<String, egui::Rect> {
    use std::collections::HashMap;

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
        let shown = n.columns.len().min(MAX_COLS);
        let extra = if n.columns.len() > MAX_COLS { 1 } else { 0 };
        HEADER_H + (shown + extra) as f32 * ROW_H + 8.0
    };

    let mut out = HashMap::new();
    let mut y = 0.0;
    for row in &by_level {
        let row_h = row.iter().map(|n| node_h(n)).fold(0.0_f32, f32::max);
        for (i, n) in row.iter().enumerate() {
            let x = i as f32 * (NODE_W + H_GAP);
            let h = node_h(n);
            out.insert(
                n.name.clone(),
                egui::Rect::from_min_size(egui::pos2(x, y), egui::vec2(NODE_W, h)),
            );
        }
        y += row_h + V_GAP;
    }
    out
}

fn draw_node(painter: &egui::Painter, rect: egui::Rect, node: &TableNode, z: f32) {
    let rounding = egui::Rounding::same(7.0);
    painter.rect_filled(rect, rounding, egui::Color32::from_rgb(33, 37, 45));
    painter.rect_stroke(
        rect,
        rounding,
        egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 76, 86)),
    );

    // ヘッダ
    let header = egui::Rect::from_min_max(
        rect.min,
        egui::pos2(rect.max.x, rect.min.y + HEADER_H * z),
    );
    painter.rect_filled(header, rounding, ACCENT.gamma_multiply(0.85));
    let fs = |s: f32| (s * z).max(6.0);
    painter.text(
        header.left_center() + egui::vec2(8.0 * z, 0.0),
        egui::Align2::LEFT_CENTER,
        &node.name,
        egui::FontId::proportional(fs(14.0)),
        egui::Color32::from_rgb(12, 18, 30),
    );

    // カラム
    let mut cy = rect.min.y + HEADER_H * z + 4.0 * z;
    for col in node.columns.iter().take(MAX_COLS) {
        painter.text(
            egui::pos2(rect.min.x + 8.0 * z, cy),
            egui::Align2::LEFT_TOP,
            col,
            egui::FontId::monospace(fs(11.5)),
            TEXT,
        );
        cy += ROW_H * z;
    }
    if node.columns.len() > MAX_COLS {
        painter.text(
            egui::pos2(rect.min.x + 8.0 * z, cy),
            egui::Align2::LEFT_TOP,
            format!("… 他 {} 列", node.columns.len() - MAX_COLS),
            egui::FontId::proportional(fs(11.0)),
            MUTED,
        );
    }
}

fn draw_arrow(painter: &egui::Painter, from: egui::Pos2, to: egui::Pos2, color: egui::Color32, z: f32) {
    let stroke = egui::Stroke::new((1.6 * z).max(1.0), color);
    painter.line_segment([from, to], stroke);

    // 矢じり（親側 `to` に向ける）
    let dir = (to - from).normalized();
    if !dir.is_finite() {
        return;
    }
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
                    ui.label(egui::RichText::new(c).color(ACCENT).strong());
                }
                ui.end_row();
                for row in &result.rows {
                    for cell in row {
                        ui.label(cell);
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
