use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use gcp_auth::TokenProvider;
use serde::Deserialize;

/// Cloud Monitoring の読み取りスコープ
const SCOPE: &str = "https://www.googleapis.com/auth/monitoring.read";

pub struct Config {
    pub mock: bool,
}

/// UI に渡す 1 サンプル
#[derive(Clone, Debug)]
pub struct Sample {
    pub t: f64,                // unix 秒
    pub cpu_percent: f64,      // 0..100
    pub storage_used: f64,     // bytes
    pub storage_limit: f64,    // bytes（0 のときは未取得）
    pub processing_units: f64, // インスタンス容量（PU。0 のときは未取得）
    pub error: Option<String>,
}

impl Sample {
    fn error_at(t: f64, msg: String) -> Self {
        Self {
            t,
            cpu_percent: f64::NAN,
            storage_used: f64::NAN,
            storage_limit: 0.0,
            processing_units: 0.0,
            error: Some(msg),
        }
    }
}

// ---- Monitoring timeSeries レスポンスの最小デシリアライズ ----

#[derive(Deserialize)]
struct TimeSeriesResponse {
    #[serde(default, rename = "timeSeries")]
    time_series: Vec<TimeSeries>,
}

#[derive(Deserialize)]
struct TimeSeries {
    #[serde(default)]
    points: Vec<Point>,
}

#[derive(Deserialize)]
struct Point {
    value: TypedValue,
}

#[derive(Deserialize)]
struct TypedValue {
    #[serde(rename = "doubleValue")]
    double_value: Option<f64>,
    // int64 は JSON 上は文字列で返る
    #[serde(rename = "int64Value")]
    int64_value: Option<String>,
}

impl TypedValue {
    fn as_f64(&self) -> f64 {
        if let Some(d) = self.double_value {
            d
        } else if let Some(s) = &self.int64_value {
            s.parse().unwrap_or(0.0)
        } else {
            0.0
        }
    }
}

/// ポーリングループ。UI 側の Receiver が落ちたら終了する。間隔は共有 Atomic から都度読む。
pub async fn poll_loop(
    cfg: Config,
    interval: std::sync::Arc<std::sync::atomic::AtomicU64>,
    tx: Sender<Sample>,
) {
    if cfg.mock {
        mock_loop(interval, tx).await;
        return;
    }

    // エミュレータには Cloud Monitoring が無く、認証情報も通常は無い。
    // 実 API に繋ぎにいくと「認証初期化に失敗」になるため、明示的に無効化する。
    if std::env::var("SPANNER_EMULATOR_HOST")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        let _ = tx.send(Sample::error_at(
            now_unix(),
            "エミュレータでは Cloud Monitoring は利用できません（CPU/ストレージ監視は無効）".to_string(),
        ));
        return;
    }

    let client = reqwest::Client::new();
    // 認証プロバイダはループ内で遅延生成し、認証失敗時は破棄して作り直す。
    // こうすることで「起動時は未ログイン → 後でログイン」や「セッション切れ →
    // 再ログイン」のあとに、アプリを再起動しなくても監視が自動復帰する。
    let mut provider: Option<Arc<dyn TokenProvider>> = None;

    loop {
        // 接続先は設定画面で切り替えられるので毎回読む
        let env = crate::query::current_spanner_env();
        let sample = if !env.configured() {
            Sample::error_at(now_unix(), "Spanner 環境が未設定です".to_string())
        } else {
            // 認証プロバイダが無ければ作る（再ログインを毎回ここで拾える）。
            if provider.is_none() {
                if let Ok(p) = gcp_auth::provider().await {
                    provider = Some(p);
                }
            }
            match &provider {
                None => {
                    // 初期化に失敗してもループは止めない（後でログインすれば復帰）。
                    Sample::error_at(
                        now_unix(),
                        "未認証です（gcloud auth application-default login などでログインしてください）"
                            .to_string(),
                    )
                }
                Some(p) => {
                    let (sample, auth_failed) =
                        poll_once(p, &client, &env.project, &env.instance).await;
                    if auth_failed {
                        // セッション切れ等。次回はプロバイダを作り直して再ログインを拾う。
                        provider = None;
                    }
                    sample
                }
            }
        };
        if tx.send(sample).is_err() {
            break; // UI が閉じられた
        }
        let secs = interval.load(std::sync::atomic::Ordering::Relaxed).max(1);
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }
}

/// 実 Spanner も認証も使わず、合成データを流すモード。
/// UI / グラフ描画の開発・デモ用。
async fn mock_loop(interval: std::sync::Arc<std::sync::atomic::AtomicU64>, tx: Sender<Sample>) {
    const LIMIT: f64 = 2.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0; // 2 TiB 上限を想定
    let mut tick: u64 = 0;

    loop {
        let phase = tick as f64;
        // CPU: 基準40% + ゆるやかな波 + tick由来の擬似ノイズ
        let wave = (phase * 0.15).sin() * 25.0 + (phase * 0.9).sin() * 6.0;
        let noise = pseudo_noise(tick) * 8.0;
        let cpu = (45.0 + wave + noise).clamp(0.0, 100.0);

        // ストレージ: ゆっくり増えていく（55% 付近で頭打ち）
        let used = LIMIT * (0.4 + 0.15 * (1.0 - (-(phase * 0.02)).exp()));

        let sample = Sample {
            t: now_unix(),
            cpu_percent: cpu,
            storage_used: used,
            storage_limit: LIMIT,
            processing_units: 1000.0,
            error: None,
        };
        if tx.send(sample).is_err() {
            break;
        }
        tick = tick.wrapping_add(1);
        let secs = interval.load(std::sync::atomic::Ordering::Relaxed).max(1);
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }
}

/// tick から -1.0..1.0 の決定的な擬似ノイズを作る（rand クレート不要）。
fn pseudo_noise(tick: u64) -> f64 {
    // 線形合同法的なハッシュで散らす
    let h = tick
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let frac = (h >> 11) as f64 / (1u64 << 53) as f64; // 0.0..1.0
    frac * 2.0 - 1.0
}

/// 1 回ポーリングする。戻り値 `(Sample, auth_failed)`。`auth_failed=true` のとき
/// 呼び出し側はプロバイダを破棄して作り直す（セッション切れ→再ログインの復帰用）。
async fn poll_once(
    provider: &Arc<dyn TokenProvider>,
    client: &reqwest::Client,
    project: &str,
    instance: &str,
) -> (Sample, bool) {
    let t = now_unix();
    // トークン取得失敗はセッション切れ等の認証問題なので作り直し対象。
    let token = match provider.token(&[SCOPE]).await {
        Ok(tok) => tok,
        Err(e) => {
            return (
                Sample::error_at(
                    t,
                    format!("認証に失敗: {e}（gcloud auth application-default login で再ログインしてください）"),
                ),
                true,
            );
        }
    };
    match try_poll(token.as_str(), client, project, instance).await {
        Ok((cpu, used, limit, pu)) => (
            Sample {
                t,
                cpu_percent: cpu * 100.0,
                storage_used: used,
                storage_limit: limit,
                processing_units: pu,
                error: None,
            },
            false,
        ),
        Err(e) => {
            // HTTP 401/403 等も認証起因として作り直し対象にする。
            let s = e.to_string();
            let auth = s.contains("401")
                || s.contains("403")
                || s.contains("UNAUTHENTICATED")
                || s.contains("PERMISSION_DENIED")
                || s.contains("invalid_grant");
            (Sample::error_at(t, s), auth)
        }
    }
}

async fn try_poll(
    bearer: &str,
    client: &reqwest::Client,
    project: &str,
    instance: &str,
) -> anyhow::Result<(f64, f64, f64, f64)> {
    let cpu = fetch_latest(
        client,
        bearer,
        project,
        "spanner.googleapis.com/instance/cpu/utilization",
        instance,
        "ALIGN_MEAN",
    )
    .await?;

    let used = fetch_latest(
        client,
        bearer,
        project,
        "spanner.googleapis.com/instance/storage/used_bytes",
        instance,
        "ALIGN_MEAN",
    )
    .await?;

    let limit = fetch_latest(
        client,
        bearer,
        project,
        "spanner.googleapis.com/instance/storage/limit_bytes",
        instance,
        "ALIGN_MEAN",
    )
    .await
    .unwrap_or(0.0);

    // インスタンス容量（処理ユニット）。取得失敗しても致命的でないので 0。
    let pu = fetch_latest(
        client,
        bearer,
        project,
        "spanner.googleapis.com/instance/processing_units",
        instance,
        "ALIGN_MEAN",
    )
    .await
    .unwrap_or(0.0);

    Ok((cpu, used, limit, pu))
}

/// 指定メトリクスの直近値を取得。
/// CPU は優先度(priority)別に複数系列で返るため、最新点を合算する。
async fn fetch_latest(
    client: &reqwest::Client,
    token: &str,
    project: &str,
    metric_type: &str,
    instance: &str,
    aligner: &str,
) -> anyhow::Result<f64> {
    let now = Utc::now();
    let start = now - chrono::Duration::minutes(10);
    let url = format!("https://monitoring.googleapis.com/v3/projects/{project}/timeSeries");
    let filter =
        format!("metric.type=\"{metric_type}\" AND resource.labels.instance_id=\"{instance}\"");

    let resp = client
        .get(&url)
        .bearer_auth(token)
        .query(&[
            ("filter", filter.as_str()),
            ("interval.startTime", &start.to_rfc3339()),
            ("interval.endTime", &now.to_rfc3339()),
            ("aggregation.alignmentPeriod", "60s"),
            ("aggregation.perSeriesAligner", aligner),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<TimeSeriesResponse>()
        .await?;

    // points は新しい順。各系列の最新点を合算する。
    let mut total = 0.0;
    for ts in &resp.time_series {
        if let Some(p) = ts.points.first() {
            total += p.value.as_f64();
        }
    }
    Ok(total)
}

fn now_unix() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}
