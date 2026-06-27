# Spanner Viewer

Cloud Spanner の CPU 使用率とストレージ使用率を Cloud Monitoring から定期取得し、
egui でリアルタイムにグラフ表示する最小ツール。

> 注: Spanner はメモリ使用量メトリクスを公開していないため、
> 「メモリ」の代わりにストレージ使用率 (`storage/used_bytes` / `limit_bytes`) を表示します。

## 必要なもの

- Rust (cargo)
- gcloud CLI（ADC 認証に使用）
- 対象 Spanner インスタンスへの `roles/monitoring.viewer` 権限

## 認証 (ADC)

```sh
gcloud auth application-default login
```

## 実行

```sh
export SPANNER_PROJECT=your-project-id
export SPANNER_INSTANCE=your-instance-id
export POLL_INTERVAL_SECS=30   # 省略時 30。Monitoring の最小粒度は約60秒
cargo run --release
```

## 配布 (.dmg のビルド) — macOS

署名なしの配布用ディスクイメージを作成します（標準の `hdiutil` のみ使用）。

```sh
scripts/build-dmg.sh
# => target/dist/Spanner Viewer <version>.dmg
```

`.app` バンドルにリリースビルドのバイナリとアイコンを同梱し、ドラッグ&ドロップ用に
`/Applications` へのリンクを並べた DMG を生成します。アイコン (`assets/AppIcon.icns`)
は `scripts/make-icon.py` で生成されます（リポジトリにコミット済みなので python3 が
無くても DMG は作れます）。

> 署名していないため、受け取った側は初回のみ Finder でアプリを右クリック →「開く」で
> Gatekeeper の警告を許可してください。

## モックモード（実 Spanner / 認証 不要）

UI・グラフの開発やデモ用。合成データ（CPU は波形＋擬似ノイズ、ストレージは漸増）を流します。
Spanner エミュレータは Cloud Monitoring メトリクスを返さないため、描画確認にはこちらが手軽です。

```sh
MONITOR_MOCK=1 cargo run        # 既定 2秒間隔
MONITOR_MOCK=1 POLL_INTERVAL_SECS=1 cargo run
```

## エミュレータで使う（無料・ローカル完結）

データビューアと loadgen は Cloud Spanner エミュレータで動きます（**監視タブは Monitoring 非対応のため動きません**）。
`SPANNER_EMULATOR_HOST` を設定すると認証はスキップされ、コード変更は不要です。

```sh
# 1) エミュレータ起動
docker run -d --name spanner-emu -p 9010:9010 -p 9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator

# 2) 接続先（値は任意でOK）
export SPANNER_EMULATOR_HOST=localhost:9010
export SPANNER_PROJECT=test-project
export SPANNER_INSTANCE=test-instance
export SPANNER_DATABASE=test-db

# 3) インスタンス/DB/テーブルを作成（冪等）
cargo run --bin setup

# 4) データ投入（任意）
LOAD_CONCURRENCY=4 LOAD_DURATION_SECS=5 cargo run --release --bin loadgen

# 5) ビューア起動 → データタブで SELECT * FROM LoadTest LIMIT 100
cargo run --release

# 後片付け
docker rm -f spanner-emu
```

`setup` は `SPANNER_INSTANCE_CONFIG`（既定 `emulator-config`）で instanceConfig 名を変更可能。
実 Spanner に対しても同じ `setup` が使えます（その場合は ADC 認証が走る）。

## 負荷生成 (loadgen) — CPU グラフを動かす

実 Spanner に並行 INSERT を流し、監視ツールの CPU 使用率を上げて挙動を確認するためのバイナリ。
**エミュレータ／モックでは Monitoring メトリクスが出ないので、実インスタンスに対して実行すること。**

### 1. 負荷用テーブルを作る

```sql
CREATE TABLE LoadTest (
  Id      STRING(36) NOT NULL,
  Payload STRING(MAX),
) PRIMARY KEY (Id);
```

（`Id` は UUID。主キーがランダムなのでホットスポットを避けつつ書き込み負荷をかけられる）

### 2. 実行

```sh
gcloud auth application-default login
export SPANNER_PROJECT=your-project
export SPANNER_INSTANCE=your-instance
export SPANNER_DATABASE=your-db
export LOAD_CONCURRENCY=32      # 負荷の主な調整つまみ。CPUが上がらなければ増やす
export LOAD_BATCH=100           # 1コミットあたりの行数
export LOAD_DURATION_SECS=180   # 省略時は Ctrl-C まで継続
cargo run --release --bin loadgen
```

2秒ごとに `rows/s` を表示。別ターミナルで監視ツール本体を**同じ実インスタンスに向けて**起動しておくと、
CPU グラフが上昇します（Monitoring の粒度のため**約1分遅れ**で反映）。

> 100 PU 程度の小構成なら `LOAD_CONCURRENCY` を上げていくと CPU が上がります。
> 上げすぎると `RESOURCE_EXHAUSTED` 等のエラーが出るので、`errors=` を見ながら調整してください。

## 取得メトリクス

| 表示 | メトリクス |
|---|---|
| CPU 使用率 | `spanner.googleapis.com/instance/cpu/utilization`（優先度別を合算） |
| ストレージ使用量 | `spanner.googleapis.com/instance/storage/used_bytes` |
| ストレージ上限 | `spanner.googleapis.com/instance/storage/limit_bytes` |

## 構成

- `src/monitoring.rs` — Cloud Monitoring REST API の定期ポーリング（バックグラウンド tokio スレッド）
- `src/app.rs` — egui / egui_plot による描画
- `src/main.rs` — 起動・チャネル配線

データは mpsc チャネルで UI スレッドへ送られ、`egui_plot` で時系列グラフ化されます。

## 既知の制約 / 今後

- リアルタイム粒度は Cloud Monitoring 由来で最小約 60 秒。秒単位ではありません。
- 設定は現状環境変数のみ。UI 内設定パネルは未実装。
- クエリ単位の負荷を見たい場合は `SPANNER_SYS.QUERY_STATS_*` を SQL で取得する別経路が必要。
