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

- `src/main.rs` — 起動・全チャネルの配線・背景 tokio ランタイム
- `src/app.rs` — egui / egui_plot による全 UI（監視・データ・スキーマ・インポート・照合・実行計画・CSV ビューア・k8s）
- `src/monitoring.rs` — Cloud Monitoring REST API の定期ポーリング
- `src/query.rs` — Spanner のクエリ/スキーマ/DDL・DML・実行計画・CSV インポート・突合・GCS の背景ワーカー
- `src/csvview.rs` — 巨大 CSV ビューア（mmap + 疎な行索引で 100GB 級を仮想化）
- `src/k8s.rs` — kubectl 経由の Kubernetes リソースブラウザ

UI（メインスレッド）と背景ワーカー（tokio）は mpsc / tokio チャネルで疎結合に通信します。
監視データは `egui_plot` で時系列グラフ化されます。

## CSV インポートのデータストリーム設計

巨大 CSV（〜100GB 級）を **全行をメモリに載せずに** Spanner へ高速投入するための、
ストリーミング＋並列 BatchWrite パイプラインです（`src/query.rs` の
`run_streaming_import`）。

### パイプライン全体像

```
ソース            プロデューサ              バウンドチャネル        ワーカー×N（セッション毎）
(File / GCS)  →  next_chunk(64KB)     →  (容量 = 並列数×2)   →  型変換 → BatchWrite(gRPC)
                 CsvStreamer で逐次パース    ←―― backpressure ――       ↑ 並列・各行=独立ミューテーション
```

- **ソース抽象 `ByteSource`**: ローカルファイル（`tokio::fs`、64KB 単位で読む）と
  GCS オブジェクト（`reqwest` のチャンクストリーム）を同じ `next_chunk()` で扱う。
  本文を一括バッファしない。
- **逐次 CSV パーサ `CsvStreamer`**: バイト単位の RFC 4180 風パーサ。チャンク境界を
  またいでも壊れない（状態を保持）。対応:
  - クォート内のカンマ・改行、エスケープ `""` → `"`、CRLF/LF 混在、空行スキップ
  - フィールド**先頭の** `"` のみ引用開始（途中の `"` は `5'10"` 等のデータとして保持）
  - 文字コードは UTF-8 / Shift-JIS(CP932)、先頭 BOM 除去（チャンク分割耐性あり）
  - 構造文字（`" , CR LF`）は ASCII なのでマルチバイトを壊さずバイト処理できる
- **バックプレッシャ**: プロデューサ→ワーカー間は容量 `並列数×2` の
  `tokio::sync::mpsc` 有界チャネル。ワーカーが詰まるとプロデューサの読み出しが止まる
  ので、**メモリ使用量は概ね「1 バッチ × 並列数」**に収まる（全行を溜めない）。
- **並列 BatchWrite**: セッションを並列数ぶん（`IMPORT_CONCURRENCY = 8`、エミュレータは 1）
  作成し、ワーカーごとに 1 セッションで生 gRPC `BatchWrite` を実行。1 行 = 1 つの独立した
  ミューテーショングループ（非原子）で、`BATCH_CELLS_PER_REQUEST = 20,000` セル
  （= `20000 / 列数` 行）を 1 リクエストに詰める。
- **型変換 `convert_cell`**: CSV 文字列を列の SPANNER_TYPE に合わせて型付け
  （INT64/FLOAT64/BOOL はパース、NUMERIC/TIMESTAMP/DATE/BYTES(base64)/JSON は文字列表現で受理）。
  空欄→NULL（`empty_as_null`）、`null_token` 一致→NULL。
- **書き込みモード**: 新規挿入（`Insert`）/ 上書き挿入（`InsertOrUpdate` = INSERT OR UPDATE）。

### 再開（チェックポイント）と冪等性

- バッチには **決定的な連番 index** を振る（同じファイル・列・`per_request` なら同じ index =
  同じ行集合）。コミットできたバッチごとに `index 終端バイト位置 累積行数` をチェックポイント
  ファイルへ追記し、毎回 flush。
- **バイトオフセット再開（シーク）**: 再開時は「先頭から連続してコミット済みのバッチ」の
  **終端バイト位置までシークして読み飛ばす**（ローカルは `seek`、GCS は Range リクエスト）。
  巨大ファイルを終盤で中断しても、手前を読み直さずに続きから書ける。連続していない
  飛び石コミット分は、シーク後に index で再送スキップする（冪等な上書き挿入なので安全）。
- **署名（signature）** でチェックポイントの同一性を担保する:
  `v1\ttable=…\tper=…\thdr=…\tnull=…\tsrc=…\tcols=name|type|idx,…`。
  `src` はローカルなら `file:パス:サイズ:mtime`、GCS なら `gcs:URI`＋世代/サイズの `srcid`。
  → **中身が変われば署名が変わり**、古いチェックポイントで新データを取りこぼすことを防ぐ。
- 再開時はコミット済みバッチをスキップし、書き込みは **冪等な上書き挿入に切替**（重複を無害化）。
- 一過性エラー（ABORTED 等）は指数バックオフでリトライ。再送も冪等なので重複しない。
- 不正行は `skip_bad_rows` でスキップしてリジェクト CSV に記録、または即中断。

### 進捗・並列ジョブ

- 進捗は「読み出し済みバイト / 全体バイト」で割合表示。総件数は
  `書込済 行数 × 全体バイト ÷ 読込済バイト` で推定（`X / 約Y 件`）。
- ジョブは **ジョブ id** で進捗/完了を UI 行に紐付ける。**別テーブル宛ては並列**
  （最大 `MAX_PARALLEL_IMPORTS = 3`）、**同一テーブル宛ては直列**（書込・チェックポイント
  競合を回避）。

## CSV ↔ DB 突合（照合）処理

「照合」タブは、インポートした CSV と Spanner テーブルが一致するかを **主キーで突合し、
各カラムの値まで比較**する（`src/query.rs` の `run_verify`）。

### アルゴリズム

1. **DB 全件読み込み**: 比較対象列を `SELECT` し、`主キー → 全カラム値` の `HashMap` を作る
   （複合キーは制御文字 `U+0001` で連結）。メモリ保護のため上限 `VERIFY_DB_CAP = 5,000,000` 行
   （超過時は「DB 打切」を通知）。
2. **CSV ストリーミング突合**: インポートと同じ `ByteSource` + `CsvStreamer` で CSV を逐次読み、
   各行について主キーで DB マップを引く:
   - キーが DB に無い → **CSVのみ**
   - キーが有る → 全カラム値を比較。すべて一致 → **一致**、1 つでも違えば → **値差異**
     （最初に異なった列を `列名: 'csv値' ≠ 'db値'` で記録）
3. CSV で突合できた DB キーを集合で記録し、**DBのみ** = DB 全キー − 突合済みキー。
4. CSV 内で主キーが重複した行数（**CSV内PK重複**）も数える。

### 値の正規化

- 空欄は `empty_as_null` で NULL 扱い、`null_token` 一致も NULL。DB の NULL は文字列
  `"NULL"` に揃えて比較するので、両者の NULL が一致する。
- 比較は文字列同士。INT64 等は両側とも 10 進文字列なので一致する。

### 出力と上限

- 結果: **一致 / 値差異 / CSVのみ / DBのみ** の件数 + サマリーカード。差分明細は
  種別フィルタ付きで一覧（サンプルは上限 `VERIFY_SAMPLE_CAP = 500` 件、超過は打切表示）。
- 完全一致なら「✓ 完全一致」。中断可能（部分結果を返す）。

### 既知の制約

- **型の表記差**は値差異として出る場合がある（例: FLOAT64 の `2.0` ↔ DB 表記 `2`、
  TIMESTAMP の書式差）。整数・文字列中心のデータでは正確に突合する。
- 主キー列がマッピングされていないと実行できない（突合キーに必要）。

## 既知の制約 / 今後

- リアルタイム粒度は Cloud Monitoring 由来で最小約 60 秒。秒単位ではありません。
- クエリ単位の負荷を見たい場合は `SPANNER_SYS.QUERY_STATS_*` を SQL で取得する別経路が必要。
- **実行計画（EXPLAIN）はエミュレータ未対応**。実 Spanner 接続時のみ取得できます。
- 突合の値比較は文字列ベースのため、型の表記差（FLOAT64 / TIMESTAMP 等）は値差異として出ることがあります。
