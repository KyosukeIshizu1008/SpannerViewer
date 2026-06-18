//! Spanner にインスタンス / データベース / 負荷用テーブルを作成するセットアップ。
//! エミュレータ・実 Spanner の両対応（SPANNER_EMULATOR_HOST があればエミュレータ）。
//! 冪等: 既存ならスキップし、テーブルは CREATE TABLE IF NOT EXISTS で追加する。
//!
//! 実行例（エミュレータ）:
//!   docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
//!   export SPANNER_EMULATOR_HOST=localhost:9010
//!   export SPANNER_PROJECT=test-project SPANNER_INSTANCE=test-instance SPANNER_DATABASE=test-db
//!   cargo run --bin setup

use anyhow::Context;
use gcloud_spanner::admin::client::Client as AdminClient;
use gcloud_spanner::admin::AdminClientConfig;
use google_cloud_gax::grpc::Code;
use google_cloud_googleapis::spanner::admin::database::v1::{
    CreateDatabaseRequest, UpdateDatabaseDdlRequest,
};
use google_cloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let project = env_required("SPANNER_PROJECT")?;
    let instance = env_required("SPANNER_INSTANCE")?;
    let database = env_required("SPANNER_DATABASE")?;
    let table = env_or("LOAD_TABLE", "LoadTest");
    let config_name = env_or("SPANNER_INSTANCE_CONFIG", "emulator-config");

    match std::env::var("SPANNER_EMULATOR_HOST") {
        Ok(host) => println!("エミュレータに接続: {host}"),
        Err(_) => println!("実 Spanner に接続します（ADC 認証）"),
    }

    // with_auth() は SPANNER_EMULATOR_HOST 設定時は no-op（認証スキップ）
    let config = AdminClientConfig::default()
        .with_auth()
        .await
        .context("認証に失敗")?;
    let client = AdminClient::new(config).await.context("admin 接続に失敗")?;

    let parent = format!("projects/{project}");
    let instance_path = format!("{parent}/instances/{instance}");
    let db_path = format!("{instance_path}/databases/{database}");

    // 1) インスタンス
    let req = CreateInstanceRequest {
        parent: parent.clone(),
        instance_id: instance.clone(),
        instance: Some(Instance {
            name: instance_path.clone(),
            config: format!("{parent}/instanceConfigs/{config_name}"),
            display_name: instance.clone(),
            node_count: 1,
            ..Default::default()
        }),
    };
    match client.instance().create_instance(req, None).await {
        Ok(mut op) => {
            op.wait(None).await.context("インスタンス作成待ち")?;
            println!("✓ インスタンス作成: {instance}");
        }
        Err(e) if e.code() == Code::AlreadyExists => {
            println!("• インスタンスは既存: {instance}");
        }
        Err(e) => return Err(anyhow::Error::new(e).context("create_instance 失敗")),
    }

    // 2) データベース
    let req = CreateDatabaseRequest {
        parent: instance_path.clone(),
        create_statement: format!("CREATE DATABASE `{database}`"),
        ..Default::default()
    };
    match client.database().create_database(req, None).await {
        Ok(mut op) => {
            op.wait(None).await.context("データベース作成待ち")?;
            println!("✓ データベース作成: {database}");
        }
        Err(e) if e.code() == Code::AlreadyExists => {
            println!("• データベースは既存: {database}");
        }
        Err(e) => return Err(anyhow::Error::new(e).context("create_database 失敗")),
    }

    // 3) テーブル（IF NOT EXISTS で冪等）
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {table} (Id STRING(36) NOT NULL, Payload STRING(MAX)) PRIMARY KEY (Id)"
    );
    let req = UpdateDatabaseDdlRequest {
        database: db_path.clone(),
        statements: vec![ddl],
        ..Default::default()
    };
    let mut op = client
        .database()
        .update_database_ddl(req, None)
        .await
        .context("update_database_ddl 失敗")?;
    op.wait(None).await.context("テーブル作成待ち")?;
    println!("✓ テーブル準備完了: {table}");

    println!("\n完了。database = {db_path}");
    Ok(())
}

fn env_required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).with_context(|| format!("環境変数 {key} が未設定です"))
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
