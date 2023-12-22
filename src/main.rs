#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

use amazon_qldb_driver::aws_sdk_qldbsession::Config;
use amazon_qldb_driver::QldbDriverBuilder;
use anyhow::Result;
use ion_rs::serde::from_ion;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio;

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct Penguin {
    id: usize,
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let aws_config = aws_config::load_from_env().await;

    let driver = QldbDriverBuilder::new()
        .ledger_name("kawasaki")
        .sdk_config(Config::new(&aws_config))
        .await?;

    // //create table
    driver
        .transact(|mut tx| async {
            let _ = tx.execute_statement("create table cago").await?;
            tx.commit(()).await
        })
        .await?;

    //insert values
    let penguins = vec![
        Penguin {
            id: 1,
            name: "crigo".into(),
        },
        Penguin {
            id: 2,
            name: "estriper".into(),
        },
        Penguin {
            id: 3,
            name: "Sanya".into(),
        },
    ]
    .iter()
    .map(|p| serde_json::to_string(p))
    .filter_map(Result::ok)
    .collect::<Vec<_>>()
    .join(", ");
    driver
        .transact(|mut tx| async {
            let _ = tx
                .execute_statement(format!("insert into cago <<{}>>", penguins).replace("\"", "'"))
                .await?;
            tx.commit(()).await
        })
        .await?;
    // delete value
    driver
        .transact(|mut tx| async {
            let _ = tx
                .execute_statement("delete from cago as c where c.id = 1")
                .await?;
            tx.commit(()).await
        })
        .await?;
    // select value
    let penguins = driver
        .transact(|mut tx| async {
            let statment_result = tx
                .execute_statement("select * from cago where cago.id > 0")
                .await?;
            let r: Vec<Penguin> = statment_result
                .raw_values()
                .into_iter()
                .filter_map(|reader| from_ion(reader).ok())
                .collect();
            tx.commit(r).await
        })
        .await?;
    println!("{:?}", penguins);
    Ok(())
}
