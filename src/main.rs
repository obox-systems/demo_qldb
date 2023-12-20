#![ warn( rust_2018_idioms ) ]
#![ warn( missing_debug_implementations ) ]

use amazon_qldb_driver::aws_sdk_qldbsession::Config;
use amazon_qldb_driver::QldbDriverBuilder;
use anyhow::Result;
use ion_c_sys::reader::IonCReader;
use tokio;


#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let aws_config = aws_config::load_from_env().await;

    let driver = QldbDriverBuilder::new()
        .ledger_name("kawasaki")
        .sdk_config(Config::new(&aws_config))
        .await?;

    //create table
    driver
        .transact(|mut tx| async {
            let _ = tx.execute_statement("create table cago").await?;
            tx.commit(()).await
        })
        .await?;
    //insert values
    driver
        .transact(|mut tx| async {
            let _ = tx
                .execute_statement(
                    "insert into cago <<{'id':1, 'name':'crigo'}, {'id':2, 'name':'estriper'}>>",
                )
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
    let estriper = driver
        .transact(|mut tx| async {
            let result = tx.execute_statement("select value name from cago where cago.id = 2").await?;
            let r: Vec<String> = result.readers()
            .map(|reader| {
                let mut reader = reader?;
                let _ = reader.next()?;
                let s = reader.read_string()?;
                Ok(s.as_str().to_string())
            })
            .filter_map(|it: Result<String>| it.ok())
            .collect();
            tx.commit(r).await
        })
        .await?;
    println!("name {:?}", estriper);
    Ok(())
}
