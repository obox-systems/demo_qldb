#![ warn( rust_2018_idioms ) ]
#![warn( missing_debug_implementations ) ]

use anyhow::Result;
use ion_binary_rs::IonValue;
use qldb::QldbClient;
use serde::{Serialize, Deserialize};

#[ derive( Debug, Serialize, Deserialize, Clone ) ]
struct Penguin 
{
  id : usize,
  name : String,
}

#[tokio::main]
async fn main() -> Result<()> {
  dotenv::dotenv().ok();
  let client = QldbClient::default( "kawasaki", 200 ).await?;
  // create table
  client.transaction_within
  (
    | client  | async move
    {
      client.query( "create table cago" ).execute().await?;
      Ok( () )
    }
  ).await?;

  let mut penguins = vec!
  [
    Penguin 
    {
      id : 1,
      name : "crigo".into(),
    },
    Penguin 
    {
      id : 2,
      name : "estriper".into(),
    },
    Penguin 
    {
      id : 3,
      name : "Sanya".into(),
    },
  ]
  .into_iter()
  .filter_map( | p | IonValue::try_from( serde_json::to_value( p ).ok()? ).ok() );
  // insert values
  client.transaction_within
  (
    | client | async move
    {
      client
      .query( "insert into cago << ?, ?, ? >>" )
      .param( penguins.next().unwrap() )
      .param( penguins.next().unwrap() )
      .param( penguins.next().unwrap() )
      .execute()
      .await?;
      Ok(())
    }
  ).await?;
  // // delete value
  client.transaction_within
  ( 
    | client | async move
    {
      client
      .query( "delete from cago1 as c where c.id = ?" )
      .param( 1 )
      .execute()
      .await?;
      Ok( () )
    }
  ).await?;
  // select values
  let p = client
  .transaction_within
  ( 
    | client | async move
    {
      let result= client.query( "select * from cago where cago.id > ?" )
      .param( 1 )
      .execute()
      .await?
      .into_vec()
      .into_iter().map
      ( 
        | d |
        {
          Penguin
          {
            id: d.get_value::< i64 >( "id" ).unwrap() as usize, 
            name: d.get_value( "name" ).unwrap()
          }
        }
      ).collect::< Vec< Penguin > >();
      Ok( result )
    }
  ).await?;
  dbg!( p );
  Ok( () )
} 