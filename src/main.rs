use redis::AsyncCommands;
use redis::Client;
use dotenv::dotenv;
use futures_util::StreamExt;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::task;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    dotenv().ok();
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let channel_name = env::var("CHANNEL_NAME").expect("CHANNEL_NAME must be set");

    let client = Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(&channel_name).await?;

    println!("Subscribed to the channel: {}", channel_name);

    while let Some(msg) = pubsub.on_message().next().await {
        let payload: String = msg.get_payload()?;
        println!("Received: {}", payload);

        let client_clone = client.clone();
        let channel_name_clone = channel_name.clone();

        task::spawn(async move {
            if let Err(err) = handle_message(payload, client_clone, channel_name_clone).await {
                eprintln!("Error handling message: {}", err);
            }
        });
    }

    Ok(())
}

#[allow(deprecated, dependency_on_unit_never_type_fallback)]
async fn handle_message(
    payload: String,
    client: Client,
    channel_name: String,
) -> Result<(), redis::RedisError> {
    log_to_file(format!("PROCESSING MESSAGE: {}", payload))
        .map_err(|e| redis::RedisError::from(e))?;


    // TODO: call the business logic that must be in another file (use the main enty point in this file and show me how to inject the code here)

    let mut con = client.get_async_connection().await?;
    let response_message = "Hi stranger";

    con.publish(channel_name, response_message).await?;
    println!("Published: {}", response_message);

    Ok(())
}

fn log_to_file(content: String) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("microservice.log")?;
    writeln!(file, "{}", content)?;
    Ok(())
}
