use redis::Client;
use dotenv::dotenv;
use futures_util::StreamExt;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::task;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // Load environment variables
    dotenv().ok();
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let channel_name = env::var("CHANNEL_NAME").expect("CHANNEL_NAME must be set");

    // Connect to Redis and subscribe to the channel
    let client = Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(&channel_name).await?;

    println!("Subscribed to the channel: {}", channel_name);

    // Message handling loop
    while let Some(msg) = pubsub.on_message().next().await {
        let payload: String = msg.get_payload()?;
        println!("Received: {}", payload);

        // Spawn a new task for each publication
        task::spawn(async move {
            handle_message(payload).await
        });
    }

    Ok(())
}

async fn handle_message(payload: String) -> std::io::Result<()> {
    log_to_file(format!("PROCESSING MESSAGE: {}", payload))?;

    // TODO:: publish a message saying "Hi stranger" to the same channel

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
