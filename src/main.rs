use redis::{AsyncCommands, Client, Msg};
use dotenv::dotenv;
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

    // Connect to Redis
    let client = Client::open(redis_url)?;
    let mut connection = client.get_async_connection().await?;
    let mut pubsub = connection.as_pubsub();
    pubsub.subscribe(channel_name).await?;

    println!("Subscribed to the channel.");

    loop {
        if let Some(msg) = pubsub.on_message().next().await {
            let payload: String = msg.get_payload().expect("Payload should be valid UTF-8");
            println!("Received: {}", payload);

            // Spawn a new task for each publication
            task::spawn(handle_message(payload));
        }
    }
}

async fn handle_message(payload: String) {
    log_to_file(format!("Payload: {}", payload)).expect("Failed to write to log");

    // Wait another second
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Log "finished"
    log_to_file("Finished".to_string()).expect("Failed to write to log");
}

fn log_to_file(content: String) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("microservice.log")?;
    writeln!(file, "{}", content)?;
    Ok(())
}
