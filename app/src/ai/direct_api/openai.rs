use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use reqwest_eventsource::Event;
use serde::Deserialize;
use serde_json::{json, Value};

pub(super) const MINIMAX_CN_BASE_URL: &str = "https://api.minimaxi.com";

#[derive(Deserialize)]
struct Chunk {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    delta: Delta,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
}

pub async fn call(
    client: &http_client::Client,
    api_key: &str,
    model_id: &str,
    messages: Vec<Value>,
    base_url: &str,
) -> Result<String> {
    let body = json!({
        "model": model_id,
        "stream": true,
        "messages": messages,
    });

    let mut es = client
        .post(format!("{base_url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {api_key}"))
        .header("content-type", "application/json")
        .json(&body)
        .eventsource();

    let mut text = String::new();
    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => {}
            Ok(Event::Message(msg)) => {
                if msg.data == "[DONE]" {
                    break;
                }
                if let Ok(chunk) = serde_json::from_str::<Chunk>(&msg.data) {
                    for choice in chunk.choices {
                        if let Some(content) = choice.delta.content {
                            text.push_str(&content);
                        }
                    }
                }
            }
            Err(e) => return Err(anyhow!("{base_url} stream error: {e}")),
        }
    }

    Ok(text)
}
