use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use reqwest_eventsource::{Event, EventSource};
use serde::Deserialize;
use serde_json::{json, Value};

const BASE_URL: &str = "https://api.minimaxi.com";

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

/// Call an OpenAI-compatible API (used for MiniMax China) and return the full response text.
pub async fn call(
    api_key: &str,
    model_id: &str,
    messages: Vec<Value>,
    base_url: &str,
) -> Result<String> {
    let client = reqwest::Client::new();

    let body = json!({
        "model": model_id,
        "stream": true,
        "messages": messages,
    });

    let request = client
        .post(format!("{base_url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {api_key}"))
        .header("content-type", "application/json")
        .json(&body);

    let mut es = EventSource::new(request)
        .map_err(|e| anyhow!("Failed to connect to {base_url}: {e}"))?;

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
            Err(reqwest_eventsource::Error::StreamEnded) => break,
            Err(e) => return Err(anyhow!("MiniMax stream error: {e}")),
        }
    }

    Ok(text)
}

pub fn minimax_cn_base_url() -> &'static str {
    BASE_URL
}
