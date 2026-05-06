use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use reqwest_eventsource::{Event, EventSource};
use serde::Deserialize;
use serde_json::{json, Value};

const BASE_URL: &str = "https://api.kimi.com/coding";
// Kimi Coding gates access by User-Agent; this value is on the whitelist.
const USER_AGENT: &str = "KimiCLI/1.5";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 32000;

#[derive(Deserialize)]
struct StreamEvent {
    r#type: String,
    delta: Option<Delta>,
}

#[derive(Deserialize)]
struct Delta {
    r#type: Option<String>,
    text: Option<String>,
}

/// Call the Kimi Coding API (Anthropic Messages format) and return the full response text.
pub async fn call(
    api_key: &str,
    model_id: &str,
    messages: Vec<Value>,
) -> Result<String> {
    let client = reqwest::Client::new();

    let body = json!({
        "model": model_id,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "stream": true,
        "messages": messages,
    });

    let request = client
        .post(format!("{BASE_URL}/v1/messages"))
        .header("x-api-key", api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
        .json(&body);

    let mut es = EventSource::new(request)
        .map_err(|e| anyhow!("Failed to connect to Kimi Coding API: {e}"))?;

    let mut text = String::new();
    while let Some(event) = es.next().await {
        match event {
            Ok(Event::Open) => {}
            Ok(Event::Message(msg)) => {
                if msg.data == "[DONE]" {
                    break;
                }
                if let Ok(ev) = serde_json::from_str::<StreamEvent>(&msg.data) {
                    if ev.r#type == "content_block_delta" {
                        if let Some(delta) = ev.delta {
                            if delta.r#type.as_deref() == Some("text_delta") {
                                if let Some(t) = delta.text {
                                    text.push_str(&t);
                                }
                            }
                        }
                    }
                }
            }
            Err(reqwest_eventsource::Error::StreamEnded) => break,
            Err(e) => return Err(anyhow!("Kimi Coding stream error: {e}")),
        }
    }

    Ok(text)
}
