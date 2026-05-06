use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use reqwest_eventsource::Event;
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

impl StreamEvent {
    fn text_delta(self) -> Option<String> {
        let delta = self.delta?;
        if delta.r#type.as_deref() != Some("text_delta") {
            return None;
        }
        delta.text
    }
}

pub async fn call(
    client: &http_client::Client,
    api_key: &str,
    model_id: &str,
    messages: Vec<Value>,
) -> Result<String> {
    let body = json!({
        "model": model_id,
        "max_tokens": DEFAULT_MAX_TOKENS,
        "stream": true,
        "messages": messages,
    });

    let mut es = client
        .post(format!("{BASE_URL}/v1/messages"))
        .header("x-api-key", api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
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
                if let Ok(ev) = serde_json::from_str::<StreamEvent>(&msg.data) {
                    if ev.r#type == "content_block_delta" {
                        if let Some(t) = ev.text_delta() {
                            text.push_str(&t);
                        }
                    }
                }
            }
            Err(e) => return Err(anyhow!("Kimi Coding stream error: {e}")),
        }
    }

    Ok(text)
}
