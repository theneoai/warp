//! Client-side direct API calls for providers that have their own keys stored locally.
//!
//! Kimi Coding uses the Anthropic Messages API format.
//! MiniMax China uses the OpenAI Chat Completions API format.
//!
//! Both bypass the Warp server entirely — the request goes from the user's
//! machine straight to the provider's endpoint.

use std::sync::Arc;

use serde_json::{json, Value};

static HTTP_CLIENT: std::sync::LazyLock<http_client::Client> =
    std::sync::LazyLock::new(http_client::Client::new);
use uuid::Uuid;
use warp_multi_agent_api::{
    self as api,
    client_action,
    response_event::{self, stream_finished, ClientActions},
    ClientAction, ResponseEvent,
};

use crate::ai::agent::AIAgentInput;
use crate::server::server_api::AIApiError;

pub mod anthropic;
pub mod openai;

#[derive(Clone, Debug)]
pub struct DirectApiConfig {
    pub kind: DirectApiKind,
    pub api_key: String,
}

#[derive(Clone, Debug)]
pub enum DirectApiKind {
    /// Kimi Coding — Anthropic Messages API at api.kimi.com/coding
    KimiCoding,
    /// MiniMax China — OpenAI-compatible API at api.minimaxi.com
    MinimaxCN,
}

impl DirectApiKind {
    pub fn default_model(&self) -> &'static str {
        match self {
            Self::KimiCoding => "kimi-for-coding",
            Self::MinimaxCN => "MiniMax-M2",
        }
    }
}

pub type Event = Result<ResponseEvent, Arc<AIApiError>>;

pub async fn generate(
    inputs: Vec<AIAgentInput>,
    tasks: Vec<api::Task>,
    config: DirectApiConfig,
    model_id: &str,
) -> impl futures_util::Stream<Item = Event> + Send + 'static {
    let model_id = if model_id.is_empty() {
        config.kind.default_model().to_string()
    } else {
        model_id.to_string()
    };

    let messages = build_messages(&tasks, &inputs);

    let result = match &config.kind {
        DirectApiKind::KimiCoding => {
            anthropic::call(&HTTP_CLIENT, &config.api_key, &model_id, messages).await
        }
        DirectApiKind::MinimaxCN => {
            openai::call(
                &HTTP_CLIENT,
                &config.api_key,
                &model_id,
                messages,
                openai::MINIMAX_CN_BASE_URL,
            )
            .await
        }
    };

    let request_id = Uuid::new_v4().to_string();
    let conv_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4().to_string();
    let msg_id = Uuid::new_v4().to_string();

    let events: Vec<Event> = match result {
        Ok(text) => vec![
            Ok(init_event(&request_id, &conv_id)),
            Ok(create_task_event(&task_id)),
            Ok(add_message_event(&task_id, &msg_id, &request_id, text)),
            Ok(finished_event()),
        ],
        Err(e) => vec![
            Ok(init_event(&request_id, &conv_id)),
            Err(Arc::new(AIApiError::Other(e))),
        ],
    };

    futures_util::stream::iter(events)
}

fn build_messages(tasks: &[api::Task], inputs: &[AIAgentInput]) -> Vec<Value> {
    let mut messages: Vec<Value> = vec![];

    for task in tasks {
        for msg in &task.messages {
            match &msg.message {
                Some(api::message::Message::UserQuery(uq)) => {
                    if !uq.query.is_empty() {
                        messages.push(json!({"role": "user", "content": uq.query}));
                    }
                }
                Some(api::message::Message::AgentOutput(out)) => {
                    if !out.text.is_empty() {
                        messages.push(json!({"role": "assistant", "content": out.text}));
                    }
                }
                _ => {}
            }
        }
    }

    // inputs are oldest-to-newest; reverse to find the most recent user-visible query
    for input in inputs.iter().rev() {
        match input {
            AIAgentInput::UserQuery { query, .. }
            | AIAgentInput::AutoCodeDiffQuery { query, .. }
            | AIAgentInput::CreateNewProject { query, .. } => {
                if !query.is_empty() {
                    messages.push(json!({"role": "user", "content": query}));
                    break;
                }
            }
            _ => {}
        }
    }

    if messages.is_empty() {
        messages.push(json!({"role": "user", "content": ""}));
    }

    messages
}

fn init_event(request_id: &str, conv_id: &str) -> ResponseEvent {
    ResponseEvent {
        r#type: Some(response_event::Type::Init(response_event::StreamInit {
            request_id: request_id.to_string(),
            conversation_id: conv_id.to_string(),
            run_id: String::new(),
        })),
    }
}

fn create_task_event(task_id: &str) -> ResponseEvent {
    ResponseEvent {
        r#type: Some(response_event::Type::ClientActions(ClientActions {
            actions: vec![ClientAction {
                action: Some(client_action::Action::CreateTask(
                    client_action::CreateTask {
                        task: Some(api::Task {
                            id: task_id.to_string(),
                            messages: vec![],
                            dependencies: None,
                            description: String::new(),
                            summary: String::new(),
                            server_data: String::new(),
                        }),
                    },
                )),
            }],
        })),
    }
}

fn add_message_event(task_id: &str, msg_id: &str, request_id: &str, text: String) -> ResponseEvent {
    ResponseEvent {
        r#type: Some(response_event::Type::ClientActions(ClientActions {
            actions: vec![ClientAction {
                action: Some(client_action::Action::AddMessagesToTask(
                    client_action::AddMessagesToTask {
                        task_id: task_id.to_string(),
                        messages: vec![api::Message {
                            id: msg_id.to_string(),
                            task_id: task_id.to_string(),
                            message: Some(api::message::Message::AgentOutput(
                                api::message::AgentOutput { text },
                            )),
                            request_id: request_id.to_string(),
                            server_message_data: String::new(),
                            citations: vec![],
                            timestamp: None,
                        }],
                    },
                )),
            }],
        })),
    }
}

fn finished_event() -> ResponseEvent {
    ResponseEvent {
        r#type: Some(response_event::Type::Finished(
            response_event::StreamFinished {
                reason: Some(stream_finished::Reason::Done(stream_finished::Done {})),
                conversation_usage_metadata: None,
                token_usage: vec![],
                should_refresh_model_config: false,
                request_cost: None,
            },
        )),
    }
}
