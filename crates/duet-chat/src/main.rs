//! `duet-chat`: the concrete MCP server an instance of Claude Code runs.
//!
//! It exposes two tools over stdio and forwards them to a `duet-bus` broker via
//! [`BusClient`]. Sending goes through this server; *receiving* does not — a
//! background long-poll (kept alive by the `duet-liveness` Stop hook) delivers
//! incoming messages, because only a background shell task can wake Claude.

use clap::Parser;
use duet_mcp::BusClient;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, ServerCapabilities, ServerInfo};
use rmcp::transport::stdio;
use rmcp::{ErrorData as McpError, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[derive(Parser)]
struct Config {
    /// This instance's id on the bus (the `from` on sends, the queue it reads).
    #[arg(long = "self", env = "DUET_SELF")]
    me: String,
    #[arg(long, env = "DUET_URL", default_value = "https://localhost:9443")]
    url: String,
    #[arg(long, env = "DUET_CA")]
    ca: String,
}

#[derive(Clone)]
struct Chat {
    bus: BusClient,
    me: String,
    // Read by the generated `#[tool_handler]` impl; the analyzer can't see that.
    #[allow(dead_code)]
    tool_router: ToolRouter<Chat>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendArgs {
    /// Recipient agent id on the bus (e.g. "bob").
    to: String,
    /// The message text to deliver.
    text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct PollArgs {
    /// How long to wait for a message before returning, in milliseconds.
    #[serde(default = "default_poll_ms")]
    timeout_ms: u64,
}

fn default_poll_ms() -> u64 {
    2_000
}

#[tool_router]
impl Chat {
    fn new(cfg: Config) -> anyhow::Result<Self> {
        let bus = BusClient::new(cfg.url, std::path::Path::new(&cfg.ca))?;
        Ok(Self {
            bus,
            me: cfg.me,
            tool_router: Self::tool_router(),
        })
    }

    #[tool(description = "Send a message to another agent on the bus.")]
    async fn send_message(
        &self,
        Parameters(args): Parameters<SendArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.bus
            .send(&args.to, json!({ "from": self.me, "text": args.text }))
            .await
            .map_err(|e| McpError::internal_error(format!("send failed: {e}"), None))?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "delivered to {}",
            args.to
        ))]))
    }

    #[tool(
        description = "Manually poll for one queued message (backup for the background listener)."
    )]
    async fn poll_messages(
        &self,
        Parameters(args): Parameters<PollArgs>,
    ) -> Result<CallToolResult, McpError> {
        let resp = self
            .bus
            .recv(&self.me, args.timeout_ms)
            .await
            .map_err(|e| McpError::internal_error(format!("poll failed: {e}"), None))?;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            resp.to_string(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for Chat {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            format!(
                "Message bus for agent '{}'. Use send_message to talk to a peer. Incoming \
                 messages are delivered by a background listener, not this server.",
                self.me
            ),
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let chat = Chat::new(Config::parse())?;
    let service = chat.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
