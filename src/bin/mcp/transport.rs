//! Preserve legacy MCP initialization when a client probes newer discovery first.

use rmcp::RoleServer;
use rmcp::model::{
    ClientJsonRpcMessage, ClientRequest, ErrorCode, ErrorData, ServerJsonRpcMessage,
};
use rmcp::transport::Transport;

pub(super) struct LegacyDiscovery<T> {
    inner: T,
    initialized: bool,
}

impl<T> LegacyDiscovery<T> {
    pub(super) fn new(inner: T) -> Self {
        Self {
            inner,
            initialized: false,
        }
    }
}

impl<T: Transport<RoleServer>> Transport<RoleServer> for LegacyDiscovery<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        item: ServerJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.inner.send(item)
    }

    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            let message = self.inner.receive().await?;
            if !self.initialized
                && let ClientJsonRpcMessage::Request(request) = &message
            {
                if let ClientRequest::CustomRequest(custom) = &request.request
                    && custom.method == "server/discover"
                {
                    // rmcp 2 rejects this before reaching ServerHandler. A method-
                    // not-found reply lets newer hosts fall back to initialize.
                    let response = ServerJsonRpcMessage::error(
                        ErrorData::new(
                            ErrorCode::METHOD_NOT_FOUND,
                            "server/discover is not supported; use initialize",
                            None,
                        ),
                        Some(request.id.clone()),
                    );
                    if self.inner.send(response).await.is_err() {
                        return None;
                    }
                    continue;
                }
                self.initialized = matches!(request.request, ClientRequest::InitializeRequest(_));
            }
            return Some(message);
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}
