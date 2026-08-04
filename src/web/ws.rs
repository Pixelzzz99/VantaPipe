use axum::{
    extract::{State, WebSocketUpgrade},
    response::Response,
};

use super::AppState;
use axum::extract::ws::{Message, WebSocket};
use tokio::time::{Duration, interval};

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let initial_logs = state.get_logs();
    for log in &initial_logs {
        if socket.send(Message::Text(log.clone())).await.is_err() {
            return;
        }
    }
    let mut last_sent = initial_logs.len();

    let mut ticket = interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = ticket.tick() => {
                let logs = state.get_logs();
                if logs.len() > last_sent {
                    for log in &logs[last_sent..] {
                        if socket.send(Message::Text(log.clone())).await.is_err() {
                            return;
                        }
                    }
                    last_sent = logs.len();
                } else if logs.len() < last_sent {
                        last_sent = logs.len();
                    }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => return,
                    _=> {}
                }
            }
        }
    }
}
