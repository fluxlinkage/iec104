use std::sync::Arc;

use snafu::ResultExt;
use tokio::task::JoinHandle;

use crate::{
	asdu::Asdu, client::{Client, OnNewObjects}, config::ClientConfig, error::Error
};

const CLEAN_UP_SECONDS: u64 = 60;

#[async_trait::async_trait]
pub trait ServerCallbacks {
	async fn on_connection_requested(
		&self,
		address: std::net::SocketAddr,
	) -> Option<Arc<dyn OnNewObjects + Send + Sync>>;
}

struct ServerInternal {
	// Although it is a little strange, server configurations are the same as client configurations.
	config: ClientConfig,
	stop_watch_tx: tokio::sync::watch::Sender<bool>,
	stop_watch_rx: tokio::sync::watch::Receiver<bool>,
	connections: Vec<Client>,
}

impl ServerInternal {
	async fn run_results(
		&mut self,
		listener: tokio::net::TcpListener,
		callbacks: Arc<dyn ServerCallbacks + Send + Sync>,
        mut broadcast_mpsc_rx: tokio::sync::broadcast::Receiver<Asdu>
	) -> Result<(), Error> {
		self.stop_watch_tx.send_replace(false);
		let clean_up_interval = tokio::time::Duration::from_secs(CLEAN_UP_SECONDS);
		loop {
			tokio::select! {
				res = self.stop_watch_rx.changed()=>{
					res.whatever_context("Channel error")?;
					if *self.stop_watch_rx.borrow_and_update(){
						break;
					}
				}
				res = listener.accept() =>{
					let (stream,addr)=res.whatever_context("Error waiting connection")?;
					match callbacks.on_connection_requested(addr.clone()).await{
						Some(connection_callbacks) => {
							match Client::new_server_side(stream, self.config.to_owned(), connection_callbacks).await{
								Ok(client) => {
									tracing::info!("New connection from {}",addr);
									self.connections.push(client);
								}
								Err(e) => {
									tracing::warn!("Error initilizing connection from {} : {}",addr,e);
								}
							}
						}
						None => {
							// Reject connection
							tracing::info!("Rejected connection from {}",addr)
						}
					}
				}
                res=broadcast_mpsc_rx.recv()=>{
                    let asdu=res.whatever_context("Channel error")?;
                    for connection in &self.connections{
                        if let Ok(_)= connection.check_connection_started(){
                            if let Err(e)=connection.send_asdu(asdu.clone()).await{
                                tracing::warn!("Error sending {:?} : {}",asdu,e);
                            }
                        }
                    }
                }
				_ = tokio::time::sleep(clean_up_interval)=>{
					self.connections.retain(|connection|{
                        if let Ok(_)= connection.check_connection_started(){
                            true
                        }else{
                            false
                        }
                    });
				}
			}
		}
		return Ok(());
	}
	async fn run(
		&mut self,
		listener: tokio::net::TcpListener,
		callbacks: Arc<dyn ServerCallbacks + Send + Sync>,
        broadcast_mpsc_rx: tokio::sync::broadcast::Receiver<Asdu>
	) {
		if let Err(e) = self.run_results(listener, callbacks,broadcast_mpsc_rx).await {
			tracing::error!("{}", e);
		}
		self.stop_watch_tx.send_replace(true);
	}
}

pub struct Server {
	stop_watch_tx: tokio::sync::watch::Sender<bool>,
	stop_watch_rx: tokio::sync::watch::Receiver<bool>,
    broadcast_mpsc_tx: tokio::sync::broadcast::Sender<Asdu>,
	background_task: JoinHandle<()>,
}

// This seems meaningless, but if I don't write this, it keeps warning.
impl core::fmt::Debug for Server {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Server handler")
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		self.stop();
		self.background_task.abort();
	}
}

impl Server {
	pub async fn new(
		config: ClientConfig,
		callbacks: impl ServerCallbacks + Send + Sync + 'static,
	) -> Result<Self, Error> {
		let listener = tokio::net::TcpListener::bind(format!("{}:{}", config.address, config.port))
			.await
			.whatever_context("Bind port error")?;
		let (stop_watch_tx, stop_watch_rx) = tokio::sync::watch::channel(true);
        let (broadcast_mpsc_tx,broadcast_mpsc_rx)=tokio::sync::broadcast::channel(1024);
		let mut server = ServerInternal {
			config,
			stop_watch_tx: stop_watch_tx.clone(),
			stop_watch_rx: stop_watch_rx.clone(),
			connections: Vec::new(),
		};
		let join_handle =
			tokio::spawn(async move { server.run(listener, Arc::new(callbacks),broadcast_mpsc_rx).await });
		return Ok(Self { stop_watch_tx, stop_watch_rx,broadcast_mpsc_tx, background_task: join_handle });
	}
	pub async fn wait_until_stop(&mut self) {
		loop {
			if let Err(e) = self.stop_watch_rx.changed().await {
				tracing::error!("Channel error {}", e);
				return;
			}
			if *self.stop_watch_rx.borrow_and_update() {
				return;
			}
		}
	}
	pub fn stop(&self) {
		self.stop_watch_tx.send_replace(true);
	}
	pub fn broadcast(&self,asdu:Asdu)->Result<(),Error>{
        self.broadcast_mpsc_tx.send(asdu).whatever_context("Failed sending broadcast")?;
        return Ok(());
    }
}
