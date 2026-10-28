//! Hold the real BEGIN packet after SET succeeded, then cancel the borrower.
use super::*;
use sqlx::ConnectOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Barrier = Arc<Mutex<Option<(oneshot::Sender<u64>, oneshot::Receiver<bool>)>>>;

struct Proxy {
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn packet(stream: &mut (impl AsyncRead + Unpin + ?Sized)) -> std::io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let size = usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
    let mut bytes = vec![0; size + 4];
    bytes[..4].copy_from_slice(&header);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(bytes)
}

async fn relay(
    mut client: TcpStream,
    options: MySqlConnectOptions,
    barrier: Barrier,
) -> std::io::Result<()> {
    let mut backend: Box<dyn Io> = match options.get_socket() {
        #[cfg(unix)]
        Some(socket) => Box::new(tokio::net::UnixStream::connect(socket).await?),
        _ => Box::new(TcpStream::connect((options.get_host(), options.get_port())).await?),
    };
    let greeting = packet(&mut *backend).await?;
    let version_end = greeting[5..]
        .iter()
        .position(|b| *b == 0)
        .expect("server greeting");
    let start = 5 + version_end + 1;
    let id = u32::from_le_bytes(greeting[start..start + 4].try_into().unwrap());
    client.write_all(&greeting).await?;
    let (mut server_read, mut server_write) = tokio::io::split(backend);
    let (mut client_read, mut client_write) = client.into_split();
    let upload = async {
        loop {
            let mut bytes = packet(&mut client_read).await?;
            if bytes.get(4) == Some(&3) && bytes[5..].eq_ignore_ascii_case(b"BEGIN") {
                let gate = barrier.lock().unwrap().take();
                if let Some((sent, release)) = gate {
                    let _ = sent.send(u64::from(id));
                    if !release.await.unwrap_or(true) {
                        // The real server rejects this BEGIN; its preceding
                        // SET succeeded, but no transaction consumed the option.
                        let sql = b"\x03START TRANSACTION INVALID";
                        let size = sql.len();
                        let sequence = bytes[3];
                        bytes = vec![size as u8, (size >> 8) as u8, (size >> 16) as u8, sequence];
                        bytes.extend_from_slice(sql);
                    }
                }
            }
            server_write.write_all(&bytes).await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    };
    let download = tokio::io::copy(&mut server_read, &mut client_write);
    tokio::select! { result = upload => result, result = download => result.map(|_| ()) }
}

#[tokio::test]
async fn cancellation_and_begin_error_discard_a_connection_with_unconsumed_isolation() {
    for cancel in [true, false] {
        let original = std::env::var("BREEZE_MYSQL_TEST_URL").unwrap();
        let options = MySqlConnectOptions::from_str(&original).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut url = options.to_url_lossy();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(port)).unwrap();
        let query = url
            .query_pairs()
            .filter(|(key, _)| key != "socket" && key != "ssl-mode")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(query)
            .append_pair("ssl-mode", "disabled");
        let (sent, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let barrier = Arc::new(Mutex::new(Some((sent, released))));
        let proxy = Proxy {
            task: tokio::spawn(async move {
                let mut clients = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let Ok((client,_)) = accepted else { break };
                            let options = options.clone();
                            let barrier = barrier.clone();
                            clients.spawn(async move { let _ = relay(client, options, barrier).await; });
                        }
                        _ = clients.join_next(), if !clients.is_empty() => {}
                    }
                }
            }),
        };
        let mysql = MysqlService::connect_with_options(
            url.as_str(),
            MysqlServiceOptions::default()
                .with_max_connections(1)
                .with_query_timeout(Duration::from_secs(10)),
        )
        .await
        .unwrap();
        let service = mysql.clone();
        let called = Arc::new(AtomicBool::new(false));
        let callback_called = called.clone();
        let operation = tokio::spawn(async move {
            service
                .with_transaction_options(read_committed(), async |_| {
                    callback_called.store(true, Ordering::SeqCst);
                    MysqlResult::Ok(())
                })
                .await
        });
        let old = tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .unwrap()
            .unwrap();
        if cancel {
            operation.abort();
            assert!(operation.await.unwrap_err().is_cancelled());
            let _ = release.send(true);
        } else {
            let _ = release.send(false);
            let error = operation.await.unwrap().unwrap_err();
            assert_eq!(error.database_code(), Some(1064));
        }
        assert!(!called.load(Ordering::SeqCst));
        let next: u64 = mysql.fetch_one("SELECT CONNECTION_ID()", ()).await.unwrap();
        assert_ne!(
            old, next,
            "a pending next-transaction setting cannot return to the pool"
        );
        let default: String = mysql
            .fetch_one("SELECT @@session.transaction_isolation", ())
            .await
            .unwrap();
        assert_eq!(default, "REPEATABLE-READ");
        mysql.close().await;
        drop(proxy);
    }
}
