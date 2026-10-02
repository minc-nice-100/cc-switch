//! 使用量写入的重试队列
//!
//! # 背景
//!
//! `proxy_request_logs` 只写 token/成本/延迟这类标量，不碰响应体。但它仍然
//! 是磁盘 IO：`Database` 是单连接 + `Mutex<Connection>`（`database/mod.rs`），
//! 机械盘上一次 fsync 就能耗掉几十毫秒。原实现由 tokio worker 线程直接同步
//! 调用 `UsageLogger::log_request`，磁盘一慢就把 runtime 饿死。
//!
//! 现在的链路是：
//!
//! ```text
//! response_processor / handlers
//!   └─ UsageLogger::log_with_calculation_async(Arc<Database>, …)
//!        └─ spawn_blocking ─→ 同步写入
//!             ├─ Ok      → 前端刷新事件
//!             └─ Err     → retry_log(…) 入队，不丢记录
//! ```
//!
//! # 为什么是内存队列而不是 WAL
//!
//! 应用侧只有一个连接、且所有访问都被同一把 `Mutex` 串行化。WAL 的收益
//! （读写并发）来自多连接，这里不存在；代价（`-wal`/`-shm` 边文件，以及
//! `database/backup.rs` 里 20 处直接操作库文件的路径）反而实实增大。所以
//! 走 `spawn_blocking` 移出 runtime + 内存重试，不动 journal 模式。
//!
//! # 边界
//!
//! - `sync_channel` 有界：写满时 `try_send` 失败，本条记录丢弃并落 error。
//!   这是**有意的背压**，防止上游持续故障时内存无限增长。
//! - 进程退出时队列内未写入的记录会丢失。使用量是尽力而为的计账数据，不
//!   值得为它加崩溃恢复盘；真要保证不丢得单独设计持久化队列（见 issue）。

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::database::Database;
use super::logger::UsageLogger;
use super::parser::TokenUsage;

/// 队列容量。按"上游短暂抖动期间积压的日志条数"估：代理侧一次请求一条，
/// 10000 足够覆盖数分钟的持续故障，超过则说明故障已超出重试能救的范围。
const QUEUE_CAPACITY: usize = 10_000;

/// 每条记录的最大尝试次数（含首次）。
///
/// 有限次而不是无限：无限重试会让队列在持续故障时被坏记录堵住，
/// 后面正常记录的写入也被拖住。超过次数后落 error 日志并丢弃——
/// 到这一步说明故障已持续数十秒，日志里留下痕迹可追。
const MAX_ATTEMPTS: u32 = 5;

/// 重试退避基数，按尝试次数线性放大：250ms / 500ms / 750ms / 1s。
const RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// 待重试的使用量记录。
///
/// 字段与 [`super::logger::UsageLogger::log_with_calculation`] 一致，之所以
/// 不直接存 `RequestLog`：后者含已算好的 `Option<CostBreakdown>`，而重试时
/// 定价表可能已更新（补价、同步），重算比复用陈旧成本更正确。
#[derive(Debug, Clone)]
pub(crate) struct RetryLog {
    pub request_id: String,
    pub provider_id: String,
    pub app_type: String,
    pub model: String,
    pub request_model: String,
    pub pricing_model: String,
    pub usage: TokenUsage,
    pub latency_ms: u64,
    pub first_token_ms: Option<u64>,
    pub status_code: u16,
    pub session_id: Option<String>,
    pub provider_type: Option<String>,
    pub is_streaming: bool,
}

/// 全局发送端。队列是进程级的：代理重启会重建 `ProxyStatus`，但 DB 连接
/// 由 `AppState` 持有，重试队列跟着进程走即可。
static RETRY_TX: OnceLock<Sender<(Arc<Database>, RetryLog)>> = OnceLock::new();

fn sender() -> &'static Sender<(Arc<Database>, RetryLog)> {
    RETRY_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        tokio::spawn(drain(rx));
        tx
    })
}

/// 把写入失败的记录重新入队。
///
/// 队列已满时记录 error 并丢弃——背压，见模块文档。**不会 panic**：
/// 记账失败不能反过来让请求转发失败。
pub(crate) fn retry_log(db: Arc<Database>, log: RetryLog) {
    if let Err(error) = sender().try_send((db, log)) {
        log::error!(
            "[USG-004] 使用量重试队列已满（capacity={QUEUE_CAPACITY}），丢弃一条记录: {error}"
        );
    }
}

/// 持续消费队列：对每条记录做有限次退避重试，成功即继续下一条。
///
/// 串行处理是有意的：所有 DB 写入本来就共用一把连接锁，并发重试只会让它们
/// 互相排队，还会把 `spawn_blocking` 线程池占满。
async fn drain(mut rx: Receiver<(Arc<Database>, RetryLog)>) {
    while let Some((db, log)) = rx.recv().await {
        let request_id = log.request_id.clone();
        let mut attempt = 0_u32;
        let mut backoff = RETRY_BASE_DELAY;

        loop {
            attempt += 1;
            // 首次也退避：入队本身就说明刚刚失败过，立刻重写大概率再失败。
            tokio::time::sleep(backoff).await;

            let written = tokio::task::spawn_blocking({
                let db = Arc::clone(&db);
                let log = log.clone();
                move || {
                    let logger = UsageLogger { db: &db };
                    logger.log_with_calculation(
                        log.request_id,
                        log.provider_id,
                        log.app_type,
                        log.model,
                        log.request_model,
                        log.pricing_model,
                        log.usage,
                        log.latency_ms,
                        log.first_token_ms,
                        log.status_code,
                        log.session_id,
                        log.provider_type,
                        log.is_streaming,
                    )
                }
            })
            .await;

            match written {
                // 成功：跳出重试循环，处理下一条。
                Ok(Ok(())) => break,
                Ok(Err(error)) => {
                    log::warn!(
                        "[USG-005] 使用量写入第 {attempt}/{MAX_ATTEMPTS} 次失败 (id={request_id}): {error}"
                    );
                }
                Err(join_error) => {
                    log::warn!(
                        "[USG-006] 使用量写入任务第 {attempt}/{MAX_ATTEMPTS} 次异常 (id={request_id}): {join_error}"
                    );
                }
            }

            if attempt >= MAX_ATTEMPTS {
                log::error!(
                    "[USG-007] 使用量记录 {MAX_ATTEMPTS} 次尝试后仍写入失败，放弃: id={request_id}"
                );
                break;
            }

            backoff = RETRY_BASE_DELAY * attempt;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 队列满时的背压语义：`try_send` 返回 Err，调用方据此落 error 而非丢
    /// 弃或 panic。用一个 `()` 通道复现同一 API 的失败形状，不需要真连库。
    #[tokio::test]
    async fn a_full_channel_reports_failure_rather_than_panicking() {
        let (tx, rx) = mpsc::channel::<()>(1);
        // 占满容量且不消费。
        tx.send(()).await.expect("first send fits the capacity");

        let second = tx.try_send(());
        assert!(second.is_err(), "容量已满时应报错而不是 panic");

        drop(rx);
    }

    /// 退避按尝试次数线性放大，不会无限增长（第 1 次 250ms、第 4 次 1s）。
    #[test]
    fn backoff_grows_linearly_and_stays_bounded() {
        let first = RETRY_BASE_DELAY * 1;
        let fourth = RETRY_BASE_DELAY * 4;

        assert_eq!(first, Duration::from_millis(250));
        assert_eq!(fourth, Duration::from_millis(1000));
        // 最坏情况 = 5 次尝试的总等待，应远小于一次请求超时量级。
        let worst_total: Duration = (1..=MAX_ATTEMPTS).map(|n| RETRY_BASE_DELAY * n).sum();
        assert!(worst_total <= Duration::from_secs(5));
    }
}
