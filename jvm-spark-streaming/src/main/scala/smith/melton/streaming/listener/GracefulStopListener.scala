package smith.melton.streaming.listener

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryManager}
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.locks.{Lock, ReentrantLock}

/**
 * @author Melton Smith
 * @since 18.06.2025
 */
class GracefulStopListener(streams: StreamingQueryManager) extends StreamingQueryListener {

  private val log = LoggerFactory.getLogger(getClass)
  private val runningQuery = new ConcurrentHashMap[UUID, (Runnable, Lock)]()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val stream = streams.get(event.id)
    val queryLock = new ReentrantLock()
    queryLock.lock()
    val shutdownHook: Runnable = () => {
      if (stream.isActive) {
        log.info(s"stop signal arrived,query ${stream.id} wait for until current batch ready")
        try {
          queryLock.lock()
          log.info(s"Send stop ${stream.id}")
          stream.stop()
          stream.awaitTermination()
          log.info(s"Query ${stream.id} stopped")
        }
        finally {
          queryLock.unlock()
        }
      }
    }
    ShutdownHookManager.get().addShutdownHook(shutdownHook, 100, 20, TimeUnit.MINUTES)
    runningQuery.put(stream.id, (shutdownHook, queryLock))
    log.info(s"Register shutdown hook for query ${event.id}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    log.info(s"Query ${event.progress.id} batch ready " + event.progress.batchId)
    val (_, lock) = runningQuery.get(event.progress.id)
    lock.unlock()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val (shutdownHook, lock) = runningQuery.remove(event.id)
    log.info(s"Do shutdown hook for ${event.id} exist: " + ShutdownHookManager.get().hasShutdownHook(shutdownHook))
    if (!ShutdownHookManager.get().isShutdownInProgress) ShutdownHookManager.get().removeShutdownHook(shutdownHook)
    log.info(s"query ${event.id} shutdown, release hook.")
    lock.unlock()
  }
}
