package smith.melton.streaming.listener

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryManager}
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.locks.{Lock, ReentrantLock}

/**
 * @author Melton Smith
 * @since 18.06.2025
 */
class GracefulStopListener(streams: StreamingQueryManager) extends StreamingQueryListener {

  private val log = LoggerFactory.getLogger(getClass)
  private val runningQuery = new ConcurrentHashMap[UUID, (Runnable, CountDownLatch)]()

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val stream = streams.get(event.id)
    val latch = new CountDownLatch(2)
    val shutdownHook: Runnable = () => {
      if (stream.isActive) {
        log.info(s"Stop signal arrived, query ${stream.name} will wait util current batch is ready")
        try {
          latch.countDown()
          latch.await()
          log.info(s"Send stop ${stream.name}")
          stream.stop()
          stream.awaitTermination()
          log.info(s"Query ${stream.name} stopped")
        }
      }
    }
    ShutdownHookManager.get().addShutdownHook(shutdownHook, 100, 20, TimeUnit.MINUTES)
    runningQuery.put(stream.id, (shutdownHook, latch))
    log.info(s"Register shutdown hook for query ${event.name}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    log.info(s"Query with name ${event.progress.name}, batch is ready " + event.progress.batchId)
    val (_, latch) = runningQuery.get(event.progress.id)
    if (latch.getCount == 1) //means that shutdown hook is waiting
      latch.countDown()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val (shutdownHook, latch) = runningQuery.remove(event.id)
    log.info(s"Does shutdown hook for ${event.id} exist: " + ShutdownHookManager.get().hasShutdownHook(shutdownHook))
    if (!ShutdownHookManager.get().isShutdownInProgress) {
      log.info(s"Removing a shutdown hook for ${event.id}")
      ShutdownHookManager.get().removeShutdownHook(shutdownHook)
    }
    log.info(s"Query ${event.id} is shutting down, releasing hook...")
    if (latch.getCount == 1) //means that hook is waiting
      latch.countDown()
  }
}
