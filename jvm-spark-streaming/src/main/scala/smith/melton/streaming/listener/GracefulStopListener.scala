package smith.melton.streaming.listener

import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * @author Melton Smith
 * @since 18.06.2025
 */
class GracefulStopListener() extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    ???
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    ???
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    ???
  }
}
