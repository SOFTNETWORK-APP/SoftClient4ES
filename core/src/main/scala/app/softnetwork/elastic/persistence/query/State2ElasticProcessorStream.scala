package app.softnetwork.elastic.persistence.query

import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.query.{
  JournalProvider,
  OffsetProvider,
  State2ExternalProcessorStream
}
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.persistence.message._

/** Created by smanciot on 16/05/2020.
  */
trait State2ElasticProcessorStream[T <: Timestamped, E <: CrudEvent]
    extends State2ExternalProcessorStream[T, E]
    with ManifestWrapper[T] { _: JournalProvider with OffsetProvider with ElasticProvider[T] =>

  override val externalProcessor = "elastic"

  override protected def init(): Unit = {
    initIndex()
  }

}
