package app.softnetwork.persistence.query

import app.softnetwork.elastic.persistence.query.State2ElasticProcessorStream
import app.softnetwork.persistence.message.CrudEvent
import app.softnetwork.persistence.model.Timestamped

trait State2ElasticProcessorStreamWithRestProvider[T <: Timestamped, E <: CrudEvent]
    extends State2ElasticProcessorStream[T, E]
    with RestHighLevelClientProvider[T] { _: JournalProvider with OffsetProvider => }
