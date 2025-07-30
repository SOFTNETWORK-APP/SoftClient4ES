package app.softnetwork.elastic.persistence.query

import app.softnetwork.persistence.message.CrudEvent
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.persistence.query.{JournalProvider, OffsetProvider}

trait State2ElasticProcessorStreamWithJavaProvider[T <: Timestamped, E <: CrudEvent]
    extends State2ElasticProcessorStream[T, E]
    with ElasticsearchClientProvider[T] { _: JournalProvider with OffsetProvider => }
