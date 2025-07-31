package app.softnetwork.elastic.persistence.query

import app.softnetwork.elastic.client.java.ElasticsearchClientApi
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.model.Timestamped

trait ElasticsearchClientProvider[T <: Timestamped]
    extends ElasticProvider[T]
    with ElasticsearchClientApi {
  _: ManifestWrapper[T] =>

}
