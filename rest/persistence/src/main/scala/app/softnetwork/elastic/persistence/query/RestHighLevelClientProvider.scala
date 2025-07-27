package app.softnetwork.elastic.persistence.query

import app.softnetwork.elastic.client.rest.RestHighLevelClientApi
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.model.Timestamped

trait RestHighLevelClientProvider[T <: Timestamped]
    extends ElasticProvider[T]
    with RestHighLevelClientApi {
  _: ManifestWrapper[T] =>

}
