package app.softnetwork.elastic.persistence.query

import app.softnetwork.elastic.client.jest.JestClientApi
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.model.Timestamped

/** Created by smanciot on 20/05/2021.
  */
trait JestProvider[T <: Timestamped] extends ElasticProvider[T] with JestClientApi {
  _: ManifestWrapper[T] =>
}
