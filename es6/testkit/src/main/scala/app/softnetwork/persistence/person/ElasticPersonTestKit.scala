package app.softnetwork.persistence.person

import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit
import app.softnetwork.persistence.scalatest.InMemoryPersistenceTestKit

trait ElasticPersonTestKit
    extends PersonTestKit
    with InMemoryPersistenceTestKit
    with EmbeddedElasticTestKit {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initAndJoinCluster()
  }
}
