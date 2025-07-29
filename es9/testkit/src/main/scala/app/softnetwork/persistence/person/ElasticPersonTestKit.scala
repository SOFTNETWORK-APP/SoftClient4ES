package app.softnetwork.persistence.person

import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.scalatest.InMemoryPersistenceTestKit

trait ElasticPersonTestKit
    extends PersonTestKit
    with InMemoryPersistenceTestKit
    with ElasticDockerTestKit {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initAndJoinCluster()
  }
}
