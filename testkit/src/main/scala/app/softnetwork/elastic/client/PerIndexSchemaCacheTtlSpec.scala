/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.schema.{Schema, SchemaCacheTtl}
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

/** An index carries its own schema-cache TTL, in its own mapping metadata (story 21.8 Part D).
  *
  * The unit tests pin the RESOLUTION (metadata > `elastic.schema-cache.ttl` > built-in). What only
  * a real cluster can settle is the ROUND TRIP: that `ALTER TABLE … SET SCHEMA CACHE TTL` reaches
  * Elasticsearch's `_meta`, comes back through `loadSchema`, and — the part that has bitten this
  * project before — SURVIVES a later ALTER, because `Table.update()` rebuilds the `_meta` keys it
  * owns on every schema (a flag not recoverable from the column list is erased on the first round
  * trip; see MEDIUM-7).
  */
trait PerIndexSchemaCacheTtlSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers
    with ScalaFutures {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val patience: PatienceConfig =
    PatienceConfig(timeout = 30.seconds, interval = 100.millis)

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "schema_cache_ttl"

  override def beforeAll(): Unit = {
    super.beforeAll()
    ddl(s"""CREATE TABLE IF NOT EXISTS $index (
           |  id INT NOT NULL,
           |  name VARCHAR
           |)""".stripMargin)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    system.terminate()
    super.afterAll()
  }

  private def ddl(sql: String): Unit =
    client.run(sql).futureValue match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    }

  /** The schema as Elasticsearch answers it — never the in-memory table the DDL produced. The cache
    * is invalidated first so this is a genuine round trip.
    */
  private def freshSchema: Schema = {
    client.invalidateSchema(index)
    client.loadSchema(index) match {
      case ElasticSuccess(schema) => schema
      case ElasticFailure(error)  => fail(s"loadSchema($index) failed: ${error.message}")
    }
  }

  "SET SCHEMA CACHE TTL" should "write the TTL into the index's own metadata" in {
    ddl(s"ALTER TABLE $index SET SCHEMA CACHE TTL = '10m'")
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(600000L))
  }

  it should "survive an unrelated ALTER on the same index" in {
    // `Table.update()` rebuilds `_meta`'s five owned keys on every schema; a key it does not own
    // must come through untouched, or the TTL would silently revert to the default.
    ddl(s"ALTER TABLE $index ADD COLUMN IF NOT EXISTS age INT")
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(600000L))
  }

  it should "be replaceable" in {
    ddl(s"ALTER TABLE $index SET SCHEMA CACHE TTL = '1h'")
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(3600000L))
  }

  it should "accept the equivalent SET MAPPING spelling it desugars to" in {
    ddl(s"ALTER TABLE $index SET MAPPING ${SchemaCacheTtl.MetadataPath} = '30s'")
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(30000L))
  }

  "DROP SCHEMA CACHE TTL" should "return the index to the client default" in {
    // Sets its own precondition rather than inheriting the previous test's: an assertion that a
    // value is ABSENT proves nothing unless this test is what removed it.
    ddl(s"ALTER TABLE $index SET SCHEMA CACHE TTL = '10m'")
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(600000L))
    ddl(s"ALTER TABLE $index DROP SCHEMA CACHE TTL")
    SchemaCacheTtl.of(freshSchema) shouldBe None
  }

  "a TTL that is not a duration" should "be refused before it reaches the cluster" in {
    ddl(s"ALTER TABLE $index SET SCHEMA CACHE TTL = '10m'")
    client.run(s"ALTER TABLE $index SET SCHEMA CACHE TTL = 'soon'").futureValue match {
      case ElasticFailure(error) =>
        error.message should include("not a duration")
      case ElasticSuccess(other) =>
        fail(s"expected a rejection, got $other")
    }
    // The rejected statement changed nothing: the previous value still stands.
    SchemaCacheTtl.of(freshSchema) shouldBe Some(Right(600000L))
  }
}
