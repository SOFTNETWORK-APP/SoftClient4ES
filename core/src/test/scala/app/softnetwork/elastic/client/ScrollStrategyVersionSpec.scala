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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.sql.query.{SQLAggregation, SelectStatement}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

/** SCHEMA-7 (A-63, A-64; R-85) -- the scroll path reads the ONCE-per-client version
  * (`VersionApi.dateDialectVersion`), never `version`: a user whose role is denied `GET /` (a
  * `read`-only user, 403) pages by the major the client module is built for -- 6 and 7 by
  * search_after, 8 and 9 by PIT, whose slicing asks `_settings` (403 too, remembered: one slice)
  * --, and `GET /` is asked once per client, however many statements run. Main asked `version` on
  * every un-LIMITed statement and failed it ("Failed to get ES version").
  *
  * Each client here is a NEW instance: the version is kept for the client's life.
  */
class ScrollStrategyVersionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("scroll-strategy-version-spec")
  implicit val context: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  private def forbidden(action: String): ElasticError =
    ElasticError(
      message = s"action [$action] is unauthorized for user [ro]",
      statusCode = Some(403)
    )

  /** A client whose `GET /` answers `answer`, built for `major`; `_settings` answers 3 shards, or a
    * 403 when `readOnly`, as `GET <index>` does then -- the statement typed from `_field_caps`, as
    * a `read` user's is (T0-3). Records the paging the stream took and the slices it was given.
    */
  private class VersionClient(
    answer: ElasticResult[String],
    major: Option[Int],
    readOnly: Boolean = true
  ) extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    override protected def configuredMaxSlices: Int = ScrollConfig.DefaultMaxSlices
    // the `_settings` lookup is remembered for the schema-cache TTL (story 21.8 Part D)
    override protected def schemaCacheTtlMs: Long = 5 * 60 * 1000L

    val versionCalls = new AtomicInteger(0)
    val settingsCalls = new AtomicInteger(0)
    val paging = new AtomicReference[String]("")
    val slices = new AtomicReference[Int](0)

    override private[client] def executeVersion(): ElasticResult[String] = {
      versionCalls.incrementAndGet()
      answer
    }

    override private[client] def buildElasticMajor: Option[Int] = major

    // a `read` user may not read the mapping (403), but may read the field caps
    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
      if (readOnly) ElasticFailure(forbidden("indices:admin/get")) else super.executeGetIndex(index)

    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] =
      ElasticSuccess(
        Some(
          s"""{"indices":["$source"],"fields":{"id":{"keyword":{"type":"keyword",""" +
          """"metadata_field":false,"searchable":true,"aggregatable":true}}}}"""
        )
      )

    override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
      settingsCalls.incrementAndGet()
      if (readOnly) ElasticFailure(forbidden("indices:monitor/settings/get"))
      else
        ElasticSuccess(s"""{"$index":{"settings":{"index":{"number_of_shards":"3"}}}}""")
    }

    private def page(name: String, config: ScrollConfig): Source[ListMap[String, Any], NotUsed] = {
      paging.set(name)
      slices.set(config.slices)
      Source(List(ListMap[String, Any]("id" -> "doc-0")))
    }

    override private[client] def scrollClassic(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      aggregations: ListMap[String, SQLAggregation],
      config: ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = page("scroll", config)

    override private[client] def searchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = page("search_after", config)

    override private[client] def pitSearchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = page("pit", config)
  }

  private def run(client: VersionClient): Seq[(ListMap[String, Any], ScrollMetrics)] =
    Await.result(
      client.scroll(SelectStatement("SELECT id FROM idx_a")).runWith(Sink.seq),
      30.seconds
    )

  private val denied: ElasticResult[String] = ElasticFailure(forbidden("cluster:monitor/main"))

  "A read-only user's un-LIMITed statement" should "page by the major the client is built for" in {
    Seq(
      Some(6) -> ("search_after", 1),
      Some(7) -> ("search_after", 1),
      Some(8) -> ("pit", 1),
      Some(9) -> ("pit", 1)
    ).foreach { case (major, (paging, slices)) =>
      withClue(s"build major $major: ") {
        val client = new VersionClient(denied, major)
        // three statements: the rows answer, `GET /` is asked once
        (1 to 3).foreach { _ =>
          run(client).map(_._1("id")) shouldBe Seq("doc-0")
          client.paging.get() shouldBe paging
          client.slices.get() shouldBe slices
        }
        client.versionCalls.get() shouldBe 1
        // PIT's slicing asks `_settings` once: its 403 is remembered for the TTL
        client.settingsCalls.get() shouldBe (if (paging == "pit") 1 else 0)
      }
    }
  }

  it should "fail the stream as main did when no version is known at all (a core client)" in {
    val client = new VersionClient(denied, None)
    val source = client.scroll(SelectStatement("SELECT id FROM idx_a"))
    val failure = Await.result(source.runWith(Sink.seq).failed, 30.seconds)
    failure.getMessage should include("Failed to get ES version")
  }

  "A version the cluster answers" should "decide the paging as before" in {
    val older = new VersionClient(ElasticSuccess("7.11.0"), Some(7), readOnly = false)
    run(older)
    older.paging.get() shouldBe "search_after"
    val current = new VersionClient(ElasticSuccess("8.18.3"), Some(8), readOnly = false)
    run(current)
    run(current)
    current.paging.get() shouldBe "pit"
    current.slices.get() shouldBe 3
    current.versionCalls.get() shouldBe 1
  }
}
