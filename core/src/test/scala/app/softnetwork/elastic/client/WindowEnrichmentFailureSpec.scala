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
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.{SQLAggregation, SelectStatement, SingleSearch}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** An un-LIMITed statement with a window function whose window values cannot be computed fails,
  * instead of answering its rows WITHOUT their window values.
  *
  * The scroll route computes the window values first (one aggregation search), then streams the
  * rows and enriches each one. When that aggregation search fails -- for example when some of its
  * shards fail, which core now answers as an error -- the stream used to fall back to the rows
  * alone: every projected window column silently missing, HTTP 200. The fallback stays available
  * only through an explicit `ScrollConfig.failOnWindowError = Some(false)`.
  *
  * Docker-free: the window aggregation answers a failure and the scroll primitives one base row,
  * through the client seams.
  */
class WindowEnrichmentFailureSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)
  private implicit val system: ActorSystem = ActorSystem("window-enrichment-failure-spec")
  private implicit val conversionContext: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }

  private val windowFailure = "1 of 3 shards failed: script_exception: boom"

  private val baseRow: ListMap[String, Any] = ListMap("id" -> "a1", "name" -> "n1", "flag" -> true)

  private class FailingWindowClient extends NopeClientApi {
    override protected def logger: Logger = testLogger

    override private[client] def executeWindowAggregations(
      request: SingleSearch,
      aggRequest: SingleSearch,
      elasticQuery: ElasticQuery
    )(implicit context: ConversionContext): ElasticResult[WindowCache] =
      ElasticResult.failure(ElasticError(message = windowFailure))

    private def rows: Source[ListMap[String, Any], NotUsed] = Source.single(baseRow)

    override private[client] def scrollClassic(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      aggregations: ListMap[String, SQLAggregation],
      config: ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = rows

    override private[client] def searchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = rows

    override private[client] def pitSearchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = rows
  }

  private val sql =
    "SELECT id, name, FIRST_VALUE(name) OVER (PARTITION BY flag ORDER BY name) AS fv FROM t"

  private def scrolled(config: ScrollConfig): Try[Seq[ListMap[String, Any]]] =
    Try(
      Await.result(
        new FailingWindowClient()
          .scroll(SelectStatement(sql), config)
          .map(_._1)
          .runWith(Sink.seq),
        30.seconds
      )
    )

  "an un-LIMITed window whose computation fails" should "fail the stream, not answer rows without window values" in {
    scrolled(ScrollConfig()) match {
      case Failure(e) =>
        e.getMessage shouldBe s"Window function computation failed: $windowFailure"
      case Success(rows) =>
        fail(s"answered ${rows.size} row(s) without their window values: $rows")
    }
  }

  it should "fail it with an explicit failOnWindowError = Some(true) too" in {
    scrolled(ScrollConfig(failOnWindowError = Some(true))).isFailure shouldBe true
  }

  it should "answer the rows without window values only with failOnWindowError = Some(false)" in {
    scrolled(ScrollConfig(failOnWindowError = Some(false))) match {
      case Success(rows) =>
        rows.map(_.get("id")) shouldBe Seq(Some("a1"))
        rows.flatMap(_.get("fv")).flatMap(Option(_)) shouldBe empty
      case Failure(e) => fail(s"the explicit fallback failed: ${e.getMessage}")
    }
  }
}
