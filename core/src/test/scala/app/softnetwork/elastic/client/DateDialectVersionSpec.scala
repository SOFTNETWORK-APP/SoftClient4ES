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

import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.sql.{StringValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.operator.SetOperator
import app.softnetwork.elastic.sql.query.{
  MultiSearch,
  SQLAggregation,
  SelectStatement,
  SingleSearch
}
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.`type`.SQLTypes
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** A client whose `GET /` answers `answer` -- a 403 by default, a role denied it -- built for the
  * Elasticsearch `major` its module's build info says; it counts its `GET /` and keeps the leg
  * projections a one-shot `_msearch` would carry, which it does not send.
  */
private[elastic] class VersionDouble(major: Option[Int], val log: Logger = VersionDouble.log)
    extends NopeClientApi {

  /** What `GET /` answers: `None`, a 403. */
  @volatile var answer: Option[String] = None

  val calls = new AtomicInteger(0)

  @volatile var legProjections: Seq[LegProjection] = Nil

  override protected def logger: Logger = log

  override private[client] def buildElasticMajor: Option[Int] = major

  override private[client] def executeVersion(): ElasticResult[String] = {
    calls.incrementAndGet()
    answer.fold[ElasticResult[String]](
      ElasticFailure(
        ElasticError("403 Forbidden: action [cluster:monitor/main]", statusCode = Some(403))
      )
    )(ElasticSuccess(_))
  }

  override def multiSearch(
    elasticQueries: ElasticQueries,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: Seq[ListMap[String, Any]] = Seq.empty,
    legProjections: Seq[LegProjection] = Seq.empty
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
    this.legProjections = legProjections
    ElasticFailure(ElasticError("not sent"))
  }
}

private[elastic] object VersionDouble {
  val log: Logger = LoggerFactory.getLogger("app.softnetwork.elastic.client.VersionDouble")

  private def format(f: String): ListMap[String, Value[_]] =
    ListMap[String, Value[_]]("format" -> StringValue(f))

  /** The schema the doubles read set operations with. */
  val schema: Schema = Table(
    "t",
    columns = List(
      Column("c_es", SQLTypes.Timestamp, options = format("epoch_second")),
      Column("c_c1", SQLTypes.Timestamp, options = format("yyyy/MM/dd")),
      Column("tsz", SQLTypes.Timestamp, options = format("yyyy-MM-dd HH:mm z")),
      Column("ts", SQLTypes.Timestamp),
      Column("d", SQLTypes.Date),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("s", SQLTypes.Keyword)
    )
  )

  /** A set operation's requests, with the schema attached, and its operators. */
  def resolved(sql: String): (Seq[SingleSearch], Seq[SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) => (m.requests.map(_.update(Some(schema))), m.resolvedOperators)
      case other                 => throw new IllegalArgumentException(s"[$sql] $other")
    }
}

/** The version a statement reads its dates as (`VersionApi.dateDialectVersion`): the cluster's,
  * else -- a user whose role is denied `GET /` -- the major the client module is built for, asked
  * ONCE per client, shared by every wrapper of it, read by the relational engine's leg schemas and
  * by core's own `UNION ALL`.
  */
class DateDialectVersionSpec extends AnyFlatSpec with Matchers {

  implicit val context: ConversionContext = NativeContext

  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  private def union(a: String, b: String, operator: String = "UNION ALL"): String =
    s"SELECT $a AS k FROM t $operator SELECT $b AS k FROM t"

  private def utc(s: String): ZonedDateTime = ZonedDateTime.parse(s)

  private def shown(v: Any): (Any, Class[_]) =
    v match {
      case null  => (null, classOf[Null])
      case other => (other, other.getClass)
    }

  private def forbidden(major: Option[Int] = Some(8)): VersionDouble = new VersionDouble(major)

  private def answering(version: String, major: Option[Int] = Some(8)): VersionDouble = {
    val double = new VersionDouble(major)
    double.answer = Some(version)
    double
  }

  /** Leg `leg`'s slot at the first column, as the relational engine reads it. */
  private def slot(sql: String, version: => Option[String], leg: Int = 0) = {
    val (requests, operators) = VersionDouble.resolved(sql)
    SetOperationValues.legSchemas(requests, operators, version) match {
      case Right(legs)   => legs(leg).flatMap(_.slots.head).getOrElse(fail(s"[$sql] no slot"))
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  "A client denied GET /" should "read dates as the major its module is built for" in {
    Seq(6, 7, 8).foreach { major =>
      val client = forbidden(Some(major))
      client.dateDialectVersion shouldBe Some(major.toString)
      val typed = slot(union("c_es", "ts", "UNION"), client.dateDialectVersion)
      typed.sqlType.sqlType shouldBe SQLTypes.Timestamp
      typed.asStored shouldBe empty
      shown(typed.read(received("1706697000"))) shouldBe shown(utc("2024-01-31T10:30:00Z"))
      slot(union("ts", "c_es", "UNION"), client.dateDialectVersion).read(
        received("\"2024-01-31T10:30:00Z\"")
      ) shouldBe utc("2024-01-31T10:30:00Z")
    }
    // a client that builds for no major (core's): no dialect, the dates of a format as stored
    val core = forbidden(None)
    core.dateDialectVersion shouldBe None
    slot(union("ts", "ts", "UNION"), core.dateDialectVersion).sqlType shouldBe
    MultiSearch.SetOperationType(SQLTypes.Varchar)
    // the cluster's version where GET / answers
    answering("8.18.3").dateDialectVersion shouldBe Some("8.18.3")
  }

  it should "read them so through every wrapper of the client" in {
    val base = forbidden(Some(8))
    val gateway: GatewayApi = new MetricsElasticClient(base, new MetricsCollector)
    gateway.asInstanceOf[VersionApi].dateDialectVersion shouldBe Some("8")
    base.calls.get() shouldBe 1
  }

  "Core's UNION ALL" should "read a converting date for a user denied GET /" in {
    def rows(client: VersionDouble, sql: String, values: Seq[String]): Seq[(Any, Class[_])] = {
      val (requests, _) = VersionDouble.resolved(sql)
      val mappers = client.unionAllRowMappers(MultiSearch(requests))
      values.zipWithIndex.map { case (json, leg) =>
        shown(mappers(leg)(ListMap("k" -> received(json))).apply("k"))
      }
    }
    val dates = Seq("\"2024-01-31\"", "\"2024/01/31\"")
    val midnight = shown(utc("2024-01-31T00:00:00Z"))
    rows(forbidden(), union("d", "c_c1"), dates) shouldBe Seq(midnight, midnight)
    rows(answering("8.18.3"), union("d", "c_c1"), dates) shouldBe Seq(midnight, midnight)
    // the statements that answered before answer the same values
    Seq(
      union("n", "x")       -> Seq("5", "2.5"),
      union("d", "d")       -> Seq("\"2024-01-31\"", "\"2024-02-01\""),
      union("ts", "ts")     -> Seq("\"2024-01-31T10:30:00Z\"", "1706697000000"),
      union("c_c1", "c_c1") -> Seq("\"2024/01/31\"", "\"2024/01/31\""),
      union("tsz", "tsz")   -> Seq("\"2024-01-31 10:30 UTC\"", "\"2024-01-31 10:30 CET\"")
    ).foreach { case (sql, values) =>
      withClue(sql)(rows(forbidden(), sql, values) shouldBe rows(answering("8.18.3"), sql, values))
    }
    // an agreeing date of a format of its own is read, a zone name kept as stored
    rows(forbidden(), union("c_c1", "c_c1"), Seq("\"2024/01/31\"")) shouldBe Seq(midnight)
    rows(
      forbidden(),
      union("tsz", "tsz"),
      Seq("\"2024-01-31 10:30 UTC\"", "\"2024-01-31 10:30 CET\"")
    ) shouldBe Seq(
      shown(utc("2024-01-31T10:30:00Z")),
      shown("2024-01-31 10:30 CET")
    )
  }

  it should "carry the same reading to the legs of a one-shot _msearch" in {
    val client = forbidden()
    client.updateSchema("t", VersionDouble.schema)
    client.search(
      SelectStatement("SELECT d AS k FROM t LIMIT 5 UNION ALL SELECT c_c1 AS k FROM t LIMIT 5")
    )
    val converters = client.legProjections.map(_.converters.map(_.apply(0)))
    converters should have size 2
    converters.head.map(c => shown(c(received("\"2024-01-31\"")))) shouldBe
    Some(shown(utc("2024-01-31T00:00:00Z")))
    converters(1).map(c => shown(c(received("\"2024/01/31\"")))) shouldBe
    Some(shown(utc("2024-01-31T00:00:00Z")))
  }

  "GET /" should "be asked once per client, whatever the wrappers and the statements" in {
    val appender = new ListAppender[ILoggingEvent]()
    val logback = LoggerFactory.getLogger(VersionDouble.log.getName).asInstanceOf[LogbackLogger]
    appender.start()
    logback.addAppender(appender)
    try {
      val base = forbidden()
      val gateway: GatewayApi = new MetricsElasticClient(base, new MetricsCollector)
      val statements = Seq(
        union("c_es", "ts", "UNION"),
        union("d", "c_c1", "INTERSECT"),
        union("tsz", "ts", "EXCEPT")
      )
      (1 to 10).foreach { i =>
        val (requests, operators) = VersionDouble.resolved(statements(i % statements.size))
        SetOperationValues.legSchemas(
          requests,
          operators,
          gateway.asInstanceOf[VersionApi].dateDialectVersion
        ) shouldBe a[Right[_, _]]
      }
      (1 to 10).foreach { _ =>
        val (requests, _) = VersionDouble.resolved(union("d", "c_c1"))
        base.unionAllRowMappers(MultiSearch(requests))
      }
      base.calls.get() shouldBe 1
      appender.list.asScala.count(_.getLevel == Level.ERROR) shouldBe 1
      // a privilege granted later is seen after a restart: the client keeps its reading
      base.answer = Some("8.18.3")
      gateway.asInstanceOf[VersionApi].dateDialectVersion shouldBe Some("8")
      base.dateDialectVersion shouldBe Some("8")
      base.calls.get() shouldBe 1
    } finally logback.detachAppender(appender)
    // a client whose GET / answers: once, the cluster's version
    val answered = answering("8.18.3")
    val wrapped: GatewayApi = new MetricsElasticClient(answered, new MetricsCollector)
    (1 to 10).foreach { _ =>
      val (requests, operators) = VersionDouble.resolved(union("c_es", "ts", "UNION"))
      SetOperationValues.legSchemas(
        requests,
        operators,
        wrapped.asInstanceOf[VersionApi].dateDialectVersion
      )
      answered.unionAllRowMappers(MultiSearch(VersionDouble.resolved(union("d", "c_c1"))._1))
    }
    answered.calls.get() shouldBe 1
    wrapped.asInstanceOf[VersionApi].dateDialectVersion shouldBe Some("8.18.3")
  }
}
