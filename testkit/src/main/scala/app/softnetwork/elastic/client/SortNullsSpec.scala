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
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

/** Integration tests for Story 14.1: `NULLS FIRST` / `NULLS LAST` on `ORDER BY`. Verifies that the
  * bridge's `missing` parameter actually controls null placement when executed against a live ES
  * instance.
  *
  * Fixture: 4 documents in the `sort_nulls` index with `bonus` set to (100, null, 50, missing). The
  * null and missing cases both translate to "no value" in ES; the `missing` sort parameter applies
  * to both.
  */
trait SortNullsSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "name":  { "type": "keyword" },
        |    "bonus": { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex("sort_nulls", mappings = None, aliases = Nil).get shouldBe true
    client.setMapping("sort_nulls", mapping).get shouldBe true

    val docs = List(
      """{"id":"1","name":"alice","bonus":100}""",
      """{"id":"2","name":"bob","bonus":null}""",
      """{"id":"3","name":"carol","bonus":50}""",
      """{"id":"4","name":"dave"}"""
    )

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = "sort_nulls",
      logEvery = 5
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }

    client.refresh("sort_nulls")
  }

  override def afterAll(): Unit = {
    client.deleteIndex("sort_nulls")
    unmappedIndices.foreach(client.deleteIndex)
    super.afterAll()
  }

  "ORDER BY bonus DESC NULLS LAST" should "place non-null bonuses before null bonuses" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus DESC NULLS LAST"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val nonNullFirst = rows.takeWhile(_.bonus.isDefined)
        val nullsLast = rows.dropWhile(_.bonus.isDefined)
        nonNullFirst.map(_.name) shouldBe Seq("alice", "carol")
        nullsLast.map(_.name) should contain theSameElementsAs Seq("bob", "dave")
        log.info(s"NULLS LAST order: ${rows.map(_.name).mkString(", ")}")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "place null bonuses before non-null bonuses with NULLS FIRST" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus DESC NULLS FIRST"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val nullsFirst = rows.takeWhile(_.bonus.isEmpty)
        val nonNulls = rows.dropWhile(_.bonus.isEmpty)
        nullsFirst.map(_.name) should contain theSameElementsAs Seq("bob", "dave")
        nonNulls.map(_.name) shouldBe Seq("alice", "carol")
        log.info(s"NULLS FIRST order: ${rows.map(_.name).mkString(", ")}")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "default to nulls last for ASC when ordering is unspecified" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus ASC"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        rows.take(2).map(_.name) shouldBe Seq("carol", "alice")
        rows.drop(2).flatMap(_.bonus) shouldBe empty

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  // -- a sort over a column some indices of a pattern do not map ----------------------------------

  /** A field sort over a column some indices of a pattern or list do not map answers those indices'
    * documents, sorted as having no value.
    *
    * Without an `unmapped_type`, Elasticsearch fails every shard of an index that does not map the
    * sort column (`No mapping found for [name] in order to sort on`), whether or not that index
    * holds documents; the search still answers HTTP 200 with the other indices' hits. So an EMPTY
    * member used to be harmless with a LIMIT and failed the paged route, and a POPULATED member's
    * documents were missing from the answer. The sort now carries the column's type, read from the
    * schema of the pattern.
    *
    * Fixture, 3 shards each: `sort_unm_e_a` and `sort_unm_p_a` map every column and hold six
    * documents whose names sort in the REVERSE of their ids (`a1` is `n6`), so an answer in id
    * order is an unsorted one; `sort_unm_e_b` has no mapping and no document; `sort_unm_p_b` maps
    * `id` only and holds `p1` and `p2`, which carry no other field.
    */
  private val unmappedIndices = Seq("sort_unm_e_a", "sort_unm_e_b", "sort_unm_p_a", "sort_unm_p_b")

  implicit val conversionContext: ConversionContext = NativeContext

  private val threeShards = """{"number_of_shards": 3, "number_of_replicas": 0}"""

  private val fullMapping =
    """{"properties": {"id": {"type": "keyword"}, "name": {"type": "keyword"},
      | "num": {"type": "long"}, "amt": {"type": "double"}, "ts": {"type": "date"},
      | "flag": {"type": "boolean"}}}""".stripMargin

  /** `a1` .. `a6`: name, num, amt and ts all DESCEND with the id; flag is true for even ids. */
  private val fullDocs: List[String] = (1 to 6).toList.map { i =>
    val r = 7 - i
    s"""{"id":"a$i","name":"n$r","num":${r * 10},"amt":${r * 1.5},"ts":"2025-01-0${r}T00:00:00Z",""" +
    s""""flag":${i % 2 == 0}}"""
  }

  /** The ids in the order every ascending sort over a populated column answers them. */
  private val sortedIds: Seq[String] = (6 to 1 by -1).map(i => s"a$i")

  private def index(name: String, mapping: Option[String], docs: List[String]): Unit = {
    client.createIndex(name, settings = threeShards).get shouldBe true
    mapping.foreach(m => client.setMapping(name, m).get shouldBe true)
    if (docs.nonEmpty) {
      implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = name, logEvery = 10)
      implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
        Source.fromIterator(() => list.iterator)
      client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
        case ElasticSuccess(_) => // ok
        case ElasticFailure(error) =>
          error.cause.foreach(_.printStackTrace())
          fail(s"Bulk indexing into $name failed: ${error.message}")
      }
    }
    client.refresh(name)
  }

  private lazy val unmappedFixture: Unit = {
    index("sort_unm_e_a", Some(fullMapping), fullDocs)
    index("sort_unm_e_b", None, Nil)
    index("sort_unm_p_a", Some(fullMapping), fullDocs)
    index(
      "sort_unm_p_b",
      Some("""{"properties": {"id": {"type": "keyword"}}}"""),
      List("""{"id":"p1"}""", """{"id":"p2"}""")
    )
  }

  private def idOf(row: ListMap[String, Any]): String = row("id").toString

  private def valueOf(row: ListMap[String, Any], column: String): Option[Any] =
    row.get(column).flatMap(Option(_)).flatMap {
      case s: Seq[_] => s.headOption
      case other     => Some(other)
    }

  /** The rows of a LIMITed statement (the one-shot search door). */
  private def searched(sql: String): Seq[ListMap[String, Any]] =
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}\n$sql")
    }

  /** The rows of an un-LIMITed statement, read page by page (PIT / search_after / scroll). */
  private def scrolled(sql: String, pageSize: Int = 1000): Seq[ListMap[String, Any]] =
    Await.result(
      client
        .scroll(SelectStatement(sql), ScrollConfig(scrollSize = pageSize))
        .map(_._1)
        .runWith(Sink.seq),
      2.minutes
    )

  /** The populated member's six rows in sort order, then the two rows of `sort_unm_p_b`, which
    * carry no value for the column.
    */
  private def assertSortedThenNulls(rows: Seq[ListMap[String, Any]], column: String): Unit =
    withClue(rows.mkString("; ")) {
      rows.map(idOf).take(6) shouldBe sortedIds
      rows.drop(6).map(idOf).toSet shouldBe Set("p1", "p2")
      rows.drop(6).flatMap(valueOf(_, column)) shouldBe empty
    }

  "ORDER BY a column an empty index of the pattern does not map" should "answer every row, sorted" in {
    unmappedFixture
    val rows = searched("SELECT id, name FROM sort_unm_e_* ORDER BY name LIMIT 100")
    rows.map(idOf) shouldBe sortedIds
  }

  it should "answer every row, sorted, without a LIMIT (read page by page)" in {
    unmappedFixture
    val rows = scrolled("SELECT id, name FROM sort_unm_e_* ORDER BY name")
    rows.map(idOf) shouldBe sortedIds
  }

  it should "answer the same rows as the populated index alone" in {
    unmappedFixture
    searched("SELECT id, name FROM sort_unm_e_a ORDER BY name LIMIT 100")
      .map(idOf) shouldBe sortedIds
  }

  it should "answer every row over a function of the column too" in {
    unmappedFixture
    // A script sort carries no key: an EMPTY index that does not map the column holds no
    // document for the script to read, so its shards do not fail.
    searched("SELECT id FROM sort_unm_e_* ORDER BY UPPER(name) LIMIT 100")
      .map(idOf) shouldBe sortedIds
  }

  "ORDER BY a column a populated index of the pattern does not map" should "answer that index's rows last, as NULL" in {
    unmappedFixture
    assertSortedThenNulls(
      searched("SELECT id, name FROM sort_unm_p_* ORDER BY name LIMIT 100"),
      "name"
    )
  }

  it should "answer them first with DESC NULLS FIRST" in {
    unmappedFixture
    val rows =
      searched("SELECT id, name FROM sort_unm_p_* ORDER BY name DESC NULLS FIRST LIMIT 100")
    withClue(rows.mkString("; ")) {
      rows.take(2).map(idOf).toSet shouldBe Set("p1", "p2")
      rows.drop(2).map(idOf) shouldBe sortedIds.reverse
    }
  }

  it should "answer them last when the pages cross the rows without a value" in {
    unmappedFixture
    // Three rows per page: the last pages are read after a row that has no sort value.
    assertSortedThenNulls(
      scrolled("SELECT id, name FROM sort_unm_p_* ORDER BY name", pageSize = 3),
      "name"
    )
  }

  it should "sort a date, a long, a double and a boolean column the same way" in {
    unmappedFixture
    Seq("ts", "num", "amt").foreach { column =>
      val sql = s"SELECT id, $column FROM sort_unm_p_* ORDER BY $column"
      withClue(s"$sql LIMIT 100: ") {
        assertSortedThenNulls(searched(s"$sql LIMIT 100"), column)
      }
      withClue(s"$sql (paged): ") {
        assertSortedThenNulls(scrolled(sql, pageSize = 3), column)
      }
    }
    val flagSql = "SELECT id, flag FROM sort_unm_p_* ORDER BY flag"
    Seq(searched(s"$flagSql LIMIT 100"), scrolled(flagSql, pageSize = 3)).foreach { rows =>
      withClue(rows.mkString("; ")) {
        rows.map(idOf).take(3).toSet shouldBe Set("a1", "a3", "a5")
        rows.map(idOf).slice(3, 6).toSet shouldBe Set("a2", "a4", "a6")
        rows.drop(6).map(idOf).toSet shouldBe Set("p1", "p2")
      }
    }
  }

  "ORDER BY a function of a column a populated index does not map" should "answer an error, not the other indices' rows" in {
    unmappedFixture
    // A script sort carries no key, and the script cannot read the column on the shards of
    // `sort_unm_p_b` that hold a document: those shards fail, and the search answers an error
    // instead of the six rows of `sort_unm_p_a`.
    val sql = "SELECT id FROM sort_unm_p_* ORDER BY UPPER(name) LIMIT 100"
    client.search(SelectStatement(sql)) match {
      case ElasticFailure(error) =>
        withClue(error.message) {
          "[12] of 6 shards failed".r.findFirstIn(error.message) should not be empty
        }
      case ElasticSuccess(response) =>
        fail(
          s"a search on which a shard failed answered ${response.results.size} row(s) with no " +
          s"error: $sql"
        )
    }
  }

  "a UNION ALL leg sorted over a column some indices do not map" should "answer every row of both legs" in {
    unmappedFixture
    val leg1 = "SELECT id FROM sort_unm_e_a WHERE name = 'n1' LIMIT 10"
    searched(s"$leg1 UNION ALL SELECT id FROM sort_unm_e_* ORDER BY name LIMIT 10")
      .map(idOf) shouldBe ("a6" +: sortedIds)
    val rows = searched(s"$leg1 UNION ALL SELECT id FROM sort_unm_p_* ORDER BY name LIMIT 10")
    withClue(rows.mkString("; ")) {
      rows.map(idOf).take(7) shouldBe ("a6" +: sortedIds)
      rows.drop(7).map(idOf).toSet shouldBe Set("p1", "p2")
    }
  }

}

case class SortNullsRow(name: String, bonus: Option[Int] = None)
