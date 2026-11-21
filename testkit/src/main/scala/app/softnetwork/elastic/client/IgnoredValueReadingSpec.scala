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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{
  DdlResult,
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.`type`.ValueCoercion
import app.softnetwork.persistence.generateUUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** SCHEMA-17 fix R-172 -- a value Elasticsearch ignored as malformed (`ignore_malformed`: the hit's
  * `_ignored` names its field) reads as NULL for every kind that takes the option, as a date
  * already did (SCHEMA-2 D8); a keyword or a wildcard over `ignore_above`, which `_ignored` names
  * too, keeps its text. On a real cluster, one document per value, beside a keyword and a number
  * Elasticsearch indexed:
  *
  *   - the premises: every kind takes `ignore_malformed` (a `boolean` only where the major maps the
  *     option), every document is indexed, and the raw hit's `_ignored` names each field holding a
  *     value Elasticsearch could not parse -- a canary, so no row below passes empty;
  *   - such a value is NULL in its column in `SELECT *`, a named column, a WHERE-filtered and a
  *     scrolled (unLIMITed) statement, the document's other cells intact; a value Elasticsearch
  *     indexed keeps its reading;
  *   - an `ARRAY<X>` column declared in place keeps each element Elasticsearch indexed and answers
  *     the others NULL;
  *   - an `integer` holding 3000000000 and a `float` holding 1e39 are NULL, never a wrapped number
  *     through `ValueCoercion`;
  *   - `_ignored` naming an object's sub-field never NULLs the object (the key matches exactly);
  *   - SCHEMA-17 fix R-177: a number Elasticsearch indexed that core answers as stored outside its
  *     column's type -- an `INT` declared over a `long` field, a computed `INT` past the int range,
  *     an object's sub-field -- fails through `ValueCoercion` naming the value, never wraps.
  *
  * Elasticsearch 7.17 and later (R-176): 6.8 rejects an out-of-range `short` / `integer` / `long`
  * document outright; this spec has no 6.8 runner. A `scaled_float` Elasticsearch refused is named
  * in `_ignored` only from 8.6 (A-206): 6.8, 7.x and 8.0 to 8.5 index it and name nothing, so there
  * core has no verdict and answers it as stored (D6), which the spec asserts there in place of NULL
  * (`namesScaledFloat`). Documents are indexed raw: the core client's `index` rewrites an
  * `unsigned_long` above 2^63-1.
  */
trait IgnoredValueReadingSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val mapper = new ObjectMapper()

  protected val table = "r172_t"

  /** SCHEMA-17 fix R-177 -- a table core creates, `w` declared `INT` over a `long` field, `p` and
    * `q` two `INT` columns: one document, holding 3000000000 in `w` and 2000000000 in `p` and `q`.
    */
  protected val wide = "r172_w"

  /** SCHEMA-17 fix R-172 (A-206) -- whether this major names a `scaled_float` it refused in the
    * hit's `_ignored`: from 8.6 on (`ScaledFloatFieldMapper` calls `addIgnoredField` from 8.6.0).
    * Before, the refused value is indexed unnamed and keeps its as-stored reading; every other kind
    * is named from 7.17 on.
    */
  protected lazy val namesScaledFloat: Boolean =
    ElasticsearchVersion.isAtLeast(elasticVersion, 8, 6)

  /** The fields whose refused values this major does not name in `_ignored` (A-206). */
  private lazy val unnamed: Set[String] = if (namesScaledFloat) Set.empty else Set("sf")

  /** A kind `ignore_malformed` applies to: its field, its Elasticsearch type, a value Elasticsearch
    * cannot parse into it, a value out of its range where it has one, a valid value and its read.
    */
  protected final case class Kind(
    field: String,
    esType: String,
    malformed: String,
    outOfRange: Option[String],
    valid: String,
    read: Any,
    options: String = ""
  ) {
    def mapping: String = s"""{"type":"$esType","ignore_malformed":true$options}"""
  }

  /** A string kind over `ignore_above`: Elasticsearch names it in `_ignored`, and keeps it. */
  protected final case class Capped(field: String, esType: String)

  /** A field declared `ARRAY<X>` in place, holding values Elasticsearch indexed and values it
    * ignored, and the list it reads.
    */
  protected final case class Listed(
    field: String,
    esType: String,
    declared: String,
    values: String,
    read: Seq[Any]
  )

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  protected val point: ListMap[String, Any] = ListMap("lat" -> 1.5, "lon" -> 2.5)

  /** A text a geo_point cannot read: `x` alone would be a geohash. */
  private val notAPoint = "\"not a point\""

  protected lazy val kinds: Seq[Kind] = Seq(
    Kind("tb", "byte", "\"x\"", Some("200"), "7", Integer.valueOf(7)),
    Kind("sh", "short", "\"x\"", Some("40000"), "7", Integer.valueOf(7)),
    Kind("i", "integer", "\"x\"", Some("3000000000"), "7", Integer.valueOf(7)),
    Kind("l", "long", "\"x\"", Some("100000000000000000000"), "7", java.lang.Long.valueOf(7L)),
    Kind("u", "unsigned_long", "\"x\"", Some("-1"), "7", java.lang.Long.valueOf(7L)),
    Kind("r", "float", "\"x\"", Some("1e39"), "1.5", java.lang.Float.valueOf(1.5f)),
    Kind("hf", "half_float", "\"x\"", Some("65520"), "0.5", java.lang.Float.valueOf(0.5f)),
    Kind("x", "double", "\"x\"", Some("\"1e309\""), "2.5", java.lang.Double.valueOf(2.5)),
    Kind(
      "sf",
      "scaled_float",
      "\"x\"",
      Some("\"1e309\""),
      "1.25",
      java.lang.Double.valueOf(1.25),
      options = ""","scaling_factor":100"""
    ),
    Kind("ip", "ip", "\"x\"", None, "\"10.0.0.1\"", "10.0.0.1"),
    Kind(
      "g",
      "geo_point",
      notAPoint,
      Some("""{"lat":100.0,"lon":200.0}"""),
      """{"lat":1.5,"lon":2.5}""",
      point
    ),
    Kind("b", "boolean", "\"x\"", None, "true", java.lang.Boolean.TRUE),
    Kind("dm", "date", "\"not-a-date\"", None, "\"2024-01-31T10:30:00Z\"", utc(2024, 1, 31, 10, 30))
  )

  protected lazy val capped: Seq[Capped] = Seq(Capped("kw", "keyword"), Capped("wc", "wildcard"))

  /** The text a capped string holds, over its `ignore_above` of 3. */
  protected val cappedText = "abcdefgh"

  protected lazy val listed: Seq[Listed] = Seq(
    Listed("la", "long", "ARRAY<BIGINT>", """[1, "x", 3]""", Seq(1L, null, 3L)),
    Listed("ia", "integer", "ARRAY<INT>", """[1, 3000000000, "y"]""", Seq(1, null, null)),
    Listed("ra", "float", "ARRAY<REAL>", """[1.5, 1e39, "x"]""", Seq(1.5f, null, null)),
    Listed(
      "hfa",
      "half_float",
      "ARRAY<REAL>",
      """[65519, 65520, "x"]""",
      Seq(65519.0f, null, null)
    ),
    Listed(
      "ua",
      "unsigned_long",
      "ARRAY<BIGINT>",
      """[-1, 18446744073709551615, 5]""",
      Seq(null, new java.math.BigInteger("18446744073709551615"), 5L)
    ),
    Listed("ipa", "ip", "ARRAY<KEYWORD>", """["10.0.0.1", "x"]""", Seq("10.0.0.1", null)),
    Listed(
      "ga",
      "geo_point",
      "ARRAY<GEO_POINT>",
      s"""[{"lat":1.5,"lon":2.5}, $notAPoint]""",
      Seq(point, null)
    ),
    Listed(
      "da",
      "date",
      "ARRAY<TIMESTAMP>",
      """["2024-01-31T10:30:00Z", "bad"]""",
      Seq(utc(2024, 1, 31, 10, 30), null)
    )
  )

  // the fixture, as this major answered it (filled once, by `beforeAll`)

  /** Each field's mapping PUT: accepted, or Elasticsearch's refusal. */
  protected val mappings =
    scala.collection.mutable.LinkedHashMap.empty[String, Either[String, Unit]]

  /** Each document's indexing: indexed, or Elasticsearch's rejection. */
  protected val indexing =
    scala.collection.mutable.LinkedHashMap.empty[String, Either[String, Unit]]

  /** Each in-place `ALTER` declaring a list: done, or its failure. */
  protected val alters = scala.collection.mutable.LinkedHashMap.empty[String, Either[String, Unit]]

  /** The paths each document's raw hit `_ignored` names. */
  protected val ignoredBy = scala.collection.mutable.LinkedHashMap.empty[String, Set[String]]

  /** The one field each document holds, beside `id`, `k` and `ok`. */
  protected val fieldOf = scala.collection.mutable.LinkedHashMap.empty[String, String]

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(client.deleteIndex(index))
  }

  private def ddl(sql: String): Either[String, Unit] =
    Try(Await.result(client.run(sql), 120.seconds)) match {
      case Success(ElasticSuccess(DdlResult(true, _))) => Right(())
      case Success(ElasticFailure(error))              => Left(error.message)
      case Success(other)                              => Left(String.valueOf(other))
      case Failure(t) => Left(s"${t.getClass.getSimpleName}: ${t.getMessage}")
    }

  private def mapField(field: String, mapping: String): Boolean = {
    val outcome: Either[String, Unit] =
      Try(client.setMapping(table, s"""{"properties": {"$field": $mapping}}""")) match {
        case Success(ElasticSuccess(true))  => Right(())
        case Success(ElasticFailure(error)) => Left(error.message)
        case Success(other)                 => Left(String.valueOf(other))
        case Failure(t)                     => Left(s"${t.getClass.getSimpleName}: ${t.getMessage}")
      }
    mappings.put(field, outcome)
    outcome.isRight
  }

  /** `json` indexed raw as the document `id` of `index`, its outcome recorded in [[indexing]]. */
  private def putRaw(index: String, id: String, json: String): Unit = {
    val request = new Request("PUT", s"/$index/_doc/$id")
    request.setEntity(new NStringEntity(json, ContentType.APPLICATION_JSON))
    indexing.put(
      id,
      Try(restClient.performRequest(request)) match {
        case Success(_) => Right(())
        case Failure(t) => Left(s"${t.getClass.getSimpleName}: ${t.getMessage}")
      }
    )
    ()
  }

  private def putDoc(id: String, field: String, value: String): Unit = {
    fieldOf.put(id, field)
    putRaw(table, id, s"""{"id":"$id","k":"keep","ok":5,"$field":$value}""")
  }

  /** The `CREATE TABLE` of [[wide]]: done, or its failure. */
  protected var wideCreated: Either[String, Unit] = Left("not run")

  override def beforeAll(): Unit = {
    super.beforeAll()
    drop(wide)
    wideCreated = ddl(
      s"CREATE TABLE $wide (id KEYWORD, w INT OPTIONS (type = 'long'), p INT, q INT)"
    )
    if (wideCreated.isRight) {
      putRaw(wide, "w1", """{"id":"w1","w":3000000000,"p":2000000000,"q":2000000000}""")
      client.refresh(wide)
    }
    drop(table)
    client
      .createIndex(table, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client
      .setMapping(
        table,
        """{"properties": {"id": {"type": "keyword"}, "k": {"type": "keyword"},
          | "ok": {"type": "long"}}}""".stripMargin
      )
      .get shouldBe true
    // every kind is tried on every major: a type this major does not map, or that refuses the
    // option, is recorded and holds no document
    kinds.foreach { k =>
      if (mapField(k.field, k.mapping)) {
        putDoc(s"${k.field}_m", k.field, k.malformed)
        k.outOfRange.foreach(putDoc(s"${k.field}_o", k.field, _))
        putDoc(s"${k.field}_v", k.field, k.valid)
      }
    }
    capped.foreach { c =>
      if (mapField(c.field, s"""{"type":"${c.esType}","ignore_above":3}""")) {
        putDoc(s"${c.field}_m", c.field, s""""$cappedText"""")
        putDoc(s"${c.field}_v", c.field, "\"ab\"")
      }
    }
    listed.foreach { l =>
      if (mapField(l.field, s"""{"type":"${l.esType}","ignore_malformed":true}"""))
        putDoc(s"${l.field}_a", l.field, l.values)
    }
    if (
      mapField(
        "obj",
        """{"properties": {"n": {"type": "long", "ignore_malformed": true},
          | "i": {"type": "integer", "ignore_malformed": true}}}""".stripMargin
      )
    ) {
      putDoc("obj_n", "obj", """{"n":"x"}""")
      putDoc("obj_i", "obj", """{"i":3000000000}""")
    }
    // the lists are declared once every field is mapped: the first statement caches the table's
    // schema, which a field mapped after it would be missing from
    listed.foreach { l =>
      if (mappings.get(l.field).exists(_.isRight))
        alters.put(
          l.field,
          ddl(s"ALTER TABLE $table ALTER COLUMN ${l.field} SET DATA TYPE ${l.declared}")
        )
    }
    client.refresh(table)
    // the raw hits' `_ignored`, as Elasticsearch answers them
    val response = restClient.performRequest(new Request("GET", s"/$table/_search?size=1000"))
    val hits = mapper.readTree(EntityUtils.toString(response.getEntity)).path("hits").path("hits")
    val it = hits.elements()
    while (it.hasNext) {
      val hit = it.next()
      val names = Set.newBuilder[String]
      val ignored = hit.path("_ignored").elements()
      while (ignored.hasNext) names += ignored.next().asText()
      ignoredBy.put(hit.path("_id").asText(), names.result())
    }
  }

  override def afterAll(): Unit = {
    drop(table)
    drop(wide)
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // Results
  // -------------------------------------------------------------------------------------------

  private def answer(result: QueryResult): Seq[ListMap[String, Any]] =
    result match {
      case q: QueryRows       => q.rows
      case q: QueryStructured => q.response.results
      case q: QueryStream     => Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case other              => fail(s"unexpected result $other")
    }

  /** The statement's outcome: its rows, or its failure's message -- a failed stream included. */
  protected def outcome(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Try(Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => Right(answer(result))
      case ElasticFailure(error)  => Left(error.message)
    }).toEither.left
      .map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")
      .joinRight

  protected def rowsOf(sql: String): Seq[ListMap[String, Any]] =
    outcome(sql).fold(error => fail(s"[$sql] failed: $error"), identity)

  private def one(sql: String): ListMap[String, Any] =
    rowsOf(sql) match {
      case Seq(row) => row
      case other    => fail(s"[$sql] answered ${other.size} rows: $other")
    }

  private def byId(rows: Seq[ListMap[String, Any]]): Map[String, ListMap[String, Any]] =
    rows.map(r => String.valueOf(r.getOrElse("id", "?")) -> r).toMap

  private def indexed(doc: String): Boolean = indexing.get(doc).exists(_.isRight)

  /** The (document, field) pairs whose one value Elasticsearch ignored, by the raw hit. */
  private def ignoredCells(fields: Set[String]): Seq[(String, String)] =
    fieldOf.toSeq.collect {
      case (doc, field) if fields.contains(field) && ignoredBy.get(doc).exists(_.contains(field)) =>
        doc -> field
    }

  private def intact(row: ListMap[String, Any], doc: String): Unit = {
    row.get("id") shouldBe Some(doc)
    row.get("k") shouldBe Some("keep")
    row.get("ok") shouldBe Some(5L)
  }

  // -------------------------------------------------------------------------------------------
  // Premises (T0-1)
  // -------------------------------------------------------------------------------------------

  "The fixture" should "hold every premise of R-172 on this major (T0-1)" in {
    // every type takes `ignore_malformed` -- a boolean only where this major maps the option
    kinds.filter(_.esType != "boolean").foreach { k =>
      withClue(s"${k.field} (${k.esType}): ")(mappings.get(k.field) shouldBe Some(Right(())))
    }
    capped.foreach { c =>
      withClue(s"${c.field} (${c.esType}): ")(mappings.get(c.field) shouldBe Some(Right(())))
    }
    listed.foreach { l =>
      withClue(s"${l.field} (${l.esType}): ") {
        mappings.get(l.field) shouldBe Some(Right(()))
        alters.get(l.field) shouldBe Some(Right(()))
      }
    }
    // Elasticsearch indexes every document holding a value it cannot parse, and names the field
    // in the hit's `_ignored` -- an object's sub-field by its path
    val cappedFields = capped.map(_.field).toSet
    val refused: Seq[(String, String)] =
      fieldOf.toSeq.collect {
        case (doc, field)
            if ((doc.endsWith("_m") && !cappedFields.contains(field)) || doc.endsWith("_a")) &&
              !unnamed.contains(field) =>
          doc -> field
      } ++ Seq("obj_n" -> "obj.n", "obj_i" -> "obj.i").filter { case (doc, _) =>
        fieldOf.contains(doc)
      }
    refused should not be empty
    refused.foreach { case (doc, field) =>
      withClue(s"$doc: ") {
        indexing.get(doc) shouldBe Some(Right(()))
        ignoredBy.getOrElse(doc, Set.empty[String]) should contain(field)
      }
    }
    // ...except a scaled_float before 8.6, indexed and named nowhere (A-206)
    unnamed.foreach { field =>
      Seq(s"${field}_m", s"${field}_o").foreach { doc =>
        withClue(s"$doc (unnamed before 8.6): ") {
          indexing.get(doc) shouldBe Some(Right(()))
          ignoredBy.getOrElse(doc, Set.empty[String]) should not contain field
        }
      }
    }
    // a string over `ignore_above` is indexed, and named (AC3)
    capped.filter(c => mappings.get(c.field).exists(_.isRight)).foreach { c =>
      val doc = s"${c.field}_m"
      withClue(s"$doc: ") {
        indexing.get(doc) shouldBe Some(Right(()))
        ignoredBy.getOrElse(doc, Set.empty[String]) should contain(c.field)
      }
    }
    // ...and every valid value, naming none
    fieldOf.keys.filter(_.endsWith("_v")).foreach { doc =>
      withClue(s"$doc: ") {
        indexing.get(doc) shouldBe Some(Right(()))
        ignoredBy.getOrElse(doc, Set.empty[String]) shouldBe empty
      }
    }
    // R-177: the table core created, and its one document
    wideCreated shouldBe Right(())
    indexing.get("w1") shouldBe Some(Right(()))
  }

  // -------------------------------------------------------------------------------------------
  // AC2 -- one value
  // -------------------------------------------------------------------------------------------

  "A value Elasticsearch ignored as malformed" should
  "read NULL in its column on every venue, its document's other cells intact (AC2)" in {
    val cells = ignoredCells(kinds.map(_.field).toSet)
    // a canary: at least the text each mapped kind cannot parse -- each kind this major names
    // (A-206: a scaled_float from 8.6 on, its unnamed reading asserted below)
    val canary = kinds
      .filter(k => mappings.get(k.field).exists(_.isRight) && !unnamed.contains(k.field))
      .map(_.field + "_m")
    cells.map(_._1) should contain allElementsOf canary
    // SELECT *, scrolled and LIMITed
    Seq(s"SELECT * FROM $table", s"SELECT * FROM $table LIMIT 1000").foreach { sql =>
      val rows = byId(rowsOf(sql))
      cells.foreach { case (doc, field) =>
        withClue(s"[$sql] $doc.$field: ") {
          val row = rows.getOrElse(doc, fail("no row"))
          row.getOrElse(field, null) shouldBe (null: Any)
          intact(row, doc)
        }
      }
    }
    // a named column, scrolled, LIMITed, filtered by WHERE
    cells.groupBy(_._2).foreach { case (field, byField) =>
      val named = s"SELECT id, k, ok, $field FROM $table"
      Seq(named, s"$named LIMIT 1000", s"$named WHERE ok = 5").foreach { sql =>
        val rows = byId(rowsOf(sql))
        byField.foreach { case (doc, _) =>
          withClue(s"[$sql] $doc: ") {
            val row = rows.getOrElse(doc, fail("no row"))
            row.getOrElse(field, null) shouldBe (null: Any)
            intact(row, doc)
          }
        }
      }
      byField.foreach { case (doc, _) =>
        withClue(s"[$named WHERE id = '$doc'] ") {
          one(s"$named WHERE id = '$doc'").getOrElse(field, null) shouldBe (null: Any)
        }
      }
    }
    // a value Elasticsearch indexed keeps its reading
    kinds.filter(k => indexed(s"${k.field}_v")).foreach { k =>
      withClue(s"${k.field}_v: ") {
        one(s"SELECT id, ${k.field} FROM $table WHERE id = '${k.field}_v'")(k.field) shouldBe k.read
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // A-206 -- a scaled_float, named in _ignored from Elasticsearch 8.6 on
  // -------------------------------------------------------------------------------------------

  /** `sf`'s two refused values, each with the reading it keeps where the major names neither (T0-1
    * on 6.8.23: the text as stored; the non-finite text read as a `Double` infinity).
    */
  private val scaledFloatAsStored: Seq[(String, Any)] =
    Seq("sf_m" -> "x", "sf_o" -> java.lang.Double.valueOf(Double.PositiveInfinity))

  "A scaled_float Elasticsearch refused" should
  "read NULL where _ignored names it (8.6+), and as stored where it does not (A-206)" in {
    val star = byId(rowsOf(s"SELECT * FROM $table"))
    scaledFloatAsStored.foreach { case (doc, asStored) =>
      val expected: Any = if (namesScaledFloat) null else asStored
      withClue(s"$doc (named: $namesScaledFloat on $elasticVersion): ") {
        indexing.get(doc) shouldBe Some(Right(()))
        val names = ignoredBy.getOrElse(doc, Set.empty[String])
        if (namesScaledFloat) names should contain("sf") else names should not contain "sf"
        one(s"SELECT id, sf FROM $table WHERE id = '$doc'").getOrElse("sf", null) shouldBe expected
        val row = star.getOrElse(doc, fail("no row"))
        row.getOrElse("sf", null) shouldBe expected
        intact(row, doc)
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC3 -- a string over ignore_above
  // -------------------------------------------------------------------------------------------

  "A string over ignore_above" should "keep its text, though the hit's _ignored names it (AC3)" in {
    val docs = capped.filter(c => indexed(s"${c.field}_m"))
    docs.map(_.field) should contain("kw")
    val star = byId(rowsOf(s"SELECT * FROM $table"))
    docs.foreach { c =>
      val doc = s"${c.field}_m"
      withClue(s"$doc: ") {
        // named: the canary that the text is kept DESPITE it
        ignoredBy.getOrElse(doc, Set.empty[String]) should contain(c.field)
        one(s"SELECT id, ${c.field} FROM $table WHERE id = '$doc'")(c.field) shouldBe cappedText
        star.getOrElse(doc, fail("no row")).getOrElse(c.field, null) shouldBe cappedText
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC2 -- a declared list
  // -------------------------------------------------------------------------------------------

  "An ARRAY<X> column declared in place" should
  "keep each element Elasticsearch indexed and answer the others NULL (AC2, A-202)" in {
    val docs = listed.filter(l => indexed(s"${l.field}_a"))
    docs.map(_.field) should contain allElementsOf Seq("la", "ia", "ra")
    val star = byId(rowsOf(s"SELECT * FROM $table"))
    docs.foreach { l =>
      val doc = s"${l.field}_a"
      withClue(s"$doc: ") {
        one(s"SELECT id, ${l.field} FROM $table WHERE id = '$doc'")(l.field) shouldBe l.read
        star.getOrElse(doc, fail("no row")).getOrElse(l.field, null) shouldBe l.read
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC4 -- the SILENT case
  // -------------------------------------------------------------------------------------------

  "An out-of-range number Elasticsearch ignored" should
  "read NULL, never a wrapped number through the coercions (AC4)" in {
    // an `integer` of 3000000000
    indexing.get("i_o") shouldBe Some(Right(()))
    ignoredBy.getOrElse("i_o", Set.empty[String]) should contain("i")
    val i = one(s"SELECT id, i FROM $table WHERE id = 'i_o'").getOrElse("i", null)
    withClue(s"i = $i (${Option(i).map(_.getClass.getName).orNull}): ") {
      ValueCoercion.coerceToInt(i) shouldBe (null: java.lang.Integer)
      i shouldBe (null: Any)
    }
    // a `float` of 1e39
    indexing.get("r_o") shouldBe Some(Right(()))
    ignoredBy.getOrElse("r_o", Set.empty[String]) should contain("r")
    val r = one(s"SELECT id, r FROM $table WHERE id = 'r_o'").getOrElse("r", null)
    withClue(s"r = $r (${Option(r).map(_.getClass.getName).orNull}): ") {
      ValueCoercion.coerceToFloat(r) shouldBe (null: java.lang.Float)
      r shouldBe (null: Any)
    }
  }

  // -------------------------------------------------------------------------------------------
  // A-202 -- the key matches exactly
  // -------------------------------------------------------------------------------------------

  "A sub-field Elasticsearch ignored" should "never NULL its parent object (A-202)" in {
    indexing.get("obj_n") shouldBe Some(Right(()))
    ignoredBy.getOrElse("obj_n", Set.empty[String]) should contain("obj.n")
    one(s"SELECT id, obj FROM $table WHERE id = 'obj_n'").getOrElse("obj", null) match {
      case m: scala.collection.Map[_, _] =>
        m.asInstanceOf[scala.collection.Map[String, Any]].keySet should contain("n")
      case other => fail(s"obj answered $other")
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC7 (R-177) -- a number Elasticsearch indexed, outside its column's type
  // -------------------------------------------------------------------------------------------

  /** `cell` is the number `expected` -- as stored, outside INT -- and `coerceToInt` fails naming it
    * where it wrapped it (T0-2: 3000000000 read -1294967296).
    */
  private def failsAsInt(cell: Any, expected: Double): Unit =
    withClue(s"$cell (${Option(cell).map(_.getClass.getName).orNull}): ") {
      cell match {
        case n: Number => n.doubleValue() shouldBe expected
        case other     => fail(s"not a number: $other")
      }
      val error = the[java.sql.SQLException] thrownBy ValueCoercion.coerceToInt(cell)
      error.getMessage should (startWith("Value ") and endWith(" is out of range for INT"))
    }

  "A number outside its column's type that Elasticsearch indexed" should
  "fail through the coercions naming it, never wrap (AC7, R-177)" in {
    wideCreated shouldBe Right(())
    indexing.get("w1") shouldBe Some(Right(()))
    // a column declared INT over a `long` field (T0-2 a)
    failsAsInt(one(s"SELECT id, w FROM $wide WHERE id = 'w1'").getOrElse("w", null), 3e9)
    // a computed INT past the int range (T0-2 b)
    failsAsInt(one(s"SELECT id, p + q AS s FROM $wide WHERE id = 'w1'").getOrElse("s", null), 4e9)
    // an object's sub-field, read as a dotted column: `_ignored` names it, but D8 reads a
    // top-level field only (A-202)
    indexing.get("obj_i") shouldBe Some(Right(()))
    failsAsInt(
      one(s"SELECT id, obj.i FROM $table WHERE id = 'obj_i'").getOrElse("obj.i", null),
      3e9
    )
    // a value within the range converts as it did
    ValueCoercion.coerceToInt(one(s"SELECT id, i FROM $table WHERE id = 'i_v'")("i")) shouldBe 7
  }
}
