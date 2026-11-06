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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.StringValue
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.operator.AND
import app.softnetwork.elastic.sql.query.{Criteria, MatchNoneCriteria, Predicate, SingleSearch}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** The schema an index PATTERN or a comma list answers with (`mergedSchema`): the union of the
  * matched indices' columns, merged FIELD BY FIELD -- a field the indices map alike keeps its type;
  * a field they map differently takes the type the set operations give the two (SCHEMA-7 D6, the
  * #420 lattice: `MultiSearch.latticeType`) when every index's value reads as a value of it, else
  * it is left untyped (`Any`), so nothing is refused on it and Elasticsearch answers for it, while
  * every other field is typed as over a single index.
  */
class MergedSchemaSpec extends AnyFlatSpec with Matchers {

  private def index(name: String, properties: String): (String, Table) =
    name -> Index(name, s"""{"mappings":{"properties":{$properties}},"settings":{}}""").schema

  "indices that agree" should "answer the union of their columns" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"g":{"type":"keyword"}"""),
        index("logs-2", """"ts":{"type":"date"},"n":{"type":"long"}""")
      )
    )
    disagreements shouldBe empty
    merged.name shouldBe "logs-*"
    merged.columns.map(c => c.name -> c.dataType) should contain theSameElementsAs Seq(
      "g" -> SQLTypes.Keyword,
      "n" -> SQLTypes.BigInt,
      // a `date` nobody declared is a TIMESTAMP at the declaration seam, and stays one merged
      "ts" -> SQLTypes.Timestamp
    )
  }

  it should "agree on a TIMESTAMP one index declares and another holds undeclared" in {
    val declared = Index(
      "logs-1",
      """{"mappings":{"_meta":{"columns":{"ts":{"data_type":"TIMESTAMP","not_null":"false"}}},""" +
      """"properties":{"ts":{"type":"date"}}},"settings":{}}"""
    ).schema
    val (merged, disagreements) =
      mergedSchema("logs-*", Seq("logs-1" -> declared, index("logs-2", """"ts":{"type":"date"}""")))
    disagreements shouldBe empty
    merged.find("ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
  }

  "indices that disagree on a field" should "type it as the set operations type the two, and type every other" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"c":{"type":"date"},"g":{"type":"keyword"}"""),
        index("logs-2", """"ts":{"type":"date"},"c":{"type":"keyword"},"n":{"type":"long"}""")
      )
    )
    // SCHEMA-7 (D6, R-84): a keyword beside a date is a VARCHAR (DuckDB's lattice), as the keyword
    // index maps it -- a KEYWORD, typed by the single-index rules
    disagreements shouldBe empty
    merged.columns.map(c => c.name -> c.dataType) should contain theSameElementsAs Seq(
      "c"  -> SQLTypes.Keyword,
      "g"  -> SQLTypes.Keyword,
      "n"  -> SQLTypes.BigInt,
      "ts" -> SQLTypes.Timestamp
    )
  }

  "indices that map a field with two number types" should
  "give it the wider one, as DuckDB types the column" in {
    // DuckDB 1.5.5 (UNION ALL BY NAME of the two types): the later one of
    // TINYINT < SMALLINT < INTEGER < BIGINT < FLOAT < DOUBLE
    Seq(
      ("integer", "long")  -> SQLTypes.BigInt,
      ("long", "integer")  -> SQLTypes.BigInt,
      ("byte", "short")    -> SQLTypes.SmallInt,
      ("short", "integer") -> SQLTypes.Int,
      ("float", "double")  -> SQLTypes.Double,
      ("double", "float")  -> SQLTypes.Double,
      ("integer", "float") -> SQLTypes.Real,
      ("long", "float")    -> SQLTypes.Real,
      ("long", "double")   -> SQLTypes.Double,
      ("byte", "double")   -> SQLTypes.Double
    ).foreach { case ((a, b), wider) =>
      withClue(s"[$a + $b] ") {
        val (merged, disagreements) = mergedSchema(
          "p_*",
          Seq(
            index("p_a", s""""v":{"type":"$a"},"g":{"type":"keyword"}"""),
            index("p_b", s""""v":{"type":"$b"}""")
          )
        )
        disagreements shouldBe empty
        merged.find("v").map(_.dataType) shouldBe Some(wider)
        merged.find("g").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
      }
    }
  }

  it should "type a keyword beside numbers as the keyword index maps it (SCHEMA-7 D6)" in {
    Seq(
      Seq("long", "keyword"),
      Seq("integer", "long", "keyword"),
      Seq("keyword", "integer", "long")
    ).foreach { types =>
      withClue(s"[${types.mkString(" + ")}] ") {
        val (merged, disagreements) = mergedSchema(
          "p_*",
          types.zipWithIndex.map { case (t, i) => index(s"p_$i", s""""v":{"type":"$t"}""") }
        )
        disagreements shouldBe empty
        merged.find("v").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
      }
    }
  }

  it should "leave a pair with no common type untyped, naming every type the indices map" in {
    Seq(
      Seq("long", "date")    -> "'v' (BIGINT, TIMESTAMP)",
      Seq("long", "boolean") -> "'v' (BIGINT, BOOLEAN)",
      // a member mapped with a type elasticsql does not write (SCHEMA-7 D2: `half_float` is a
      // REAL that keeps its own mapping type) stays untyped beside another: one mapping type
      Seq("half_float", "float") -> "'v' (REAL)"
    ).foreach { case (types, disagreement) =>
      withClue(s"[${types.mkString(" + ")}] ") {
        val (merged, disagreements) = mergedSchema(
          "p_*",
          types.zipWithIndex.map { case (t, i) => index(s"p_$i", s""""v":{"type":"$t"}""") }
        )
        disagreements shouldBe Seq(disagreement)
        merged.find("v").map(_.dataType) shouldBe Some(SQLTypes.Any)
      }
    }
  }

  it should "type an integer GROUP BY key over integer and long indices, so its key filter answers no group for 1.5" in {
    val pattern = mergedSchema(
      "mixed_*",
      Seq(
        index("mixed_int", """"id":{"type":"keyword"},"lval":{"type":"integer"}"""),
        index("mixed_long", """"id":{"type":"keyword"},"lval":{"type":"long"}""")
      )
    )._1
    def filtersNothingIn(c: Criteria): Boolean = c match {
      case _: MatchNoneCriteria          => true
      case Predicate(l, AND, r, None, _) => filtersNothingIn(l) || filtersNothingIn(r)
      case _                             => false
    }
    Parser(
      "SELECT lval, COUNT(*) AS c FROM mixed_int, mixed_long GROUP BY lval HAVING lval = 1.5"
    ) match {
      case Right(ss: SingleSearch) =>
        // DuckDB: no group -- no integer is equal to 1.5
        ss.update(Some(pattern)).where.flatMap(_.criteria).exists(filtersNothingIn) shouldBe true
      case other => fail(s"$other")
    }
  }

  it should "count a date's format as part of its mapping: a temporal literal is read against it" in {
    val withFormat = Table(
      "logs-2",
      columns = List(
        Column(
          "ts",
          SQLTypes.Timestamp,
          options = ListMap("format" -> StringValue("epoch_millis"))
        ),
        Column("g", SQLTypes.Keyword)
      )
    )
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"g":{"type":"keyword"}"""),
        "logs-2" -> withFormat
      )
    )
    disagreements shouldBe Seq("'ts' (TIMESTAMP)")
    merged.find("ts").map(_.dataType) shouldBe Some(SQLTypes.Any)
    merged.find("ts").flatMap(_.options.get("format")) shouldBe None
    merged.find("g").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  it should "merge sub-fields by the same rule, under their parent" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index(
          "logs-1",
          """"s":{"type":"text","fields":{"raw":{"type":"keyword"},"n":{"type":"long"}}}"""
        ),
        index(
          "logs-2",
          """"s":{"type":"text","fields":{"raw":{"type":"keyword"},"n":{"type":"keyword"},"w":{"type":"keyword"}}}"""
        )
      )
    )
    // SCHEMA-7 (D6): a keyword beside a long is a KEYWORD at every depth
    disagreements shouldBe empty
    merged.find("s").map(_.dataType) shouldBe Some(SQLTypes.Text)
    merged.find("s.raw").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    merged.find("s.w").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    merged.find("s.n").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  /** SCHEMA-7 P2: one field `v`, mapped one way per member. `meta` is the member's `_meta.columns`
    * declaration of `v`, if any.
    */
  private case class Member(mapping: String, meta: Option[String] = None)

  private val p2: ListMap[String, Member] = ListMap(
    "keyword"          -> Member("""{"type":"keyword"}"""),
    "text"             -> Member("""{"type":"text"}"""),
    "long"             -> Member("""{"type":"long"}"""),
    "integer"          -> Member("""{"type":"integer"}"""),
    "double"           -> Member("""{"type":"double"}"""),
    "boolean"          -> Member("""{"type":"boolean"}"""),
    "dateA"            -> Member("""{"type":"date"}"""),
    "dateB"            -> Member("""{"type":"date","format":"yyyy/MM/dd"}"""),
    "DATE"             -> Member("""{"type":"date"}""", Some("DATE")),
    "constant_keyword" -> Member("""{"type":"constant_keyword","value":"C"}"""),
    "ip"               -> Member("""{"type":"ip"}"""),
    "date_nanos"       -> Member("""{"type":"date_nanos"}"""),
    "object"           -> Member("""{"properties":{"a":{"type":"keyword"}}}"""),
    "nested"           -> Member("""{"type":"nested","properties":{"a":{"type":"keyword"}}}"""),
    "geo_point"        -> Member("""{"type":"geo_point"}""")
  )

  private def member(index: String, m: Member): (String, Table) = {
    val meta =
      m.meta.fold("")(t => s""""_meta":{"columns":{"v":{"data_type":"$t","not_null":"false"}}},""")
    index -> Index(
      index,
      s"""{"mappings":{$meta"properties":{"v":${m.mapping}}},"settings":{}}"""
    ).schema
  }

  private val foreign = Set("constant_keyword", "ip", "date_nanos")
  private val structural = Set("object", "nested", "geo_point")
  private val textual = Set("keyword", "text")
  private val numbers = Seq("integer", "long", "double") // narrowest first
  private val dates = Set("dateA", "dateB", "DATE")

  /** D6, written out per pair (`x` the member whose index name sorts first): the member type whose
    * column the merged field is, `None` for a field left untyped.
    */
  private def d6(x: String, y: String): Option[String] =
    if (x == y) Some(x)
    else if (foreign(x) || foreign(y) || structural(x) || structural(y)) None
    // a text type beside any other scalar: VARCHAR, as the first text member maps it
    else if (textual(x)) Some(x)
    else if (textual(y)) Some(y)
    // a BOOLEAN is kept as stored under a number; a date beside a number is refused
    else if (x == "boolean" || y == "boolean") None
    else if (numbers.contains(x) && numbers.contains(y))
      Some(if (numbers.indexOf(x) >= numbers.indexOf(y)) x else y)
    else if (dates(x) && dates(y))
      // a DATE beside a TIMESTAMP of the SAME format is the TIMESTAMP; two formats: untyped
      Set(x, y) match {
        case pair if pair == Set("dateA", "DATE") => Some("dateA")
        case _                                    => None
      }
    else None

  "every P2 pair (SCHEMA-7 D6)" should "take the lattice type as the deciding index maps it, or none" in {
    val single = p2.map { case (name, m) => name -> member("p_0", m)._2.find("v").get }
    for {
      (x, mx) <- p2
      (y, my) <- p2
    } withClue(s"[$x + $y] ") {
      val members = Seq(member("p_0", mx), member("p_1", my))
      val (merged, disagreements) = mergedSchema("p_*", members)
      val v = merged.find("v").get
      d6(x, y) match {
        case Some(decided) =>
          disagreements shouldBe empty
          v.dataType shouldBe single(decided).dataType
          v.mappingType shouldBe single(decided).mappingType
        case None =>
          disagreements should have size 1
          v.dataType shouldBe SQLTypes.Any
          v.options.get("type") shouldBe None
          v.options.get("format") shouldBe None
      }
      // the order Elasticsearch lists the indices in decides nothing
      mergedSchema("p_*", members.reverse) shouldBe ((merged, disagreements))
    }
  }

  it should "keep a field's lattice type when every member maps it the same foreign way" in {
    Seq("constant_keyword", "ip", "date_nanos").foreach { t =>
      withClue(s"[$t] ") {
        val (merged, disagreements) =
          mergedSchema("p_*", Seq(member("p_0", p2(t)), member("p_1", p2(t))))
        disagreements shouldBe empty
        merged.find("v").map(_.mappingType) shouldBe Some(t)
      }
    }
  }

  it should "leave two lists of two element types untyped, in both orders" in {
    // the list node's deciding operand is the first list, not the widest one: no member decides
    val ints = member("p_0", Member("""{"type":"integer"}""", Some("ARRAY<INT>")))
    val longs = member("p_1", Member("""{"type":"long"}""", Some("ARRAY<BIGINT>")))
    Seq(Seq(ints, longs), Seq(longs, ints)).foreach { members =>
      mergedSchema("p_*", members)._1.find("v").map(_.dataType) shouldBe Some(SQLTypes.Any)
    }
    mergedSchema(
      "p_*",
      Seq(ints, ("p_1", ints._2))
    )._1.find("v").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Int))
  }

  it should "keep a custom date format a DATE and a TIMESTAMP member share" in {
    val (merged, disagreements) = mergedSchema(
      "p_*",
      Seq(
        member("p_0", Member("""{"type":"date","format":"yyyy/MM/dd"}""", Some("DATE"))),
        member("p_1", Member("""{"type":"date","format":"yyyy/MM/dd"}"""))
      )
    )
    disagreements shouldBe empty
    merged.find("v").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
    merged.find("v").flatMap(_.options.get("format")) shouldBe Some(StringValue("yyyy/MM/dd"))
  }

  /** The pattern of `DateArithmeticSpec`: two indices that agree on every field but `c`. */
  private lazy val pattern: Schema =
    mergedSchema(
      "ev_*",
      Seq(
        index(
          "ev_a",
          """"g":{"type":"keyword"},"d":{"type":"date"},"d2":{"type":"date"},"ts":{"type":"date"},"n":{"type":"integer"},"c":{"type":"date"}"""
        ),
        index("ev_b", """"g":{"type":"keyword"},"d":{"type":"date"},"c":{"type":"keyword"}""")
      )
    )._1

  private def resolved(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(pattern))
      case other                   => fail(s"[$sql] $other")
    }

  "a statement over a pattern with one conflicting field" should
  "type every field the indices agree on, and the conflicting one by the KEYWORD rules" in {
    // the fields they agree on: the type rules apply as over a single index (`d` is a `date`
    // elasticsql never declared -- a TIMESTAMP -- so a difference of two is fractional days)
    resolved("SELECT g, MAX(d) - MIN(d) AS x FROM ev_* GROUP BY g").select.fields
      .find(_.fieldAlias.exists(_.alias == "x"))
      .map(_.identifier.reportedType) shouldBe Some(SQLTypes.Double)
    resolved("SELECT d * 2 AS x FROM ev_*").validateResolved().swap.getOrElse("") should include(
      "operator * cannot be applied to TIMESTAMP and BIGINT"
    )
    resolved("SELECT n + g AS x FROM ev_*").validateResolved().isLeft shouldBe true
    // SCHEMA-7 (D6, R-84, A-66): the conflicting field -- a date in one index, a keyword in the
    // other -- is a VARCHAR, as the keyword index maps it: the single-index KEYWORD rules and
    // message (main: no type, then a Painless class_cast or an empty answer from Elasticsearch)
    resolved("SELECT c FROM ev_*").schema.flatMap(_.find("c")).map(_.dataType) shouldBe
    Some(SQLTypes.Keyword)
    Seq(
      "SELECT c * 2 AS x FROM ev_*",
      "SELECT c + 1 AS x FROM ev_*",
      "SELECT c + d AS x FROM ev_*",
      "SELECT n FROM ev_* WHERE c + 1 > CURRENT_DATE"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        resolved(sql).validateResolved().isLeft shouldBe true
      }
    }
    resolved("SELECT c + 1 AS x FROM ev_*").validateResolved() shouldBe
    Left("Type mismatch: output 'KEYWORD' is not compatible with input 'BIGINT'")
    Seq(
      "SELECT n FROM ev_* WHERE c = 1",
      "SELECT g, MAX(c) - MIN(c) AS x FROM ev_* GROUP BY g"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        resolved(sql).validateResolved() shouldBe Right(())
      }
    }
  }
}
