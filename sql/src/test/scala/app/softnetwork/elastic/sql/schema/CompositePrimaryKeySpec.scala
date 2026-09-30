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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** A composite PRIMARY KEY must produce a non-empty `_id`.
  *
  * The pipeline's `_id` template rendered a composite key as ONE mustache variable (`{{a\|\|b}}`),
  * which resolves to nothing: Elasticsearch rejected every document ("if _id is specified it must
  * not be empty", measured on 7.17.29 and 8.18.3) while `CREATE` reported success. The template is
  * now one variable per column, in the DECLARED order, joined by the literal separator. The JSON
  * asserted here is the pipeline `CREATE TABLE` deploys (`Table.defaultPipeline`); neither bridge
  * emits a pipeline -- this module does.
  */
class CompositePrimaryKeySpec extends AnyFlatSpec with Matchers {

  private def table(sql: String): Table = Parser(sql) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"expected a CREATE TABLE, got $other: [$sql]")
  }

  private def ddl(key: Seq[String]): String =
    s"CREATE TABLE t (${key.map(c => s"$c KEYWORD").mkString(", ")}, v INT, " +
    s"PRIMARY KEY (${key.mkString(", ")}))"

  /** The `_id` `set` processor of the deployed pipeline, as Elasticsearch receives it. */
  private def idProcessorJson(t: Table): String =
    t.defaultPipeline.node
      .get("processors")
      .elements()
      .asScala
      .toSeq
      .filter(p => p.has("set") && p.get("set").get("field").asText() == "_id") match {
      case Seq(one) => mapper.writeValueAsString(one)
      case other    => fail(s"expected exactly one _id set processor, got $other")
    }

  /** The pipeline as `PipelineApi` reads it back out of Elasticsearch. */
  private def readBack(t: Table, stored: String): IngestPipeline =
    IngestPipeline(
      name = t.defaultPipeline.name,
      json = stored,
      pipelineType = Some(IngestPipelineType.Default)
    )

  private def storedKey(pipeline: IngestPipeline): Seq[Seq[String]] =
    pipeline.processors.collect { case k: PrimaryKeyProcessor => k.value }

  private def expectedJson(key: Seq[String], template: String): String =
    s"""{"set":{"description":"PRIMARY KEY (${key.mkString(", ")})","field":"_id",""" +
    s""""value":"$template","ignore_failure":false,"ignore_empty_value":false}}"""

  private val keys: Seq[(Seq[String], String)] = Seq(
    Seq("id")              -> "{{id}}",
    Seq("city", "country") -> "{{city}}||{{country}}",
    // declared order is not alphabetical, so an order taken from anywhere else shows
    Seq("region", "city", "at") -> "{{region}}||{{city}}||{{at}}"
  )

  for ((key, template) <- keys) {
    s"a PRIMARY KEY of ${key.size} column(s)" should "render one mustache variable per column" in {
      idProcessorJson(table(ddl(key))) shouldBe expectedJson(key, template)
    }

    it should "read back as its declared columns, in order, with an empty pipeline diff" in {
      val t = table(ddl(key))
      val stored = readBack(t, t.defaultPipeline.json)
      storedKey(stored) shouldBe Seq(key)
      stored.diff(t.defaultPipeline) shouldBe Nil
    }
  }

  "a PRIMARY KEY wider than a Set keeps its order" should "render in the declared order" in {
    val key = Seq("k6", "k5", "k4", "k3", "k2", "k1")
    // Teeth: the Set the key used to be stored in does NOT iterate in the declared order here.
    Set(key: _*).toSeq should not be key
    idProcessorJson(table(ddl(key))) shouldBe
    expectedJson(key, key.map(c => s"{{$c}}").mkString("||"))
    val t = table(ddl(key))
    storedKey(readBack(t, t.defaultPipeline.json)) shouldBe Seq(key)
  }

  "a template stored before the fix" should "read back as the ONE variable it is, so the diff repairs it" in {
    val t = table(ddl(Seq("city", "country")))
    // What the previous release deployed: the regex separator inside ONE variable.
    val legacy = t.defaultPipeline.json.replace(
      "{{city}}||{{country}}",
      """{{city\\|\\|country}}"""
    )
    legacy should include("""{{city\\|\\|country}}""")
    val stored = readBack(t, legacy)
    storedKey(stored) shouldBe Seq(Seq("city\\|\\|country"))
    stored.diff(t.defaultPipeline) match {
      case List(ProcessorChanged(_, to: PrimaryKeyProcessor, d)) =>
        d.propertyDiffs shouldBe List(
          ProcessorPropertyChanged("value", "{{city\\|\\|country}}", "{{city}}||{{country}}")
        )
        to.value shouldBe Seq("city", "country")
      case other => fail(s"expected the _id processor to diff as changed, got $other")
    }
  }

  "CompositeKey" should "join with the literal text of the configured separator regex" in {
    sqlConfig.compositeKeySeparator shouldBe "\\|\\|"
    CompositeKey.separator shouldBe "||"
    CompositeKey.literal("-") shouldBe "-"
    CompositeKey.literal("\\\\") shouldBe "\\"
  }

  it should "refuse a configured separator whose literal text it cannot read back" in {
    an[IllegalArgumentException] should be thrownBy CompositeKey.literal("[|]{2}")
    an[IllegalArgumentException] should be thrownBy CompositeKey.literal("\\Q||\\E")
    // `|` alone is an EMPTY alternation: it matches no separator at all
    an[IllegalArgumentException] should be thrownBy CompositeKey.literal("|")
  }

  it should "build the _id the pipeline template renders, equal values included" in {
    CompositeKey.id(Seq("Paris", "FR")) shouldBe "Paris||FR"
    CompositeKey.id(Seq("7", "7")) shouldBe "7||7"
    CompositeKey.id(Seq("42")) shouldBe "42"
    CompositeKey.columns(CompositeKey.template(Seq("a", "b", "c"))) shouldBe Seq("a", "b", "c")
  }
}
