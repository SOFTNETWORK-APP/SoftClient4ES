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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{mergedSchema, Schema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The two schema facts a nested, a `has_child` and a `has_parent` query read to carry
  * `ignore_unmapped`: `NestedElements.mappedAsNested` (the schema maps the path as `nested`) and
  * `ElasticRelation.joinHolds` (a join field holds the relation in that role).
  *
  * The key lets an index of a pattern or list that does not map the path, or has no join field,
  * match none of its documents instead of failing its shards. It must never reach a path or a
  * relation no index knows, which must keep failing loudly. The schemas are loaded from the
  * document `GET <index>` answers, and merged as core merges the members of a pattern.
  */
class UnmappedRelationFactSpec extends AnyFlatSpec with Matchers {

  private def schemaOf(index: String, mappings: String): Schema =
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": $mappings, """ +
      s""""settings": {"index": {"number_of_shards": "3"}}}}"""
    ).schema

  private def itemsMapped(field: String, meta: String = ""): Schema =
    schemaOf("t", s"""{$meta"properties": {"id": {"type": "keyword"}, "items": $field}}""")

  private val emptyMember: Schema = schemaOf("e", "{}")

  private val nested: Schema =
    itemsMapped("""{"type": "nested", "properties": {"k": {"type": "keyword"}}}""")

  /** Every Elasticsearch field (mapping) type a path may be mapped as. */
  private val esMappingTypes: Seq[String] = Seq(
    "text",
    "keyword",
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "date",
    "date_nanos",
    "boolean",
    "binary",
    "ip",
    "object",
    "nested",
    "flattened",
    "geo_point",
    "geo_shape",
    "join",
    "wildcard",
    "constant_keyword",
    "dense_vector",
    "completion",
    "version"
  )

  "mappedAsNested" should "be true exactly for a path mapped as nested" in {
    esMappingTypes.foreach { m =>
      withClue(s"mapping type '$m': ") {
        val schema = Some(itemsMapped(s"""{"type": "$m"}"""))
        NestedElements.mappedAsNested(schema, "items") shouldBe (m == "nested")
      }
    }
  }

  it should "be true exactly for a column declared ARRAY<STRUCT>, whatever its mapping says" in {
    val names = SQLKeywords.typeWords.toSeq.sorted.flatMap(w => Seq(w, s"ARRAY<$w>"))
    names should contain("ARRAY<STRUCT>")
    val declaredNested = names.filter { name =>
      val meta = s""""_meta": {"columns": {"items": {"data_type": "$name"}}}, """
      NestedElements.mappedAsNested(
        Some(itemsMapped("""{"properties": {"k": {"type": "keyword"}}}""", meta)),
        "items"
      )
    }
    declaredNested shouldBe Seq("ARRAY<STRUCT>")
  }

  it should "follow a full dotted path, under a nested or an object parent" in {
    val deep = schemaOf(
      "t",
      """{"properties": {"items": {"type": "nested", "properties": {
        | "subs": {"type": "nested", "properties": {"v": {"type": "keyword"}}},
        | "obj": {"type": "object", "properties": {"v": {"type": "keyword"}}}}},
        | "o": {"properties": {"subs": {"type": "nested"}}}}}""".stripMargin
    )
    NestedElements.mappedAsNested(Some(deep), "items") shouldBe true
    NestedElements.mappedAsNested(Some(deep), "items.subs") shouldBe true
    NestedElements.mappedAsNested(Some(deep), "items.obj") shouldBe false
    NestedElements.mappedAsNested(Some(deep), "o.subs") shouldBe true
    NestedElements.mappedAsNested(Some(deep), "o") shouldBe false
    // the leaf name alone is not the path
    NestedElements.mappedAsNested(Some(deep), "subs") shouldBe false
  }

  it should "be false without a schema, for an absent path, and over members that disagree" in {
    NestedElements.mappedAsNested(None, "items") shouldBe false
    NestedElements.mappedAsNested(Some(nested), "itemz") shouldBe false
    NestedElements.mappedAsNested(Some(nested), "items.k") shouldBe false
    Seq(
      """{"properties": {"k": {"type": "keyword"}}}""",
      """{"type": "keyword"}"""
    ).foreach { other =>
      withClue(other) {
        val (merged, conflicts) =
          mergedSchema("p_*", Seq("a" -> nested, "b" -> itemsMapped(other)))
        conflicts should not be empty
        merged.find("items").map(_.dataType) shouldBe Some(SQLTypes.Any)
        NestedElements.mappedAsNested(Some(merged), "items") shouldBe false
      }
    }
  }

  it should "be true over a pattern whose other member does not map the path" in {
    val idOnly = schemaOf("p", """{"properties": {"id": {"type": "keyword"}}}""")
    Seq(
      Seq("a" -> nested, "e"      -> emptyMember),
      Seq("e" -> emptyMember, "a" -> nested),
      Seq("a" -> nested, "p"      -> idOnly)
    ).foreach { members =>
      withClue(members.map(_._1).mkString(",")) {
        val (merged, conflicts) = mergedSchema("p_*", members)
        conflicts shouldBe empty
        NestedElements.mappedAsNested(Some(merged), "items") shouldBe true
      }
    }
  }

  private def search(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
  }

  "every element a bridge reads" should "carry the fact once the schema is attached" in {
    val deep = schemaOf(
      "t",
      """{"properties": {"id": {"type": "keyword"}, "items": {"type": "nested", "properties": {
        | "k": {"type": "keyword"}, "subs": {"type": "nested", "properties": {
        | "v": {"type": "keyword"}}}}}}}""".stripMargin
    )
    Seq(
      "SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1' LIMIT 100",
      "SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1'",
      "SELECT t.id, i.k FROM t JOIN UNNEST(t.items) AS i",
      "SELECT t.id FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(i.subs) AS s WHERE s.v = 'v1'",
      "SELECT t.id, s.v FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(i.subs) AS s"
    ).foreach { sql =>
      withClue(sql) {
        val parsed = search(sql)
        val updated = parsed.update(Some(deep))
        // The WHERE identifiers re-derive their element on the schema pass, parents included.
        val where = updated.where.map(_.nestedElements).getOrElse(Nil)
        val elements = (where ++ updated.nested).flatMap(e => e +: e.parent.toSeq)
        elements should not be empty
        elements.map(_.ignoreUnmapped).distinct shouldBe Seq(Some(true))
        val unschemed = (parsed.where.map(_.nestedElements).getOrElse(Nil) ++ parsed.nested)
          .flatMap(e => e +: e.parent.toSeq)
        unschemed should not be empty
        unschemed.map(_.ignoreUnmapped).distinct shouldBe Seq(None)
      }
    }
  }

  // -- joinHolds ----------------------------------------------------------------------------------

  private def joinSchema(relations: Option[String]): Schema = {
    val rel = relations.map(r => s""", "rel": {"type": "join", "relations": $r}""").getOrElse("")
    schemaOf("j", s"""{"properties": {"id": {"type": "keyword"}$rel}}""")
  }

  /** Each relations shape, with the relations it holds as a parent and as a child. */
  private val shapes: Seq[(String, Option[String], Set[String], Set[String])] = Seq(
    ("{q:a}", Some("""{"q": "a"}"""), Set("q"), Set("a")),
    ("{q:[a,c]}", Some("""{"q": ["a", "c"]}"""), Set("q"), Set("a", "c")),
    ("{q:a, a:v}", Some("""{"q": "a", "a": "v"}"""), Set("q", "a"), Set("a", "v")),
    ("no join field", None, Set.empty, Set.empty),
    ("other relations {x:y}", Some("""{"x": "y"}"""), Set("x"), Set("y"))
  )

  "joinHolds" should "hold a relation exactly in the role its join field gives it" in {
    val relations = Seq("q", "a", "c", "v", "x", "y", "z")
    shapes.foreach { case (label, json, parents, children) =>
      val schema = joinSchema(json)
      relations.foreach { r =>
        withClue(s"$label, relation '$r': ") {
          ElasticRelation.joinHolds(Some(schema), r, asParent = true) shouldBe parents.contains(r)
          ElasticRelation.joinHolds(Some(schema), r, asParent = false) shouldBe children.contains(r)
          ElasticRelation.joinHolds(None, r, asParent = true) shouldBe false
          ElasticRelation.joinHolds(None, r, asParent = false) shouldBe false
        }
      }
    }
  }

  it should "read the join field of the member that maps it, over a pattern" in {
    val (merged, _) =
      mergedSchema("p_*", Seq("e" -> emptyMember, "a" -> joinSchema(Some("""{"q": "a"}"""))))
    ElasticRelation.joinHolds(Some(merged), "q", asParent = true) shouldBe true
    ElasticRelation.joinHolds(Some(merged), "a", asParent = false) shouldBe true
    ElasticRelation.joinHolds(Some(merged), "a", asParent = true) shouldBe false
  }

  private val join: Schema = schemaOf(
    "j",
    """{"properties": {"id": {"type": "keyword"},
      | "rel": {"type": "join", "relations": {"question": "answer"}},
      | "question": {"properties": {"title": {"type": "keyword"}}},
      | "answer": {"properties": {"body": {"type": "keyword"}}}}}""".stripMargin
  )

  private def relationsOf(sql: String, schema: Option[Schema]): Seq[(String, Option[Boolean])] = {
    val parsed = search(sql)
    val updated = schema.fold(parsed)(s => parsed.update(Some(s)))
    def walk(c: Criteria): Seq[(String, Option[Boolean])] = c match {
      case child: ElasticChild => Seq("has_child" -> child.ignoreUnmapped) ++ walk(child.criteria)
      case parent: ElasticParent =>
        Seq("has_parent" -> parent.ignoreUnmapped) ++ walk(parent.criteria)
      case p: Predicate => walk(p.leftCriteria) ++ walk(p.rightCriteria)
      case _            => Nil
    }
    updated.where.flatMap(_.criteria).toSeq.flatMap(walk)
  }

  "CHILD(…) and PARENT(…)" should "know their relation only when a join field holds it in that role" in {
    relationsOf("SELECT id FROM j WHERE CHILD(answer.body = 'x')", Some(join)) shouldBe
    Seq("has_child" -> Some(true))
    relationsOf("SELECT id FROM j WHERE PARENT(question.title = 't1')", Some(join)) shouldBe
    Seq("has_parent" -> Some(true))
    // the relation in the other role, a typo, no join field, no schema: no key
    Seq(
      ("SELECT id FROM j WHERE CHILD(question.title = 't1')", Some(join), "has_child"),
      ("SELECT id FROM j WHERE PARENT(answer.body = 'x')", Some(join), "has_parent"),
      ("SELECT id FROM j WHERE CHILD(answr.body = 'x')", Some(join), "has_child"),
      ("SELECT id FROM j WHERE PARENT(questn.title = 't1')", Some(join), "has_parent"),
      ("SELECT id FROM j WHERE CHILD(answer.body = 'x')", Some(joinSchema(None)), "has_child"),
      ("SELECT id FROM j WHERE CHILD(answer.body = 'x')", None, "has_child"),
      ("SELECT id FROM j WHERE PARENT(question.title = 't1')", None, "has_parent")
    ).foreach { case (sql, schema, kind) =>
      withClue(sql) { relationsOf(sql, schema) shouldBe Seq(kind -> None) }
    }
  }
}
