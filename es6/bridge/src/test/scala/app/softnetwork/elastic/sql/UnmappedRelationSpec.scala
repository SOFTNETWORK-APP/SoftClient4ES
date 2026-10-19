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
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{mergedSchema, Schema}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.collection.JavaConverters._

/** A nested query, a `has_child` and a `has_parent` carry `ignore_unmapped: true` exactly when the
  * statement's schema knows their path or relation: the path mapped as `nested`, the relation held
  * by a join field in that role.
  *
  * Without the key, Elasticsearch fails every shard of an index of a pattern or list that does not
  * map the nested path or has no join field (`failed to find nested object under path`, `no join
  * field has been configured`), even when that index is empty. Such an index holds no element and
  * no relation, so with the key the condition matches none of its documents. A path or a relation
  * no index knows (a typo, a relation in the other role, members that map the path differently)
  * keeps its bytes, and keeps failing loudly.
  *
  * Every such node of the rendered body is read, through each door that renders the same condition:
  * the row search, an aggregation, a DELETE by query. The schema is loaded from the document `GET
  * <index>` answers, alone or merged with an empty member, as core attaches it. Same cases as the
  * hand-maintained es6 bridge's copy.
  */
class UnmappedRelationSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private def schemaOf(index: String, properties: Option[String]): Schema = {
    val mappings = properties.fold("{}")(p => s"""{"properties": $p}""")
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": $mappings, """ +
      s""""settings": {"index": {"number_of_shards": "3"}}}}"""
    ).schema
  }

  /** The member alone, and the member merged with an index that maps nothing. */
  private def singleAndMerged(properties: String): Seq[(String, Schema)] = Seq(
    "single index" -> schemaOf("a", Some(properties)),
    "pattern with an empty member" ->
    mergedSchema("p_*", Seq("a" -> schemaOf("a", Some(properties)), "e" -> schemaOf("e", None)))._1
  )

  private val nestedProperties: String =
    """{"id": {"type": "keyword"},
      | "items": {"type": "nested", "properties": {"k": {"type": "keyword"},
      |   "q": {"type": "integer"},
      |   "subs": {"type": "nested", "properties": {"v": {"type": "keyword"}}}}},
      | "tags": {"type": "nested", "properties": {"n": {"type": "keyword"}}}}""".stripMargin

  /** The same paths mapped as objects. */
  private val objectProperties: String =
    """{"id": {"type": "keyword"},
      | "items": {"properties": {"k": {"type": "keyword"}, "q": {"type": "integer"},
      |   "subs": {"properties": {"v": {"type": "keyword"}}}}},
      | "tags": {"properties": {"n": {"type": "keyword"}}}}""".stripMargin

  /** Members that disagree on every path: nested in one, object (or keyword) in the other. */
  private val disagreeing: Schema = mergedSchema(
    "p_*",
    Seq(
      "a" -> schemaOf("a", Some(nestedProperties)),
      "b" -> schemaOf(
        "b",
        Some(
          """{"id": {"type": "keyword"}, "items": {"properties": {"k": {"type": "keyword"},
            | "q": {"type": "integer"}, "subs": {"type": "keyword"}}},
            | "tags": {"type": "keyword"}}""".stripMargin
        )
      )
    )
  )._1

  private def joinProperties(relations: Option[String]): String = {
    val rel = relations.map(r => s""" "rel": {"type": "join", "relations": $r},""").getOrElse("")
    s"""{"id": {"type": "keyword"},$rel
       | "question": {"properties": {"title": {"type": "keyword"}}},
       | "answer": {"properties": {"body": {"type": "keyword"}}}}""".stripMargin
  }

  private val held: String = joinProperties(Some("""{"question": "answer"}"""))

  /** A WHERE over UNNEST elements (leaf, multi-level, several trees, NOT), an UNNEST projection
    * (leaf, multi-level), and a projection beside a WHERE on another UNNEST.
    */
  private val nestedStatements: Seq[String] = Seq(
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1' LIMIT 100",
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1'",
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(i.subs) AS s WHERE s.v = 'v1' LIMIT 100",
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(t.tags) AS g WHERE i.k = 'k1' AND g.n = 'x' LIMIT 100",
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(t.tags) AS g WHERE i.k = 'k1' OR g.n = 'x' LIMIT 100",
    "SELECT t.id FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(t.tags) AS g WHERE i.k = 'k1' AND NOT g.n = 'x' LIMIT 100",
    "SELECT t.id, i.k FROM t JOIN UNNEST(t.items) AS i LIMIT 100",
    "SELECT t.id, i.k FROM t JOIN UNNEST(t.items) AS i",
    "SELECT t.id, s.v FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(i.subs) AS s",
    "SELECT t.id, i.k FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(t.tags) AS g WHERE g.n = 'x'",
    // an aggregation body: a nested aggregation filtered by the WHERE, and a root one
    "SELECT i.k, MIN(i.q) AS m FROM t JOIN UNNEST(t.items) AS i WHERE i.q > 1 GROUP BY i.k",
    "SELECT t.id, COUNT(*) AS n FROM t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1' GROUP BY t.id"
  )

  /** CHILD(…) and PARENT(…), alone and together, then in an aggregation body. */
  private val joinStatements: Seq[String] = Seq(
    "SELECT id FROM j WHERE CHILD(answer.body = 'x') LIMIT 100",
    "SELECT id FROM j WHERE PARENT(question.title = 't1') LIMIT 100",
    "SELECT id FROM j WHERE CHILD(answer.body = 'x') OR PARENT(question.title = 't1') LIMIT 100",
    "SELECT id, COUNT(*) AS n FROM j WHERE CHILD(answer.body = 'x') GROUP BY id"
  )

  /** The relation statements with each relation in the other role, or misspelt. */
  private val joinStatementsUnknown: Seq[String] = Seq(
    "SELECT id FROM j WHERE CHILD(question.title = 't1') LIMIT 100",
    "SELECT id FROM j WHERE PARENT(answer.body = 'x') LIMIT 100",
    "SELECT id FROM j WHERE CHILD(answr.body = 'x') LIMIT 100",
    "SELECT id FROM j WHERE PARENT(questn.title = 't1') LIMIT 100"
  )

  private def typo(sql: String): String =
    sql.replace("t.items", "t.itemz").replace("t.tags", "t.tagz")

  private def search(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
  }

  /** The bodies the same condition renders through: the search (row or aggregation), and the DELETE
    * by query.
    */
  private def bodies(sql: String, schema: Option[Schema]): Seq[(String, JsonNode)] = {
    val parsed = search(sql)
    val request: SingleSearch = schema.fold(parsed)(s => parsed.update(Some(s)))
    val searched: ElasticSearchRequest = request
    val deleted: ElasticSearchRequest = request.copy(deleteByQuery = true)
    Seq(
      "search"          -> mapper.readTree(searched.query),
      "delete by query" -> mapper.readTree(deleted.query)
    )
  }

  /** Every nested QUERY (a nested aggregation names a path only), `has_child` and `has_parent` node
    * of `body`, at any depth, a nested query inside another one included (`findValues` does not
    * descend into the value it matched).
    */
  private def relationNodes(body: JsonNode): Seq[(String, JsonNode)] =
    Seq("nested", "has_child", "has_parent").flatMap { kind =>
      Option(body.get(kind)).filter(node => kind != "nested" || node.has("query")).map(kind -> _)
    } ++ body.elements().asScala.toSeq.flatMap(relationNodes)

  private def assertKeys(sql: String, schema: Option[Schema], expected: Boolean): Unit =
    bodies(sql, schema).foreach { case (door, body) =>
      withClue(s"[$door] $sql\n$body\n") {
        val nodes = relationNodes(body)
        nodes should not be empty
        nodes.foreach { case (kind, node) =>
          withClue(s"$kind node $node: ") {
            if (expected) Option(node.get("ignore_unmapped")).map(_.asBoolean) shouldBe Some(true)
            else node.has("ignore_unmapped") shouldBe false
          }
        }
        // inner_hits never carries it: Elasticsearch applies the query's own
        body.findValues("inner_hits").asScala.foreach(_.has("ignore_unmapped") shouldBe false)
      }
    }

  "a nested condition or an UNNEST over paths the schema maps as nested" should
  "carry ignore_unmapped on every nested query" in {
    singleAndMerged(nestedProperties).foreach { case (label, schema) =>
      withClue(s"$label: ") {
        nestedStatements.foreach(assertKeys(_, Some(schema), expected = true))
      }
    }
  }

  it should "keep its bytes over a path no index knows, an object path, or members that disagree" in {
    singleAndMerged(nestedProperties).foreach { case (label, schema) =>
      withClue(s"$label, typo: ") {
        nestedStatements.map(typo).foreach(assertKeys(_, Some(schema), expected = false))
      }
    }
    singleAndMerged(objectProperties).foreach { case (label, schema) =>
      withClue(s"$label, object paths: ") {
        nestedStatements.foreach(assertKeys(_, Some(schema), expected = false))
      }
    }
    withClue("members that disagree: ") {
      nestedStatements.foreach(assertKeys(_, Some(disagreeing), expected = false))
    }
  }

  it should "keep its bytes without a schema" in {
    (nestedStatements ++ joinStatements).foreach(assertKeys(_, None, expected = false))
  }

  "a nested aggregation without a nested condition" should "carry nothing new" in {
    // Elasticsearch builds an unmapped aggregator for a nested aggregation over an index that does
    // not map its path, so the aggregation is not a relation node: no key, whatever the schema.
    val sql = "SELECT i.k, MIN(i.q) AS m FROM t JOIN UNNEST(t.items) AS i GROUP BY i.k"
    (singleAndMerged(nestedProperties).map(_._2).map(Option(_)) :+ None).foreach { schema =>
      bodies(sql, schema).take(1).foreach { case (door, body) =>
        withClue(s"[$door] $body") {
          body.findValues("nested").asScala.toSeq.filter(_.has("path")) should not be empty
          relationNodes(body) shouldBe empty
          body.toString should not include "ignore_unmapped"
        }
      }
    }
  }

  "CHILD(…) and PARENT(…) over a relation a join field holds in that role" should
  "carry ignore_unmapped on every has_child and has_parent" in {
    singleAndMerged(held).foreach { case (label, schema) =>
      withClue(s"$label: ") {
        joinStatements.foreach(assertKeys(_, Some(schema), expected = true))
      }
    }
  }

  it should "keep its bytes for a relation in the other role, a typo, other relations, or no join field" in {
    singleAndMerged(held).foreach { case (label, schema) =>
      withClue(s"$label: ") {
        joinStatementsUnknown.foreach(assertKeys(_, Some(schema), expected = false))
      }
    }
    Seq(
      "no join field"   -> joinProperties(None),
      "other relations" -> joinProperties(Some("""{"x": "y"}"""))
    ).foreach { case (what, properties) =>
      singleAndMerged(properties).foreach { case (label, schema) =>
        withClue(s"$what, $label: ") {
          joinStatements.foreach(assertKeys(_, Some(schema), expected = false))
        }
      }
    }
  }
}
