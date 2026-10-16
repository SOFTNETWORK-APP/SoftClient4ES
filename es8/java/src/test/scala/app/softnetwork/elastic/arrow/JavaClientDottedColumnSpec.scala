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

package app.softnetwork.elastic.arrow

import app.softnetwork.elastic.client.DottedColumnSpec
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

/** The ES 8 runner of [[DottedColumnSpec]], in a DOWNSTREAM package on purpose: the consumer smokes
  * below reach core's rows exactly as the consumers do -- `gateway.run` and the three result
  * variants -- through public API only (no publish: the cross-repo runs follow the republish).
  */
class JavaClientDottedColumnSpec extends DottedColumnSpec {

  /** Arrow's `ElasticSqlExecutor.extractRows`: every variant a gateway result takes. */
  private def extractRows(result: QueryResult): Seq[ListMap[String, Any]] = result match {
    case QueryRows(rows, _)           => rows
    case QueryStructured(response, _) => response.results
    case QueryStream(stream, _) =>
      Await.result(stream.map(_._1).runWith(akka.stream.scaladsl.Sink.seq), 60.seconds)
    case _ => Seq.empty
  }

  private def gatewayRows(sql: String): Seq[ListMap[String, Any]] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => extractRows(result)
      case ElasticFailure(error)  => fail(s"[$sql] ${error.message}")
    }

  "A consumer" should "read the SCHEMA-1 arrow legs by declared name (arrow legs)" in {
    // un-LIMITed (QueryStream) and LIMITed (one-shot) forms of both legs, read by declared name
    Seq("", " LIMIT 100").foreach { limit =>
      withClue(s"[limit='$limit'] ") {
        val dotted = gatewayRows(s"SELECT obj.a FROM $uniqueTable$limit").map(_.get("obj.a").orNull)
        val plain = gatewayRows(s"SELECT n FROM $uniqueTable$limit").map(_.get("n").orNull)
        dotted.size shouldBe uniqueDocs.size
        dotted should not contain (null: Any)
        (dotted ++ plain).map(String.valueOf).distinct.size shouldBe 8
      }
    }
  }

  it should "build JDBC metadata from the rows: the SELECT list, typed from a value (jdbc metadata)" in {
    // `ElasticResultSetMetaData.fromRows`: the union of the rows' keys in order, each typed from the
    // first non-null value found across the rows
    Seq("", " LIMIT 100").foreach { limit =>
      withClue(s"[limit='$limit'] ") {
        val rows = gatewayRows(s"SELECT id, obj.a FROM $uniqueTable$limit")
        val seen = scala.collection.mutable.LinkedHashSet.empty[String]
        rows.foreach(row => row.keys.foreach(k => seen += k))
        seen.toList shouldBe List("id", "obj.a")
        rows.iterator.map(_.getOrElse("obj.a", null)).find(_ != null).orNull shouldBe a[
          java.lang.Integer
        ]
      }
    }
  }

  it should "preload a changelog with main's documents, which read back with their values (extensions preload)" in {
    inserted
    // the statement extensions generates (`INSERT INTO <changelog> AS SELECT <columns> FROM <t>;`),
    // run through `client.run`: a nested source is written nested, as on main (no flat `obj.a`),
    // and the scripted column reads it
    val dn = org.json4s.jackson.JsonMethods.parse(storedSource(changelogTable, "dn"))
    (dn \ "obj.a") shouldBe org.json4s.JNothing
    (dn \ "obj" \ "a") shouldBe org.json4s.JInt(1)
    (dn \ "twice") shouldBe org.json4s.JInt(2)
    // a view read: QueryStream (no LIMIT) and the one-shot variants (LIMIT)
    Seq("", " LIMIT 100").foreach { limit =>
      withClue(s"[limit='$limit'] ") {
        val back = gatewayRows(s"SELECT id, obj.a FROM $changelogTable$limit")
          .map(r => String.valueOf(r("id")) -> r.get("obj.a").orNull)
          .toMap
        Seq("dn", "df", "db", "dz", "dn4").foreach { id =>
          withClue(s"$id: ")(back(id) shouldBe valueOfA(id))
        }
      }
    }
  }
}
