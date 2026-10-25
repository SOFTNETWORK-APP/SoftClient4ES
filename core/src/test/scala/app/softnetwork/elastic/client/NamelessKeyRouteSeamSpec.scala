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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

/** `resolveWithSchema` sends a statement WITHOUT a resolved schema at several exits -- a mapping
  * that cannot be read, a remembered miss -- and every one of them asks the statement's
  * `validateUnresolved`: a HAVING that filters a nameless GROUP BY key (`GROUP BY UPPER(s) HAVING
  * UPPER(s) = 'A'`) compares the key's script value as text, which is known to be the condition's
  * spelling only once the column's type is. The same statement with a mapping, and a companion
  * level's spelling without one, are sent.
  */
class NamelessKeyRouteSeamSpec extends AnyFlatSpec with Matchers {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private val schema: Schema = Table("mapped", columns = List(Column("s", SQLTypes.Keyword)))

  private class Client extends NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  private def parse(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private val nameless =
    "SELECT UPPER(s) AS k, COUNT(*) AS c FROM %s GROUP BY UPPER(s) HAVING UPPER(s) = 'A'"

  private val companion =
    "SELECT s, UPPER(s) AS k, COUNT(*) AS c FROM %s GROUP BY s, UPPER(s) HAVING UPPER(s) = 'A'"

  "a HAVING on a nameless key sent without a schema" should "be refused with 400, at every attempt" in {
    val client = new Client
    (1 to 2).foreach { attempt =>
      withClue(s"attempt $attempt: ") {
        client.resolveWithSchema(parse(nameless.format("unmapped"))) match {
          case ElasticFailure(error) =>
            error.statusCode shouldBe Some(400)
            error.operation shouldBe Some("search")
            error.index shouldBe Some("unmapped")
            error.message should startWith("HAVING cannot filter the GROUP BY key UPPER(s): ")
            error.message should include("the type of its column s is not known")
          case ElasticSuccess(sent) => fail(s"sent: ${sent.sql}")
        }
      }
    }
  }

  "the same statements" should "be sent with a mapping, and the companion spelling without one" in {
    val client = new Client
    client.updateSchema("mapped", schema)
    client.resolveWithSchema(parse(nameless.format("mapped"))).isSuccess shouldBe true
    client.resolveWithSchema(parse(companion.format("unmapped"))).isSuccess shouldBe true
    client.resolveWithSchema(parse(companion.format("mapped"))).isSuccess shouldBe true
  }
}
