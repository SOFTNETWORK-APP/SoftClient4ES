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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.query.TemporalLiterals.FieldFormat
import app.softnetwork.elastic.sql.schema.Column
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.utils.BoundedMemo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.collection.immutable.ListMap

/** SCHEMA-17 fix R-159 (A-193) -- a column's effective date format (`FieldFormat.of`, THE format
  * function, #292) is built ONCE per resolved format: every column of that format shares the
  * instance, which answers as a fresh `FieldFormat` of it does.
  */
class TemporalLiteralFormatMemoSpec extends AnyFlatSpec with Matchers {

  private def column(
    format: Option[String],
    mappingType: Option[String] = None,
    dataType: SQLType = SQLTypes.Timestamp,
    name: String = "d"
  ): Column =
    Column(
      name,
      dataType,
      options = ListMap(
        (format.map(f => "format" -> StringValue(f)).toSeq ++
        mappingType.map(t => "type" -> StringValue(t)).toSeq): _*
      )
    )

  /** Each column and the format it resolves to: the defaults (`date`, `date_nanos`), a custom
    * pattern, epochs, Elasticsearch 7's `8`-prefixed pattern, a zone name, a mixed list.
    */
  private val columns: Seq[(Column, String)] = Seq(
    column(None)                                       -> TemporalLiterals.DefaultDateFormat,
    column(None, mappingType = Some("date_nanos"))     -> TemporalLiterals.DefaultDateNanosFormat,
    column(None, dataType = SQLTypes.Date)             -> TemporalLiterals.DefaultDateFormat,
    column(Some("yyyy/MM/dd"))                         -> "yyyy/MM/dd",
    column(Some("epoch_millis"))                       -> "epoch_millis",
    column(Some("epoch_second||yyyy-MM-dd"))           -> "epoch_second||yyyy-MM-dd",
    column(Some("8yyyy-MM-dd HH:mm"))                  -> "8yyyy-MM-dd HH:mm",
    column(Some("yyyy-MM-dd HH:mm z"))                 -> "yyyy-MM-dd HH:mm z",
    column(Some(TemporalLiterals.FieldCapsDateFormat)) -> TemporalLiterals.FieldCapsDateFormat,
    column(Some("strict_date_optional_time || epoch_millis")) ->
    "strict_date_optional_time || epoch_millis"
  )

  private val literals: Seq[String] = Seq(
    "2024-01-31",
    "2024-01-31T10:30:00Z",
    "2024-01-31 10:30",
    "2024/01/31",
    "1706697000000",
    "2024-01-31 10:30 UTC",
    "not a date"
  )

  "FieldFormat.of" should "share ONE instance per resolved format" in {
    columns.foreach { case (c, spec) =>
      withClue(s"[$spec] ") {
        val shared = FieldFormat.of(c)
        shared.spec shouldBe spec
        (FieldFormat.of(c) eq shared) shouldBe true
        (FieldFormat.of(c.copy(name = "other")) eq shared) shouldBe true
      }
    }
    (FieldFormat.of(column(None)) eq FieldFormat.of(
      column(None, mappingType = Some("date_nanos"))
    )) shouldBe false
  }

  it should "answer as a fresh FieldFormat of the same format does" in {
    columns.foreach { case (c, spec) =>
      withClue(s"[$spec] ") {
        val shared = FieldFormat.of(c)
        val fresh = FieldFormat(spec)
        shared shouldBe fresh
        shared.alternatives shouldBe fresh.alternatives
        shared.acceptsIsoOptionalTime shouldBe fresh.acceptsIsoOptionalTime
        shared.fullyUnderstood shouldBe fresh.fullyUnderstood
        shared.customPatterns shouldBe fresh.customPatterns
        literals.foreach { literal =>
          shared.acceptsAsCustom(literal) shouldBe fresh.acceptsAsCustom(literal)
        }
        val instant = Instant.parse("2024-01-31T10:30:00Z")
        shared.renderCustom(instant) shouldBe fresh.renderCustom(instant)
      }
    }
  }

  it should "hand every thread the same instance on a format's first use" in {
    // a format no other test uses: its first use is this one
    val c = column(Some("dd.MM.yyyy'T'HH:mm:ss.SSS||epoch_second"))
    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    try {
      val tasks = (1 to threads).map { _ =>
        pool.submit(new Callable[FieldFormat] {
          override def call(): FieldFormat = {
            start.await(10, TimeUnit.SECONDS)
            FieldFormat.of(c)
          }
        })
      }
      start.countDown()
      val built = tasks.map(_.get(30, TimeUnit.SECONDS))
      built.forall(_ eq built.head) shouldBe true
    } finally pool.shutdownNow()
  }

  // the GLOBAL format memo is never filled in a test: its cap is proven on an instance of its own
  "BoundedMemo" should "store one value per key up to its cap, and compute every other key on each call" in {
    val memo = new BoundedMemo[String, FieldFormat](cap = 2)
    var computed = 0
    def compute(spec: String): FieldFormat = {
      computed += 1
      FieldFormat(spec)
    }
    val a = memo("epoch_millis")(compute("epoch_millis"))
    val b = memo("yyyy/MM/dd")(compute("yyyy/MM/dd"))
    (memo("epoch_millis")(compute("epoch_millis")) eq a) shouldBe true
    (memo("yyyy/MM/dd")(compute("yyyy/MM/dd")) eq b) shouldBe true
    computed shouldBe 2
    memo.size shouldBe 2
    // past the cap: computed, answered, not stored
    val c1 = memo("epoch_second")(compute("epoch_second"))
    val c2 = memo("epoch_second")(compute("epoch_second"))
    c1 shouldBe FieldFormat("epoch_second")
    c2 shouldBe c1
    (c2 eq c1) shouldBe false
    computed shouldBe 4
    memo.size shouldBe 2
    // the stored keys still answer their instance
    (memo("epoch_millis")(compute("epoch_millis")) eq a) shouldBe true
    computed shouldBe 4
  }
}
