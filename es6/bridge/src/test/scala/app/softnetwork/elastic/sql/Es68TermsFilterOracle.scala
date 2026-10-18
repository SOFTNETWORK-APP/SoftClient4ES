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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.lucene.util.{BytesRef, NumericUtils}
import org.elasticsearch.common.xcontent.{DeprecationHandler, NamedXContentRegistry, XContentParser}
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.search.DocValueFormat
import org.elasticsearch.search.aggregations.bucket.terms.{IncludeExclude => Es68IncludeExclude}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** What Elasticsearch 6.8.23 KEEPS for a `terms` include / exclude the es6 bridge emits, decided by
  * Elasticsearch 6.8.23's OWN code, in process: the ES 6.8 server jar and Lucene 7.7.3 are on this
  * module's test classpath through `elasticDependencies`. Public, so later key-filter work reuses
  * it instead of copying it.
  *
  * It replays what 6.8 does with the request body, calling its classes. Anchors, in the cached
  * `elasticsearch-6.8.23-sources.jar`, `org/elasticsearch/search/aggregations/bucket/terms/`:
  *   - `TermsAggregationBuilder.java:91` / `:94` (the `PARSER`): `include` ->
  *     `IncludeExclude.merge(parseInclude(p), cur)`, then `exclude` -> `merge(cur,
  *     parseExclude(p))`; a regex beside a set throws there (`IncludeExclude.java:85`, "Cannot mix
  *     ..."), which 6.8 answers with HTTP 400;
  *   - the filter the aggregator then builds: `convertToStringFilter(RAW)` for a keyword key (the
  *     same automaton and term sets as the `convertToOrdinalsFilter` the real aggregator uses),
  *     `convertToLongFilter(RAW)`, `convertToDoubleFilter()`, `convertToLongFilter(BOOLEAN)`. ONLY
  *     `TermsAggregatorFactory.doCreateInternal`'s two regex checks are TRANSCRIBED
  *     (`TermsAggregatorFactory.java:149-152` and `:158-161`: for the four key types modelled here,
  *     a regex on anything but a keyword key throws `AggregationExecutionException`, HTTP 500). A
  *     regex-based `convertToLongFilter` accepts everything, so skipping them would certify a
  *     numeric regex.
  *
  * The same statements run on a real Elasticsearch 6.8.23 in the testkit's
  * `GroupByCompletenessSpec` (same alphabet, both ES 6 clients). A disagreement between that run
  * and this model means the model is not faithful.
  */
object Es68TermsFilterOracle {

  sealed trait KeyType
  case object KeywordKey extends KeyType
  case object LongKey extends KeyType
  case object DoubleKey extends KeyType
  case object BooleanKey extends KeyType

  sealed trait Verdict
  final case class Kept(keys: Set[String]) extends Verdict
  final case class Failed(status: Int, reason: String) extends Verdict

  private val mapper = new ObjectMapper()

  /** Every `terms` body of a request, keyed by its aggregation PATH (`outer>inner`), in order. Two
    * levels may share a bare name -- after one resolution the two buckets of a self-join are both
    * named after the column -- so a bare name is never a key.
    */
  def termsBodies(query: String): Seq[(String, JsonNode)] = {
    def walk(aggs: JsonNode, prefix: String): Seq[(String, JsonNode)] =
      Option(aggs).toSeq.flatMap(_.fields().asScala.toList).flatMap { e =>
        val path = if (prefix.isEmpty) e.getKey else s"$prefix>${e.getKey}"
        Option(e.getValue.get("terms")).map(path -> _).toSeq ++ walk(e.getValue.get("aggs"), path)
      }
    walk(mapper.readTree(query).get("aggs"), "")
  }

  /** The ONE `terms` body whose last path segment is `name`: fails when absent or ambiguous. */
  def termsBody(query: String, name: String): JsonNode =
    termsBodies(query).filter(_._1.split('>').last == name) match {
      case Seq((_, body)) => body
      case Seq()          => throw new NoSuchElementException(s"no terms aggregation named $name")
      case many =>
        throw new IllegalStateException(
          s"ambiguous terms aggregation name $name: ${many.map(_._1).mkString(", ")}"
        )
    }

  private def parserOf(node: JsonNode): XContentParser = {
    val parser = JsonXContent.jsonXContent.createParser(
      NamedXContentRegistry.EMPTY,
      DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
      mapper.writeValueAsString(node)
    )
    parser.nextToken() // VALUE_STRING or START_ARRAY -- where the ObjectParser hands it over
    parser
  }

  // Fallback if JsonXContent ever cannot initialise in the test JVM (it does in the unforked es6
  // bridge one, on the project's own jackson-core): build the IncludeExclude with its public
  // constructors, dispatching on the Jackson node exactly as parseInclude dispatches on
  // the token -- textual => `new Es68IncludeExclude(text, null: String)`; array =>
  // `new Es68IncludeExclude(values: Array[String], null: Array[String])` -- then `merge`.

  /** What Elasticsearch 6.8.23 keeps, out of `keys`, for one emitted `terms` body. */
  def keptBy(terms: JsonNode, keyType: KeyType, keys: Seq[String]): Verdict =
    Try {
      var ie: Es68IncludeExclude = null
      Option(terms.get("include")).foreach { n =>
        ie = Es68IncludeExclude.merge(Es68IncludeExclude.parseInclude(parserOf(n)), ie)
      }
      Option(terms.get("exclude")).foreach { n =>
        ie = Es68IncludeExclude.merge(ie, Es68IncludeExclude.parseExclude(parserOf(n)))
      }
      Option(ie)
    } match {
      case Failure(e)    => Failed(400, String.valueOf(e.getMessage))
      case Success(None) => Kept(keys.toSet)
      // TRANSCRIBED -- TermsAggregatorFactory.java:149-152 and :158-161 (see the scaladoc).
      case Success(Some(ie)) if ie.isRegexBased && keyType != KeywordKey =>
        Failed(500, "cannot support regular expression style include/exclude settings")
      case Success(Some(ie)) =>
        Try(keyType match {
          case KeywordKey =>
            val filter = ie.convertToStringFilter(DocValueFormat.RAW)
            keys.filter(k => filter.accept(new BytesRef(k)))
          case LongKey =>
            val filter = ie.convertToLongFilter(DocValueFormat.RAW)
            keys.filter(k => filter.accept(k.toLong))
          case DoubleKey =>
            val filter = ie.convertToDoubleFilter()
            keys.filter(k => filter.accept(NumericUtils.doubleToSortableLong(k.toDouble)))
          case BooleanKey =>
            val filter = ie.convertToLongFilter(DocValueFormat.BOOLEAN)
            keys.filter(k => filter.accept(if (k.toBoolean) 1L else 0L))
        }) match {
          case Success(kept) => Kept(kept.toSet)
          case Failure(e)    => Failed(500, String.valueOf(e.getMessage))
        }
    }
}
