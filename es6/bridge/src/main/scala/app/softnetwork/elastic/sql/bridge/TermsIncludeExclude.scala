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

package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.query.BucketIncludesExcludes
import app.softnetwork.elastic.sql.toRegex
import com.sksamuel.elastic4s.searches.aggs.{AbstractAggregation, Aggregation, TermsAggregation}

/** How a GROUP BY key filter is written into an Elasticsearch 6 `terms` aggregation. ES 6 only:
  * elastic4s 7.17 / 8.18 / 9.0 keep exact values and a pattern apart (`includeExactValues` /
  * `includeRegex`) and always write exact values as an array.
  *
  * ==The wire rule this object owns==
  * elastic4s 6.7.8 keeps ONE list per side and its `TermsAggregationBuilder` decides the JSON by
  * COUNT: one entry is written as a bare string, two or more as an array. Elasticsearch 6.8 reads a
  * bare string as a Lucene REGEX (`IncludeExclude.parseInclude`) and an array as exact terms. So a
  * ONE-value key filter reached 6.8 as a pattern: `HAVING s = 'a.b'` also kept `axb`; on a numeric
  * or boolean key the search failed (a regex on a field that is not a string); beside a list of
  * several values in the other side it was a regex meeting a set, which 6.8 rejects ("Cannot mix a
  * set-based include with a regex-based method"). Upstream: elastic4s#1627, fixed on the 7.x line
  * only.
  *
  * ==The two spellings of a one-value side==
  * A pattern, and a list of two or more values, are written exactly as before. A ONE-value side is
  * written:
  *   - by default as the value TWICE, `["v","v"]`: elastic4s takes its array branch and 6.8 reads
  *     an exact set, collapsing the duplicate. An array is the only form a numeric or boolean key
  *     accepts, and a duplicate -- unlike a filler value -- can never match a term nobody named.
  *   - beside a PATTERN in the other side, as an escaped exact-match pattern: 6.8 cannot mix a
  *     pattern with a set, and `LIKE 'a%' AND <> 'b'` must keep working. A pattern only ever meets
  *     a string key (a pattern on any other key fails whatever the other side holds), so the regex
  *     form loses nothing.
  *
  * ==What stays inexpressible on 6.8==
  * A pattern in one side and two or more values in the other. [[unmixableIn]] finds it on the built
  * request by this same rule, and the ES 6 serializers refuse it by name.
  */
object TermsIncludeExclude {

  /** `terms` with the key filter of one grouping level written for Elasticsearch 6. */
  def apply(
    terms: TermsAggregation,
    include: BucketIncludesExcludes,
    exclude: BucketIncludesExcludes
  ): TermsAggregation = {
    val inc = entries(include, other = exclude)
    val exc = entries(exclude, other = include)
    // The Iterable overload: the String one would drop an empty entry.
    if (inc.isEmpty && exc.isEmpty) terms else terms.includeExclude(inc, exc)
  }

  /** The entries elastic4s 6.7.8 writes for ONE side. A pattern wins over values -- the precedence
    * both bridges have always applied; a side carrying both is refused before emission
    * (`SingleSearch.keyChannelConflict`).
    */
  def entries(side: BucketIncludesExcludes, other: BucketIncludesExcludes): Seq[String] =
    side match {
      case BucketIncludesExcludes(_, Some(regex)) if regex.nonEmpty => Seq(regex)
      case BucketIncludesExcludes(values, _) if values.size == 1 =>
        val value = values.head
        if (isPattern(other)) Seq(literalRegex(value)) else Seq(value, value)
      case BucketIncludesExcludes(values, _) if values.nonEmpty => values.toSeq
      case _                                                    => Nil
    }

  private def isPattern(side: BucketIncludesExcludes): Boolean = side.regex.exists(_.nonEmpty)

  /** `value` as a Lucene regular expression that matches exactly `value`.
    *
    * The metacharacter set is NOT copied here: each character is handed to the SHARED `toRegex`,
    * which escapes the union of the Java and Lucene metacharacters. The two characters `toRegex`
    * TRANSLATES instead of escaping -- `%` and `_`, the LIKE wildcards -- are literal in an exact
    * value and special to neither regex engine, so they pass through unchanged.
    */
  def literalRegex(value: String): String = {
    val out = new StringBuilder(value.length * 2)
    value.foreach {
      case c @ ('%' | '_') => out.append(c)
      case c               => out.append(toRegex(c.toString))
    }
    out.toString
  }

  /** Whether Elasticsearch 6.8 rejects this pair of written sides outright: one entry is a pattern
    * on the wire, two or more a set, and 6.8 cannot apply a pattern beside a set.
    */
  def unmixable(include: Seq[String], exclude: Seq[String]): Boolean =
    (include.size == 1 && exclude.size > 1) || (include.size > 1 && exclude.size == 1)

  /** The name of the first `terms` aggregation, anywhere under `aggs`, whose include and exclude
    * Elasticsearch 6.8 would reject together -- read from the BUILT request with the same wire
    * rule, so the refusal and the spelling cannot disagree.
    */
  def unmixableIn(aggs: Iterable[AbstractAggregation]): Option[String] =
    aggs.iterator
      .map {
        case terms: TermsAggregation =>
          (if (unmixable(terms.includes, terms.excludes)) Some(terms.name) else None)
            .orElse(unmixableIn(terms.subaggs))
        case agg: Aggregation => unmixableIn(agg.subaggs)
        case _                => None
      }
      .collectFirst { case Some(name) => name }
}
