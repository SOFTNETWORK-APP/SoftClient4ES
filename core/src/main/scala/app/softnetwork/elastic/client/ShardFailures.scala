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

import app.softnetwork.elastic.client.result.ElasticError
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

/** The shards of ONE search response that failed, read from its `_shards` section.
  *
  * Elasticsearch answers a search on which SOME shards fail with HTTP 200: `_shards.failed > 0`,
  * one entry per distinct failure under `_shards.failures`, and the hits / buckets of the shards
  * that succeeded only. Read as an answer, that is a partial result with no error, so every search
  * door reads this section: the scroll / PIT doors throw with [[reasons]], the one-shot doors
  * answer [[ShardFailures.error]].
  *
  * `skipped` shards are not failures (a pre-filter proved they hold no match), so only `failed`
  * counts. `timed_out` is a different signal and is not read here.
  *
  * @param failed
  *   `_shards.failed`, > 0
  * @param total
  *   `_shards.total`
  * @param failures
  *   the `_shards.failures` entries (Elasticsearch groups identical failures, so there may be fewer
  *   entries than failed shards, or none)
  */
private[client] final case class ShardFailures(failed: Int, total: Int, failures: Seq[JsonNode]) {

  /** Every failure's reason, `; `-separated — the message of the scroll / PIT doors. */
  def reasons: String =
    if (failures.nonEmpty)
      failures
        .map { failure =>
          val reason = failure.path("reason")
          val message = reason.path("reason")
          if (message.isTextual) message.asText() else reason.toString
        }
        .mkString("; ")
    else "Unknown shard failure"

  /** `n of m shards failed`, then the first failure's type, reason and cause. */
  def message: String =
    failures.headOption.map(ShardFailures.describe) match {
      case Some(first) if first.nonEmpty => s"$failed of $total shards failed: $first"
      case _                             => s"$failed of $total shards failed"
    }

  /** 400 when EVERY failure is one Elasticsearch itself answers as the caller's (a script error, a
    * query that does not apply to the shard's mapping, ...); otherwise undetermined (`None`).
    */
  def statusCode: Option[Int] =
    if (
      failures.nonEmpty &&
      failures.forall(f => ShardFailures.CallerErrorTypes.contains(ShardFailures.typeOf(f)))
    ) Some(400)
    else None

  def toError: ElasticError = ElasticError(message = message, statusCode = statusCode)
}

private[client] object ShardFailures {

  /** The failure types Elasticsearch answers with 400 (BAD_REQUEST) when every shard fails with
    * them: the request, not the cluster, is at fault.
    */
  val CallerErrorTypes: Set[String] = Set(
    "script_exception",
    "query_shard_exception",
    "illegal_argument_exception",
    "parsing_exception",
    "search_parse_exception",
    "x_content_parse_exception"
  )

  /** The failed shards of one search response, or `None` when no shard failed. */
  def of(response: JsonNode): Option[ShardFailures] = {
    val shards = response.path("_shards")
    val failed = shards.path("failed").asInt(0)
    if (failed > 0) {
      val failures = shards.path("failures")
      Some(
        ShardFailures(
          failed = failed,
          total = shards.path("total").asInt(0),
          failures =
            if (failures.isArray) failures.elements().asScala.toList
            else Nil
        )
      )
    } else None
  }

  /** The error a one-shot search answers when some of its shards failed, or `None`.
    *
    * A multi search is read per `responses[i]`: the first response whose shards failed answers the
    * error, prefixed with its 1-based position as the multi-search errors are. An item that failed
    * as a whole carries an `error` instead of `_shards` and is left to the response parser.
    */
  def error(tree: JsonNode): Option[ElasticError] = {
    val responses = tree.path("responses")
    if (responses.isArray) {
      responses
        .elements()
        .asScala
        .zipWithIndex
        .map { case (response, i) =>
          of(response).map(f => f.toError.copy(message = s"Query ${i + 1}: ${f.message}"))
        }
        .collectFirst { case Some(error) => error }
    } else of(tree).map(_.toError)
  }

  private[client] def typeOf(failure: JsonNode): String =
    failure.path("reason").path("type").asText("")

  /** WHY a shard failed (`For input string: "x"` under a script's `runtime error`), from a
    * `failed_shards` array: the deepest reason of each failure's `reason.caused_by…` chain, the
    * first one `said` does not hold. Elasticsearch gives it there only: the `root_cause` entries do
    * not carry the `caused_by`. Every client appends it to the message of a failed search.
    */
  private[client] def shardReason(failedShards: JsonNode, said: Seq[String]): Option[String] =
    if (!failedShards.isArray) None
    else
      failedShards
        .elements()
        .asScala
        .flatMap { failure =>
          Iterator
            .iterate(failure.path("reason"))(_.path("caused_by"))
            .takeWhile(_.isObject)
            .take(10)
            .map(_.path("reason"))
            .filter(_.isTextual)
            .map(_.asText())
            .toList
            .lastOption
        }
        .find(r => !said.contains(r))

  /** `type: reason (caused by type: reason)`, from what the failure carries. */
  private[client] def describe(failure: JsonNode): String = {
    def typeAndReason(cause: JsonNode): String = {
      val t = cause.path("type").asText("")
      val r = cause.path("reason").asText("")
      Seq(t, r).filter(_.nonEmpty).mkString(": ")
    }
    val reason = failure.path("reason")
    if (reason.isTextual) reason.asText()
    else {
      val head = typeAndReason(reason)
      val causedBy = typeAndReason(reason.path("caused_by"))
      if (causedBy.nonEmpty) s"$head (caused by $causedBy)" else head
    }
  }
}
