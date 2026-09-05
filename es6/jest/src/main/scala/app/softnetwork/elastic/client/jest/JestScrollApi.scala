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

package app.softnetwork.elastic.client.jest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.{
  endOfScrollPage,
  retryWithBackoff,
  ClientAggregation,
  ConversionContext,
  ElasticQuery,
  ScrollApi
}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.google.gson.{JsonNull, JsonObject, JsonParser}
import io.searchbox.client.JestResult
import io.searchbox.core.{ClearScroll, Search, SearchScroll}
import io.searchbox.params.Parameters

import java.io.IOException
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait JestScrollApi extends ScrollApi with JestClientHelpers {
  _: JestVersionApi with JestSearchApi with JestSettingsApi with JestClientCompanion =>

  /** Classic scroll (works for both hits and aggregations)
    */
  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    config: ScrollConfig
  )(implicit
    system: ActorSystem,
    context: ConversionContext
  ): Source[ListMap[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Source
      .unfoldAsync[Option[String], Seq[ListMap[String, Any]]](None) { scrollIdOpt =>
        retryWithBackoff(config.retryConfig) {
          Future {
            scrollIdOpt match {
              case None =>
                logger.info(
                  s"Starting classic scroll on indices: ${elasticQuery.indices.mkString(", ")}"
                )

                val searchBuilder =
                  new Search.Builder(elasticQuery.query)
                    .setParameter(Parameters.SIZE, config.scrollSize)
                    .setParameter(Parameters.SCROLL, config.keepAlive)

                for (indice <- elasticQuery.indices) searchBuilder.addIndex(indice)
                for (t      <- elasticQuery.types) searchBuilder.addType(t)

                val result = apply().execute(searchBuilder.build())
                if (!result.isSucceeded) {
                  throw new IOException(s"Initial scroll failed: ${result.getErrorMessage}")
                }

                val scrollId = result.getJsonObject.get("_scroll_id").getAsString

                val rawHits = rawHitCount(result)

                try {
                  // Extract ALL results (hits + aggregations)
                  // Single parse for core (#228): Jackson reads the raw response body Jest
                  // retained — the Gson tree is only consulted for the scroll cursor.
                  val results =
                    extractAllResults(
                      result.getJsonString,
                      fieldAliases,
                      aggregations,
                      config.retainDocumentId
                    )

                  logger.info(
                    s"Initial scroll returned ${results.size} results, scrollId: $scrollId"
                  )

                  // End of stream is decided on the RAW page (#241): a page that carried hits but
                  // produced no rows is a silent drop and must fail the stream, not end it.
                  if (endOfScrollPage(results.size, rawHits, s"Initial scroll page [$scrollId]")) {
                    // nothing left to read — release the context we just opened
                    clearScroll(scrollId)
                    None
                  } else {
                    Some((Some(scrollId), results))
                  }
                } catch {
                  case ex: Throwable =>
                    // `scrollIdOpt` is still None on the first page, so the stream-level recovery
                    // below cannot release the context opened just above (AD-S1-2)
                    clearScroll(scrollId)
                    throw ex
                }

              case Some(scrollId) =>
                logger.debug(s"Fetching next scroll batch (scrollId: $scrollId)")

                val scrollBuilder = new SearchScroll.Builder(scrollId, config.keepAlive)

                val result = apply().execute(scrollBuilder.build())
                if (!result.isSucceeded) {
                  // Lancer une exception pour trigger le retry
                  throw new IOException(s"Scroll failed: ${result.getErrorMessage}")
                }
                val newScrollId = result.getJsonObject.get("_scroll_id").getAsString
                val rawHits = rawHitCount(result)

                try {
                  val results =
                    extractAllResults(
                      result.getJsonString,
                      fieldAliases,
                      aggregations,
                      config.retainDocumentId
                    )

                  logger.debug(s"Scroll returned ${results.size} results")

                  if (endOfScrollPage(results.size, rawHits, s"Scroll page [$scrollId]")) {
                    clearScroll(scrollId)
                    None
                  } else {
                    Some((Some(newScrollId), results))
                  }
                } catch {
                  case ex: Throwable =>
                    // the cursor already advanced: the stream-level recovery below only releases
                    // the spent `scrollId`, so release the new one here too (AD-S1-2)
                    Option(newScrollId).filter(_ != scrollId).foreach(clearScroll)
                    throw ex
                }
            }
          }
        }(system, logger).recoverWith { case ex: Exception =>
          logger.error(s"Scroll failed after retries: ${ex.getMessage}", ex)
          scrollIdOpt.foreach(clearScroll)
          // fail the stream instead of ending it: ending here would surface a silently
          // truncated result set as a SUCCESSFUL result (#228 review; same defect class as
          // #209/#224)
          Future.failed(ex)
        }
      }
      .mapConcat(identity)
  }

  /** Search After (only for hits, more efficient)
    */
  override private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit
    system: ActorSystem,
    context: ConversionContext
  ): Source[ListMap[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Source
      .unfoldAsync[Option[Seq[Any]], Seq[ListMap[String, Any]]](None) { searchAfterOpt =>
        retryWithBackoff(config.retryConfig) {
          Future {
            searchAfterOpt match {
              case None =>
                logger.info(
                  s"Starting search_after on indices: ${elasticQuery.indices.mkString(", ")}"
                )
              case Some(values) =>
                logger.debug(s"Fetching next search_after batch (after: ${values.mkString(", ")})")
            }

            val queryJson = JsonParser.parseString(elasticQuery.query).getAsJsonObject

            // Check if sorts already exist in the query
            if (!hasSorts && !queryJson.has("sort")) {
              // No sorting defined, add _id by default
              logger.warn(
                "No sort fields in query for search_after, adding default _id sort. " +
                "This may lead to inconsistent results if documents are updated during scroll."
              )
              val sortArray = new com.google.gson.JsonArray()
              val sortObj = new JsonObject()
              sortObj.addProperty("_id", "asc")
              sortArray.add(sortObj)
              queryJson.add("sort", sortArray)
            } else if (hasSorts && queryJson.has("sort")) {
              // Sorts already present, check that a tie-breaker exists
              val existingSorts = queryJson.getAsJsonArray("sort")
              val hasIdSort = existingSorts.asScala.exists { sortElem =>
                sortElem.isJsonObject && sortElem.getAsJsonObject.has("_id")
              }
              if (!hasIdSort) {
                // Add _id as tie-breaker
                logger.debug("Adding _id as tie-breaker to existing sorts")
                val tieBreaker = new JsonObject()
                tieBreaker.addProperty("_id", "asc")
                existingSorts.add(tieBreaker)
              }
            }

            queryJson.addProperty("size", config.scrollSize)

            // Add search_after
            searchAfterOpt.foreach { searchAfter =>
              val searchAfterArray = new com.google.gson.JsonArray()
              searchAfter.foreach {
                case s: String  => searchAfterArray.add(s)
                case n: Number  => searchAfterArray.add(n)
                case b: Boolean => searchAfterArray.add(b)
                case null       => searchAfterArray.add(JsonNull.INSTANCE)
                case other      => searchAfterArray.add(other.toString)
              }
              queryJson.add("search_after", searchAfterArray)
            }

            val searchBuilder = new Search.Builder(queryJson.toString)
            for (indice <- elasticQuery.indices) searchBuilder.addIndex(indice)
            for (t      <- elasticQuery.types) searchBuilder.addType(t)

            val result = apply().execute(searchBuilder.build())

            if (!result.isSucceeded) {
              throw new IOException(s"Search after failed: ${result.getErrorMessage}")
            }
            // Extract ONLY hits (no aggregations)
            // Single parse for core (#228): raw body, not the Gson tree re-serialized
            val hits =
              extractHitsOnly(result.getJsonString, fieldAliases, config.retainDocumentId)

            // End of stream is decided on the RAW page (#241), never on the converted rows
            if (endOfScrollPage(hits.size, rawHitCount(result), "search_after page")) {
              None
            } else {
              val hitsArray = result.getJsonObject
                .getAsJsonObject("hits")
                .getAsJsonArray("hits")

              val lastHit = hitsArray.get(hitsArray.size() - 1).getAsJsonObject
              val nextSearchAfter = if (lastHit.has("sort")) {
                Some(
                  lastHit
                    .getAsJsonArray("sort")
                    .asScala
                    .map { elem =>
                      if (elem.isJsonPrimitive) {
                        val prim = elem.getAsJsonPrimitive
                        if (prim.isString) prim.getAsString
                        else if (prim.isBoolean) prim.getAsBoolean
                        else if (prim.isNumber) {
                          val num = prim.getAsNumber
                          if (num.toString.contains(".")) num.doubleValue()
                          else num.longValue()
                        } else prim.getAsString
                      } else if (elem.isJsonNull) {
                        null
                      } else {
                        elem.toString
                      }
                    }
                    .toSeq
                )
              } else {
                None
              }

              Some((nextSearchAfter, hits))
            }
          }
        }(system, logger).recoverWith { case ex: Exception =>
          logger.error(s"Search after failed after retries: ${ex.getMessage}", ex)
          // fail the stream instead of ending it: ending here would surface a silently
          // truncated result set as a SUCCESSFUL result (#228 review; same defect class as
          // #209/#224)
          Future.failed(ex)
        }
      }
      .mapConcat(identity)
  }

  override private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit
    system: ActorSystem,
    context: ConversionContext
  ): Source[ListMap[String, Any], NotUsed] =
    throw new NotImplementedError("PIT search after not implemented for Elasticsearch 6")

  /** Number of RAW hits the page carried — the end-of-stream signal (#241).
    *
    * One Gson tree read per page (the tree Jest already parsed), never per row. A response with no
    * `hits` section — an aggregation-only page — legitimately reports 0.
    */
  private def rawHitCount(result: JestResult): Int =
    Try(result.getJsonObject.getAsJsonObject("hits").getAsJsonArray("hits").size()).getOrElse(0)

  /** Extract ALL results: hits + aggregations
    *
    * A parse failure FAILS the page (non-retriable `IllegalStateException`): returning an empty
    * page here used to read as "end of stream" and surfaced a silently truncated result as a
    * success (#241/#217, same defect class as #228 / #209 / #224).
    */
  private[client] def extractAllResults(
    jsonString: String,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    retainDocumentId: Boolean
  )(implicit context: ConversionContext): Seq[ListMap[String, Any]] = {
    parseResponse(
      jsonString,
      fieldAliases,
      aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2)),
      retainDocumentId = retainDocumentId
    ) match {
      case Success(rows) => rows
      case Failure(ex) =>
        throw new IllegalStateException(s"Failed to parse scroll page: ${ex.getMessage}", ex)
    }
  }

  /** Extract ONLY hits (for search_after)
    *
    * A parse failure FAILS the page (non-retriable `IllegalStateException`) — see
    * [[extractAllResults]]. Jest has no PIT path (`pitSearchAfter` is a `NotImplementedError`), so
    * `search_after` is this client's only deep-paging alternative to the classic scroll.
    */
  private[client] def extractHitsOnly(
    jsonString: String,
    fieldAliases: ListMap[String, String],
    retainDocumentId: Boolean
  )(implicit context: ConversionContext): Seq[ListMap[String, Any]] = {

    parseResponse(
      jsonString,
      fieldAliases,
      ListMap.empty,
      retainDocumentId = retainDocumentId
    ) match {
      case Success(rows) => rows
      case Failure(ex) =>
        throw new IllegalStateException(
          s"Failed to parse search_after page: ${ex.getMessage}",
          ex
        )
    }
  }

  private def clearScroll(scrollId: String): Unit = {
    Try {
      logger.debug(s"Clearing Jest scroll: $scrollId")
      val clearScroll = new ClearScroll.Builder()
        .addScrollId(scrollId)
        .build()
      apply().execute(clearScroll)
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to clear Jest scroll $scrollId: ${ex.getMessage}")
    }
  }
}
