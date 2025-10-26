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

import app.softnetwork.elastic.sql.query.SQLAggregation
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.{Extraction, Formats}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.util.Try

import scala.jdk.CollectionConverters._

trait ElasticConversion {
  private[this] val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.registerModule(new JavaTimeModule())

  // Ignore unknown properties during deserialization
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def convertTo[T](map: Map[String, Any])(implicit m: Manifest[T], formats: Formats): T = {
    val jValue = Extraction.decompose(map)
    jValue.extract[T]
  }

  def convertTo[T](response: SQLSearchResponse)(implicit
    m: Manifest[T],
    formats: Formats
  ): Try[Seq[T]] = {
    parseResponse(response).map { rows =>
      rows.map { row =>
        convertTo[T](row)(m, formats)
      }
    }
  }

  // Formatters for elasticsearch ISO 8601 date/time strings
  private val isoDateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME
  private val isoDateFormatter = DateTimeFormatter.ISO_DATE
  private val isoTimeFormatter = DateTimeFormatter.ISO_TIME

  /** main entry point : parse json response from elasticsearch Handles both single search and
    * multi-search (msearch/UNION ALL) responses
    */
  def parseResponse(
    response: SQLSearchResponse
  ): Try[Seq[Map[String, Any]]] = {
    val json = mapper.readTree(response.results)
    // Check if it's a multi-search response (array of responses)
    if (json.isArray) {
      parseMultiSearchResponse(json, response.fieldAliases, response.aggregations)
    } else {
      // Single search response
      parseSingleSearchResponse(json, response.fieldAliases, response.aggregations)
    }
  }

  /** Parse a multi-search response (array of search responses) Used for UNION ALL queries
    */
  def parseMultiSearchResponse(
    jsonArray: JsonNode,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Try[Seq[Map[String, Any]]] =
    Try {
      val responses = jsonArray.elements().asScala.toList

      // Collect all errors
      val errors = responses.zipWithIndex.collect {
        case (response, idx) if response.has("error") =>
          val errorMsg = Option(response.get("error").get("reason"))
            .map(_.asText())
            .getOrElse("Unknown error")
          s"Query ${idx + 1}: $errorMsg"
      }

      if (errors.nonEmpty) {
        throw new Exception(s"Elasticsearch errors in multi-search:\n${errors.mkString("\n")}")
      } else {
        // Parse each response and combine all rows
        val allRows = responses.flatMap { response =>
          if (!response.has("error")) {
            jsonToRows(response, fieldAliases, aggregations)
          } else {
            Seq.empty
          }
        }
        allRows
      }
    }

  /** Parse a single search response
    */
  def parseSingleSearchResponse(
    json: JsonNode,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Try[Seq[Map[String, Any]]] =
    Try {
      // check if it is an error response
      if (json.has("error")) {
        val errorMsg = Option(json.get("error").get("reason"))
          .map(_.asText())
          .getOrElse("Unknown Elasticsearch error")
        throw new Exception(s"Elasticsearch error: $errorMsg")
      } else {
        jsonToRows(json, fieldAliases, aggregations)
      }
    }

  /** convert JsonNode to Rows
    */
  def jsonToRows(
    json: JsonNode,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {
    val hitsNode = Option(json.path("hits").path("hits"))
      .filter(_.isArray)
      .map(_.elements().asScala.toList)

    val aggsNode = Option(json.path("aggregations"))
      .filter(!_.isMissingNode)

    (hitsNode, aggsNode) match {
      case (Some(hits), None) if hits.nonEmpty =>
        // Case 1 : only hits
        parseSimpleHits(hits, fieldAliases)

      case (None, Some(aggs)) =>
        // Case 2 : only aggregations
        parseAggregations(aggs, Map.empty, fieldAliases, aggregations)

      case (Some(hits), Some(aggs)) if hits.isEmpty =>
        // Case 3 : aggregations with no hits
        parseAggregations(aggs, Map.empty, fieldAliases, aggregations)

      case (Some(hits), Some(aggs)) if hits.nonEmpty =>
        // Case 4 : Hits + global aggregations
        val globalMetrics = extractGlobalMetrics(aggs)
        hits.map { hit =>
          val source = extractSource(hit, fieldAliases)
          val metadata = extractHitMetadata(hit)
          val innerHits = extractInnerHits(hit, fieldAliases)
          globalMetrics ++ source ++ metadata ++ innerHits
        }

      case _ =>
        Seq.empty
    }
  }

  /** Parse simple hits (without aggregations)
    */
  def parseSimpleHits(
    hits: List[JsonNode],
    fieldAliases: Map[String, String]
  ): Seq[Map[String, Any]] = {
    hits.map { hit =>
      val source = extractSource(hit, fieldAliases)
      val metadata = extractHitMetadata(hit)
      val innerHits = extractInnerHits(hit, fieldAliases)
      source ++ metadata ++ innerHits
    }
  }

  /** Extract hit metadata (_id, _index, _score)
    */
  def extractHitMetadata(hit: JsonNode): Map[String, Any] = {
    Map(
      "_id"    -> Option(hit.get("_id")).map(_.asText()),
      "_index" -> Option(hit.get("_index")).map(_.asText()),
      "_score" -> Option(hit.get("_score")).map(n =>
        if (n.isDouble || n.isFloat) n.asDouble() else n.asLong().toDouble
      )
    ).collect { case (k, Some(v)) => k -> v }
  }

  /** Extract hit _source
    */
  def extractSource(hit: JsonNode, fieldAliases: Map[String, String]): Map[String, Any] = {
    Option(hit.get("_source"))
      .filter(_.isObject)
      .map(jsonNodeToMap(_, fieldAliases))
      .getOrElse(Map.empty)
  }

  /** Extract inner_hits from a hit (for nested or parent-child queries) */
  private def extractInnerHits(
    hit: JsonNode,
    fieldAliases: Map[String, String]
  ): Map[String, Any] = {
    Option(hit.get("inner_hits"))
      .filter(_.isObject)
      .map { innerHitsNode =>
        innerHitsNode
          .properties()
          .asScala
          .map { entry =>
            val innerHitName = entry.getKey
            val innerHitData = entry.getValue

            // Extract the hits array from inner_hits
            val innerHitsList = Option(innerHitData.path("hits").path("hits"))
              .filter(_.isArray)
              .map { hitsArray =>
                hitsArray
                  .elements()
                  .asScala
                  .map { innerHit =>
                    // Extract source and metadata for each inner hit
                    val source = extractSource(innerHit, fieldAliases)
                    val metadata = extractHitMetadata(innerHit)

                    // Recursively handle nested inner_hits if present
                    val nestedInnerHits = extractInnerHits(innerHit, fieldAliases)

                    source ++ metadata ++ nestedInnerHits
                  }
                  .toList
              }
              .getOrElse(List.empty)

            innerHitName -> innerHitsList
          }
          .toMap
      }
      .getOrElse(Map.empty)
  }

  /** Parse recursively aggregations from Elasticsearch response with parent context
    */
  def parseAggregations(
    aggsNode: JsonNode,
    parentContext: Map[String, Any],
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {

    if (aggsNode.isMissingNode || !aggsNode.isObject) {
      return Seq.empty
    }

    // Find all buckets
    val bucketAggs = aggsNode
      .properties()
      .asScala
      .flatMap { entry =>
        val aggName = normalizeAggregationKey(entry.getKey)
        val aggValue = entry.getValue

        // Détecter les agrégations avec buckets
        Option(aggValue.get("buckets"))
          .filter(n => n.isArray || n.isObject)
          .map { buckets =>
            val bucketsList = if (buckets.isArray) {
              buckets.elements().asScala.toList
            } else {
              // Named buckets (filters aggregation)
              buckets
                .properties()
                .asScala
                .map { bucketEntry =>
                  val bucketNode = mapper.createObjectNode()
                  bucketNode.put("key", bucketEntry.getKey)
                  bucketNode.setAll(
                    bucketEntry.getValue
                      .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
                  )
                  bucketNode
                }
                .toList
            }
            (aggName, bucketsList, aggValue)
          }
      }
      .toList

    if (bucketAggs.isEmpty) {
      // No buckets : it is a leaf aggregation (metrics or top_hits)
      val metrics = extractMetrics(aggsNode)
      val allTopHits = extractAllTopHits(aggsNode)

      if (allTopHits.nonEmpty) {
        // Process each top_hits aggregation with their names
        val topHitsData = allTopHits.map { case (topHitName, hits) =>
          val processedHits = hits.map { hit =>
            val source = extractSource(hit, fieldAliases)
            val metadata = extractHitMetadata(hit)
            val innerHits = extractInnerHits(hit, fieldAliases)
            source ++ metadata ++ innerHits
          }

          // Determine if it is a multivalued aggregation
          val isMultipleValues = aggregations.get(topHitName) match {
            case Some(agg) => agg.multivalued
            case None      =>
              // Fallback on naming convention if aggregation is not found
              !topHitName.toLowerCase.matches("(first|last)_.*")
          }

          // If multipleValues = true OR more than one hit, return a list
          // If multipleValues = false AND only one hit, return an object
          topHitName -> (if (!isMultipleValues && processedHits.size == 1)
                           processedHits.head
                         else
                           processedHits)
        }

        Seq(parentContext ++ metrics ++ topHitsData)

      } else if (metrics.nonEmpty || parentContext.nonEmpty) {
        Seq(parentContext ++ metrics)
      } else {
        Seq.empty
      }
    } else {
      // Handle each aggregation with buckets
      bucketAggs.flatMap { case (aggName, buckets, aggValue) =>
        buckets.flatMap { bucket =>
          val bucketKey = extractBucketKey(bucket)
          val docCount = Option(bucket.get("doc_count"))
            .map(_.asLong())
            .getOrElse(0L)

          val currentContext = parentContext ++ Map(
            aggName                 -> bucketKey,
            s"${aggName}_doc_count" -> docCount
          )

          // Check for sub-aggregations
          val subAggFields = bucket
            .properties()
            .asScala
            .filterNot { entry =>
              Set("key", "key_as_string", "doc_count", "from", "to").contains(entry.getKey)
            }
            .toList

          if (subAggFields.nonEmpty) {
            val subAggsNode = mapper.createObjectNode()
            val jsubAggFields = subAggFields.asJava.iterator()
            while (jsubAggFields.hasNext) {
              val entry = jsubAggFields.next()
              subAggsNode.set(entry.getKey, entry.getValue)
            }
            /*subAggFields.foreach { entry =>
              subAggsNode.set(entry.getKey, entry.getValue) // FIXME
            }*/
            parseAggregations(subAggsNode, currentContext, fieldAliases, aggregations)
          } else {
            Seq(currentContext)
          }
        }
      }
    }
  }

  /** Extract the bucket key with proper typing (String, Long, Double, DateTime, etc.)
    */
  def extractBucketKey(bucket: JsonNode): Any = {
    // Préférer key_as_string pour les dates
    val keyAsString = Option(bucket.get("key_as_string"))
      .map(_.asText())

    keyAsString
      .map { strValue =>
        // Try to parse as an ISO 8601 date
        tryParseAsDateTime(strValue).getOrElse(strValue)
      }
      .orElse {
        Option(bucket.get("key")).map { keyNode =>
          if (keyNode.isTextual) {
            val text = keyNode.asText()
            tryParseAsDateTime(text).getOrElse(text)
          } else if (keyNode.isIntegralNumber) {
            val longValue = keyNode.asLong()
            // If it looks like a timestamp in milliseconds, convert to Instant
            if (longValue > 1000000000000L && longValue < 9999999999999L) {
              Instant.ofEpochMilli(longValue).atZone(ZoneId.of("UTC"))
            } else {
              longValue
            }
          } else if (keyNode.isFloatingPointNumber) {
            keyNode.asDouble()
          } else if (keyNode.isBoolean) {
            keyNode.asBoolean()
          } else {
            keyNode.asText()
          }
        }
      }
      .getOrElse("")
  }

  /** Try to parse a string as ZonedDateTime, LocalDateTime, LocalDate or LocalTime
    */
  def tryParseAsDateTime(text: String): Option[Any] = {
    Try(ZonedDateTime.parse(text, isoDateTimeFormatter)).toOption
      .orElse(Try(LocalDateTime.parse(text, isoDateTimeFormatter)).toOption)
      .orElse(Try(LocalDate.parse(text, isoDateFormatter)).toOption)
      .orElse(Try(LocalTime.parse(text, isoTimeFormatter)).toOption)
  }

  /** Extract metrics from an aggregation node
    */
  def extractMetrics(aggsNode: JsonNode): Map[String, Any] = {
    if (!aggsNode.isObject) return Map.empty
    aggsNode
      .properties()
      .asScala
      .flatMap { entry =>
        val name = normalizeAggregationKey(entry.getKey)
        val value = entry.getValue

        // Detect simple metric values
        Option(value.get("value"))
          .filter(!_.isNull)
          .map { metricValue =>
            val numericValue = if (metricValue.isIntegralNumber) {
              metricValue.asLong()
            } else if (metricValue.isFloatingPointNumber) {
              metricValue.asDouble()
            } else {
              metricValue.asText()
            }
            name -> numericValue
          }
          .orElse {
            // Stats aggregations
            if (value.has("count") && value.has("sum") && value.has("avg")) {
              Some(
                name -> Map(
                  "count" -> value.get("count").asLong(),
                  "sum"   -> Option(value.get("sum")).filterNot(_.isNull).map(_.asDouble()),
                  "avg"   -> Option(value.get("avg")).filterNot(_.isNull).map(_.asDouble()),
                  "min"   -> Option(value.get("min")).filterNot(_.isNull).map(_.asDouble()),
                  "max"   -> Option(value.get("max")).filterNot(_.isNull).map(_.asDouble())
                ).collect { case (k, Some(v)) => k -> v; case (k, v: Long) => k -> v }
              )
            } else {
              None
            }
          }
          .orElse {
            // Percentiles
            if (value.has("values") && value.get("values").isObject) {
              val percentiles = value
                .get("values")
                .properties()
                .asScala
                .map { pEntry =>
                  pEntry.getKey -> pEntry.getValue.asDouble()
                }
                .toMap
              Some(name -> percentiles)
            } else {
              None
            }
          }
      }
      .toMap
  }

  /** Extract all top_hits aggregations with their names and hits */
  def extractAllTopHits(aggsNode: JsonNode): Map[String, Seq[JsonNode]] = {
    if (!aggsNode.isObject) return Map.empty
    aggsNode
      .properties()
      .asScala
      .collect {
        case entry if entry.getValue.has("hits") =>
          val normalizedKey = normalizeAggregationKey(entry.getKey)
          val hitsNode = entry.getValue.path("hits").path("hits")
          val hits = if (hitsNode.isArray) {
            hitsNode.elements().asScala.toSeq
          } else {
            Seq.empty
          }
          normalizedKey -> hits
      }
      .toMap
  }

  /** Extract global metrics from aggregations (for hits + aggs case)
    */
  def extractGlobalMetrics(aggsNode: JsonNode): Map[String, Any] = {
    if (!aggsNode.isObject) return Map.empty
    aggsNode
      .properties()
      .asScala
      .flatMap { entry =>
        val name = entry.getKey
        val value = entry.getValue
        if (!value.has("buckets") && value.has("value")) {
          val metricValue = value.get("value")
          if (!metricValue.isNull) {
            val numericValue = if (metricValue.isIntegralNumber) {
              metricValue.asLong()
            } else if (metricValue.isFloatingPointNumber) {
              metricValue.asDouble()
            } else {
              metricValue.asText()
            }
            Some(name -> numericValue)
          } else {
            None
          }
        } else {
          None
        }
      }
      .toMap
  }

  /** Convert recursively a JsonNode to Map
    */
  def jsonNodeToMap(node: JsonNode, fieldAliases: Map[String, String]): Map[String, Any] = {
    if (!node.isObject) return Map.empty
    node
      .properties()
      .asScala
      .map { entry =>
        val name = entry.getKey
        fieldAliases.getOrElse(name, name) -> jsonNodeToAny(entry.getValue, fieldAliases)
      }
      .toMap
  }

  /** Convert a JsonNode to Any (primitive types, List, Map)
    */
  def jsonNodeToAny(node: JsonNode, fieldAliases: Map[String, String]): Any = {
    if (node == null || node.isNull) null
    else if (node.isBoolean) node.booleanValue()
    else if (node.isNumber) node.numberValue()
    else if (node.isTextual) {
      val text = node.asText()
      // Try to parse as date/time
      tryParseAsDateTime(text).getOrElse(text)
    } else if (node.isArray) {
      node.elements().asScala.map(jsonNodeToAny(_, fieldAliases)).toList
    } else if (node.isObject) {
      jsonNodeToMap(node, fieldAliases)
    } else {
      node.asText()
    }
  }

  /** Normalize aggregation key by removing ES type prefix Examples: "cardinality#c" -> "c"
    * "terms#category" -> "category" "c" -> "c" (unchanged)
    */
  private[this] def normalizeAggregationKey(key: String): String = {
    key.split('#') match {
      case Array(_, suffix) if suffix.nonEmpty => suffix
      case _                                   => key
    }
  }
}
