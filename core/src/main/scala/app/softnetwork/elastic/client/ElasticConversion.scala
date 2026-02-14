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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.json4s.{Extraction, Formats}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap
import scala.util.Try
import scala.jdk.CollectionConverters._

trait ElasticConversion {

  def convertTo[T](map: Map[String, Any])(implicit m: Manifest[T], formats: Formats): T = {
    val jValue = Extraction.decompose(map)
    jValue.extract[T]
  }

  def convertTo[T](response: ElasticResponse)(implicit
    m: Manifest[T],
    formats: Formats
  ): Try[Seq[T]] = {
    Try(
      response.results.map { row =>
        convertTo[T](row)(m, formats)
      }
    )
  }

  // Formatters for elasticsearch ISO 8601 date/time strings
  private val isoDateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME
  private val isoDateFormatter = DateTimeFormatter.ISO_DATE
  private val isoTimeFormatter = DateTimeFormatter.ISO_TIME

  /** main entry point : parse json response from elasticsearch Handles both single search and
    * multi-search (msearch/UNION ALL) responses
    *
    * @param fields
    *   all requested fields in their original SQL SELECT order (output column names after alias
    *   resolution). When non-empty, rows are normalized to include all fields (with null for
    *   missing ones) and ordered to match the SQL SELECT order.
    */
  def parseResponse(
    results: String,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation],
    fields: Seq[String] = Seq.empty
  ): Try[Seq[ListMap[String, Any]]] = {
    var json = mapper.readTree(results)
    if (json.has("responses")) {
      json = json.get("responses")
    }
    // Check if it's a multi-search response (array of responses)
    if (json.isArray) {
      parseMultiSearchResponse(json, fieldAliases, aggregations, fields)
    } else {
      // Single search response
      parseSingleSearchResponse(json, fieldAliases, aggregations, fields)
    }
  }

  /** Parse a multi-search response (array of search responses) Used for UNION ALL queries
    */
  def parseMultiSearchResponse(
    jsonArray: JsonNode,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation],
    fields: Seq[String] = Seq.empty
  ): Try[Seq[ListMap[String, Any]]] =
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
            jsonToRows(response, fieldAliases, aggregations, fields)
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation],
    fields: Seq[String] = Seq.empty
  ): Try[Seq[ListMap[String, Any]]] =
    Try {
      // check if it is an error response
      if (json.has("error")) {
        val errorMsg = Option(json.get("error").get("reason"))
          .map(_.asText())
          .getOrElse("Unknown Elasticsearch error")
        throw new Exception(s"Elasticsearch error: $errorMsg")
      } else {
        jsonToRows(json, fieldAliases, aggregations, fields)
      }
    }

  /** convert JsonNode to Rows
    */
  def jsonToRows(
    json: JsonNode,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation],
    fields: Seq[String] = Seq.empty
  ): Seq[ListMap[String, Any]] = {
    val hitsNode = Option(json.path("hits").path("hits"))
      .filter(_.isArray)
      .map(_.elements().asScala.toList)

    val aggsNode = Option(json.path("aggregations"))
      .filter(!_.isMissingNode)

    (hitsNode, aggsNode) match {
      case (Some(hits), None) if hits.nonEmpty =>
        // Case 1 : only hits
        parseSimpleHits(hits, fieldAliases, fields)

      case (None, Some(aggs)) =>
        // Case 2 : only aggregations
        val ret = parseAggregations(aggs, ListMap.empty, fieldAliases, aggregations)
        val groupedRows: Map[String, Seq[ListMap[String, Any]]] =
          ret.groupBy(_.getOrElse("bucket_root", "").toString)
        groupedRows.values.foldLeft(Seq(ListMap.empty[String, Any])) { (acc, group) =>
          for {
            accMap   <- acc
            groupMap <- group
          } yield accMap ++ groupMap
        }

      case (Some(hits), Some(aggs)) if hits.isEmpty =>
        // Case 3 : aggregations with no hits
        val ret = parseAggregations(aggs, ListMap.empty, fieldAliases, aggregations)
        val groupedRows: Map[String, Seq[ListMap[String, Any]]] =
          ret.groupBy(_.getOrElse("bucket_root", "").toString)
        groupedRows.values.foldLeft(Seq(ListMap.empty[String, Any])) { (acc, group) =>
          for {
            accMap   <- acc
            groupMap <- group
          } yield accMap ++ groupMap
        }

      case (Some(hits), Some(aggs)) if hits.nonEmpty =>
        // Case 4 : Hits + global aggregations + top_hits aggregations
        val globalMetrics = extractGlobalMetrics(aggs)
        val allTopHits = extractAggregationValues(
          extractAllTopHits(aggs, fieldAliases, aggregations),
          aggregations
        )
        parseSimpleHits(hits, fieldAliases, fields).map { row =>
          globalMetrics ++ allTopHits ++ row
        }
      /*hits.map { hit =>
          var source = extractSource(hit, fieldAliases)
          fieldAliases.foreach(entry => {
            val key = entry._1
            if (!source.contains(key)) {
              findKeyValue(key, source) match {
                case Some(value) => source += (entry._2 -> value)
                case None        =>
              }
            }
          })
          val metadata = extractHitMetadata(hit)
          val innerHits = extractInnerHits(hit, fieldAliases)
          val fieldsNode = Option(hit.path("fields"))
            .filter(!_.isMissingNode)
          val fields = fieldsNode
            .map(jsonNodeToMap(_, fieldAliases))
            .getOrElse(Map.empty)
          globalMetrics ++ allTopHits ++ source ++ metadata ++ innerHits ++ fields
        }*/

      case _ =>
        Seq.empty
    }
  }

  def findKeyValue(path: String, map: Map[String, Any]): Option[Any] = {
    val keys = path.split("\\.")
    keys.foldLeft(Option(map): Option[Any]) {
      case (Some(m: Map[_, _]), key) =>
        m.asInstanceOf[Map[String, Any]].get(key)
      case _ => None
    }
  }

  /** Parse simple hits (without aggregations)
    *
    * @param requestedFields
    *   all requested fields in their original SQL SELECT order (output column names). When
    *   non-empty, each row is normalized to include all requested fields (with null for missing
    *   ones) and ordered to match the SQL SELECT order.
    */
  def parseSimpleHits(
    hits: List[JsonNode],
    fieldAliases: ListMap[String, String],
    requestedFields: Seq[String] = Seq.empty
  ): Seq[ListMap[String, Any]] = {
    hits.map { hit =>
      var source = extractSource(hit, fieldAliases)
      fieldAliases.foreach(entry => {
        if (!source.contains(entry._2)) {
          findKeyValue(entry._1, source) match {
            case Some(value) => source += (entry._2 -> value)
            case None        =>
          }
        }
      })
      val metadata = extractHitMetadata(hit)
      val innerHits = extractInnerHits(hit, fieldAliases)
      val fieldsNode = Option(hit.path("fields"))
        .filter(!_.isMissingNode)
      val fields = fieldsNode
        .map(jsonNodeToMap(_, fieldAliases))
        .getOrElse(ListMap.empty)
      val row = source ++ metadata ++ innerHits ++ fields
      normalizeRow(row, requestedFields)
    }
  }

  /** Normalize a row to ensure all requested fields are present in the original SQL SELECT order.
    * Fields missing from the row are added with null value. Extra fields (metadata like _id,
    * _index, etc.) are appended after the requested fields.
    */
  private def normalizeRow(
    row: ListMap[String, Any],
    requestedFields: Seq[String]
  ): ListMap[String, Any] = {
    if (requestedFields.isEmpty) row
    else {
      // Build ordered entries for requested fields, with null for missing ones
      val ordered = requestedFields.map { f =>
        f -> row.getOrElse(f, null)
      }
      // Append any extra fields from the row that aren't in the requested fields list
      val requestedSet = requestedFields.toSet
      val extra = row.filterNot { case (k, _) => requestedSet.contains(k) }
      ListMap(ordered: _*) ++ extra
    }
  }

  /** Extract hit metadata (_id, _index, _score)
    */
  def extractHitMetadata(hit: JsonNode): ListMap[String, Any] = {
    ListMap(
      "_id"    -> Option(hit.get("_id")).map(_.asText()),
      "_index" -> Option(hit.get("_index")).map(_.asText()),
      "_score" -> Option(hit.get("_score")).map(n =>
        if (n.isDouble || n.isFloat) n.asDouble() else n.asLong().toDouble
      ),
      "_sort" -> Option(hit.get("sort"))
        .filter(_.isArray)
        .map(sortNode => sortNode.elements().asScala.map(jsonNodeToAny(_, ListMap.empty)).toList)
    ).collect { case (k, Some(v)) => k -> v }
  }

  /** Extract hit _source
    */
  def extractSource(hit: JsonNode, fieldAliases: ListMap[String, String]): ListMap[String, Any] = {
    Option(hit.get("_source"))
      .filter(_.isObject)
      .map(jsonNodeToMap(_, fieldAliases))
      .getOrElse(ListMap.empty)
  }

  /** Extract inner_hits from a hit (for nested or parent-child queries) */
  private def extractInnerHits(
    hit: JsonNode,
    fieldAliases: ListMap[String, String]
  ): ListMap[String, Any] = {
    Option(hit.get("inner_hits")) match {
      case Some(n: ObjectNode) =>
        var entries: Seq[(String, Any)] = Seq.empty
        n.forEachEntry { case (innerHitName, innerHitData) =>
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

          entries ++= Seq(innerHitName -> innerHitsList)
        }
        ListMap(entries: _*)
      case _ => ListMap.empty
    }
  }

  /** Parse recursively aggregations from Elasticsearch response with parent context
    */
  def parseAggregations(
    aggsNode: JsonNode,
    parentContext: ListMap[String, Any],
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation]
  ): Seq[ListMap[String, Any]] = {

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

    val wrapperAggs = aggsNode
      .properties()
      .asScala
      .filter { entry =>
        val aggValue = entry.getValue
        // These aggregations have a doc_count but no buckets, and contain sub-aggregations
        aggValue.has("doc_count") &&
        !aggValue.has("buckets") &&
        !aggValue.has("value") &&
        hasSubAggregations(aggValue)
      }
      .toList

    if (wrapperAggs.nonEmpty) {
      // Process wrapper aggregations
      wrapperAggs.flatMap { entry =>
        val aggName = normalizeAggregationKey(entry.getKey)
        val aggValue = entry.getValue
        val docCount = Option(aggValue.get("doc_count"))
          .map(_.asLong())
          .getOrElse(0L)

        // Add the doc_count to the context if necessary
        val currentContext = if (docCount > 0) {
          parentContext + (s"${aggName}_doc_count" -> docCount)
        } else {
          parentContext
        }

        // Extract subaggregations (excluding doc_count)
        val subAggsNode = mapper.createObjectNode()

        val subAggFields = aggValue
          .properties()
          .asScala
          .filterNot { subEntry =>
            Set("doc_count", "doc_count_error_upper_bound", "sum_other_doc_count")
              .contains(subEntry.getKey)
          }
          .toList

        // Use a Java iterator to avoid casting problems
        val jSubAggFields = subAggFields.asJava.iterator()
        while (jSubAggFields.hasNext) {
          val subEntry = jSubAggFields.next()
          subAggsNode.set(subEntry.getKey, subEntry.getValue)
        }

        // Recursively parse subaggregations
        parseAggregations(subAggsNode, currentContext, fieldAliases, aggregations)
      }
    } else if (bucketAggs.isEmpty) {
      // No buckets : it is a leaf aggregation (metrics or top_hits)
      val metrics = extractMetrics(aggsNode, aggregations)
      val allTopHits = extractAllTopHits(aggsNode, fieldAliases, aggregations)

      if (allTopHits.nonEmpty) {
        Seq(parentContext ++ metrics ++ allTopHits)
      } else if (metrics.nonEmpty || parentContext.nonEmpty) {
        Seq(parentContext ++ metrics)
      } else {
        Seq.empty
      }
    } else {
      // Handle each aggregation with buckets
      bucketAggs.flatMap { case (aggName, buckets, _) =>
        buckets.flatMap { bucket =>
          val metrics = extractMetrics(bucket, aggregations)
          val allTopHits = extractAllTopHits(bucket, fieldAliases, aggregations)

          val bucketKey = extractBucketKey(bucket)
          val docCount = Option(bucket.get("doc_count"))
            .map(_.asLong())
            .getOrElse(0L)

          val currentContext = parentContext ++ ListMap(
            aggName                 -> bucketKey,
            s"${aggName}_doc_count" -> docCount
          ) ++ metrics ++ allTopHits

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

  /** Helper method to check if a node contains sub-aggregations */
  private def hasSubAggregations(node: JsonNode): Boolean = {
    node.properties().asScala.exists { entry =>
      val key = entry.getKey
      val value = entry.getValue
      // Une sous-agrégation est un objet qui n'est pas un champ métadata
      !Set("doc_count", "doc_count_error_upper_bound", "sum_other_doc_count", "bg_count", "score")
        .contains(key) && value.isObject
    }
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
  def extractMetrics(
    aggsNode: JsonNode,
    aggregations: ListMap[String, ClientAggregation]
  ): ListMap[String, Any] = {
    aggsNode match {
      case n: ObjectNode =>
        var bucketRoot: Option[String] = None
        var metrics: Seq[(String, Any)] = Seq.empty
        n.forEachEntry { (key, value) =>
          val name = normalizeAggregationKey(key)
          aggregations.get(name) match {
            case Some(agg) =>
              bucketRoot = Some(agg.bucketRoot)
            case _ =>
          }
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
                  name -> ListMap(
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
              if (value.has("values")) {
                Option(value.get("values")) match {
                  case Some(valuesNode: ObjectNode) =>
                    var percentiles: Seq[(String, Any)] = Seq.empty
                    valuesNode.forEachEntry { (k, v) => percentiles ++= Seq(k -> v.asDouble()) }
                    Some(name -> ListMap(percentiles: _*))
                  case _ => None
                }
              } else {
                None
              }
            } match {
            case Some(m) =>
              metrics ++= Seq(m._1 -> m._2)
            case _ =>
          }
        }
        ListMap((bucketRoot match {
          case Some(root) => metrics ++ Seq("bucket_root" -> root)
          case None       => metrics
        }): _*)

      case _ =>
        ListMap.empty
    }

  }

  /** Extract all top_hits aggregations with their names and hits */
  def extractAllTopHits(
    aggsNode: JsonNode,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation]
  ): ListMap[String, Any] = {
    aggsNode match {
      case n: ObjectNode =>
        var bucketRoot: Option[String] = None
        var allTopHits: Seq[(String, Seq[JsonNode])] = Seq.empty
        n.forEachEntry { (name, value) =>
          if (value.has("hits")) {
            val normalizedKey = normalizeAggregationKey(name)
            val hitsNode = value.path("hits").path("hits")
            val hits = if (hitsNode.isArray) {
              hitsNode.elements().asScala.toSeq
            } else {
              Seq.empty
            }
            allTopHits ++= Seq(normalizedKey -> hits)
          }
        }

        // Process each top_hits aggregation with their names
        val row = allTopHits.map { case (topHitName, hits) =>
          // Determine if it is a multivalued aggregation (array_agg, ...)
          val agg = aggregations.get(topHitName)
          val hasMultipleValues = agg match {
            case Some(agg) => agg.multivalued
            case None      =>
              // Fallback on naming convention if aggregation is not found
              !topHitName.toLowerCase.matches("(first|last)_.*")
          }

          agg match {
            case Some(agg) =>
              bucketRoot = Some(agg.bucketRoot)
            case _ =>
          }

          val processedHits = hits.map { hit =>
            val source = extractSource(hit, fieldAliases)
            if (hasMultipleValues) {
              source.size match {
                case 0 => null
                case 1 =>
                  // If only one field in source and multivalued, return the value directly
                  val value = source.head._2
                  value match {
                    case list: List[_]  => list
                    case map: Map[_, _] => map
                    case other          => other
                  }
                case _ =>
                  // Multiple fields: return as object
                  val metadata = extractHitMetadata(hit)
                  val innerHits = extractInnerHits(hit, fieldAliases)
                  source ++ metadata ++ innerHits
              }
            } else {
              val metadata = extractHitMetadata(hit)
              val innerHits = extractInnerHits(hit, fieldAliases)
              source ++ metadata ++ innerHits ++ ListMap("bucket_root" -> bucketRoot)
            }
          }

          // If multipleValues = true OR more than one hit, return a list
          // If multipleValues = false AND only one hit, return an object
          topHitName -> {
            if (!hasMultipleValues && processedHits.size == 1)
              processedHits.head
            else {
              if (aggregations.get(topHitName).exists(_.distinct))
                processedHits.distinct
              else
                processedHits
            }
          }
        }

        ListMap((bucketRoot match {
          case Some(root) => row ++ Seq("bucket_root" -> root)
          case None       => row
        }): _*)
      case _ =>
        ListMap.empty
    }

  }

  /** Extract global metrics from aggregations (for hits + aggs case)
    */
  def extractGlobalMetrics(aggsNode: JsonNode): ListMap[String, Any] = {
    aggsNode match {
      case n: ObjectNode =>
        var entries: Seq[(String, Any)] = Seq.empty
        n.forEachEntry { (name, value) =>
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
              entries ++= Seq(name -> numericValue)
            }
          }
        }
        ListMap(entries: _*)
      case _ =>
        ListMap.empty
    }
  }

  def extractAggregationValues(
    row: ListMap[String, Any],
    aggregations: ListMap[String, ClientAggregation]
  ): ListMap[String, Any] = {
    val values = aggregations
      .map { wf =>
        val fieldName = wf._1

        val aggType = wf._2.aggType

        val sourceField = wf._2.sourceField

        // Get value from row (already processed by ElasticConversion)
        val value = row.get(fieldName).orElse {
          None
        }

        val validatedValue =
          value match {
            case Some(m: Map[String, Any]) =>
              m.get(sourceField) match {
                case Some(v) =>
                  aggType match {
                    case AggregationType.ArrayAgg =>
                      v match {
                        case l: List[_] =>
                          Some(l)
                        case other =>
                          Some(List(other)) // Wrap into a List
                      }
                    case _ => Some(v)
                  }
                case None =>
                  None
              }
            case other =>
              other
          }

        fieldName -> validatedValue
      }
      .collect { case (name, Some(value)) =>
        name -> value
      }
    values
  }

  /** Convert recursively a JsonNode to ListMap
    */
  def jsonNodeToMap(node: JsonNode, fieldAliases: ListMap[String, String]): ListMap[String, Any] = {
    node match {
      case n: ObjectNode =>
        var entries: Seq[(String, Any)] = Seq.empty
        n.forEachEntry { (name, e) =>
          entries ++= Seq(fieldAliases.getOrElse(name, name) -> jsonNodeToAny(e, fieldAliases))
        }
        ListMap(entries: _*)
      case _ =>
        ListMap.empty
    }
  }

  /** Convert a JsonNode to Any (primitive types, List, Map)
    */
  def jsonNodeToAny(node: JsonNode, fieldAliases: ListMap[String, String]): Any = {
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

object ElasticConversion extends ElasticConversion
