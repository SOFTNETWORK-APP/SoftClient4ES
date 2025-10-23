package app.softnetwork.elastic.client

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.{Extraction, Formats}

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag
import scala.util.Try

//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._

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

  // Formatters for elasticsearch ISO 8601 date/time strings
  private val isoDateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME
  private val isoDateFormatter = DateTimeFormatter.ISO_DATE
  private val isoTimeFormatter = DateTimeFormatter.ISO_TIME

  /** main entry point : parse json response from elasticsearch Handles both single search and
    * multi-search (msearch/UNION ALL) responses
    */
  def parseResponse(jsonString: SQLResult): Try[Seq[Map[String, Any]]] = {
    val json = mapper.readTree(jsonString)
    // Check if it's a multi-search response (array of responses)
    if (json.isArray) {
      parseMultiSearchResponse(json)
    } else {
      // Single search response
      parseSingleSearchResponse(json)
    }
  }

  /** Parse a multi-search response (array of search responses) Used for UNION ALL queries
    */
  def parseMultiSearchResponse(jsonArray: JsonNode): Try[Seq[Map[String, Any]]] =
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
            jsonToRows(response)
          } else {
            Seq.empty
          }
        }
        allRows
      }
    }

  /** Parse a single search response
    */
  def parseSingleSearchResponse(json: JsonNode): Try[Seq[Map[String, Any]]] =
    Try {
      // check if it is an error response
      if (json.has("error")) {
        val errorMsg = Option(json.get("error").get("reason"))
          .map(_.asText())
          .getOrElse("Unknown Elasticsearch error")
        throw new Exception(s"Elasticsearch error: $errorMsg")
      } else {
        jsonToRows(json)
      }
    }

  /** convert JsonNode to Rows
    */
  def jsonToRows(json: JsonNode): Seq[Map[String, Any]] = {
    val hitsNode = Option(json.path("hits").path("hits"))
      .filter(_.isArray)
      .map(_.elements().asScala.toList)

    val aggsNode = Option(json.path("aggregations"))
      .filter(!_.isMissingNode)

    (hitsNode, aggsNode) match {
      case (Some(hits), None) if hits.nonEmpty =>
        // Case 1 : only hits
        parseSimpleHits(hits)

      case (None, Some(aggs)) =>
        // Case 2 : only aggregations
        parseAggregations(aggs, Map.empty)

      case (Some(hits), Some(aggs)) if hits.isEmpty =>
        // Case 3 : aggregations with no hits
        parseAggregations(aggs, Map.empty)

      case (Some(hits), Some(aggs)) if hits.nonEmpty =>
        // Case 4 : Hits + global aggregations
        val globalMetrics = extractGlobalMetrics(aggs)
        hits.map { hit =>
          val source = extractSource(hit)
          val metadata = extractHitMetadata(hit)
          globalMetrics ++ source ++ metadata
        }

      case _ =>
        Seq.empty
    }
  }

  /** Parse simple hits (without aggregations)
    */
  def parseSimpleHits(hits: List[JsonNode]): Seq[Map[String, Any]] = {
    hits.map { hit =>
      val source = extractSource(hit)
      val metadata = extractHitMetadata(hit)
      source ++ metadata
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
  def extractSource(hit: JsonNode): Map[String, Any] = {
    Option(hit.get("_source"))
      .filter(_.isObject)
      .map(jsonNodeToMap)
      .getOrElse(Map.empty)
  }

  /** Parse recursively aggregations from Elasticsearch response with parent context
    */
  def parseAggregations(
    aggsNode: JsonNode,
    parentContext: Map[String, Any]
  ): Seq[Map[String, Any]] = {

    if (aggsNode.isMissingNode || !aggsNode.isObject) {
      return Seq.empty
    }

    // Find all buckets
    val bucketAggs = aggsNode
      .properties()
      .asScala
      .flatMap { entry =>
        val aggName = entry.getKey
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
      val topHits = extractTopHits(aggsNode)

      if (topHits.nonEmpty) {
        topHits.map { hit =>
          parentContext ++ metrics ++ extractSource(hit) ++ extractHitMetadata(hit)
        }
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
            parseAggregations(subAggsNode, currentContext)
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
        val name = entry.getKey
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

  /** Extract all top hits from an aggregation node
    */
  def extractTopHits(aggsNode: JsonNode): Seq[JsonNode] = {
    if (!aggsNode.isObject) return Seq.empty

    aggsNode
      .properties()
      .asScala
      .collectFirst {
        case entry if entry.getValue.has("hits") =>
          val hitsNode = entry.getValue.path("hits").path("hits")
          if (hitsNode.isArray) {
            hitsNode.elements().asScala.toSeq
          } else {
            Seq.empty
          }
      }
      .getOrElse(Seq.empty)
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
  def jsonNodeToMap(node: JsonNode): Map[String, Any] = {
    if (!node.isObject) return Map.empty

    node
      .properties()
      .asScala
      .map { entry =>
        entry.getKey -> jsonNodeToAny(entry.getValue)
      }
      .toMap
  }

  /** Convert a JsonNode to Any (primitive types, List, Map)
    */
  def jsonNodeToAny(node: JsonNode): Any = {
    if (node == null || node.isNull) null
    else if (node.isBoolean) node.booleanValue()
    else if (node.isNumber) node.numberValue()
    else if (node.isTextual) {
      val text = node.asText()
      // Try to parse as date/time
      tryParseAsDateTime(text).getOrElse(text)
    } else if (node.isArray) {
      node.elements().asScala.map(jsonNodeToAny).toList
    } else if (node.isObject) {
      jsonNodeToMap(node)
    } else {
      node.asText()
    }
  }
}
