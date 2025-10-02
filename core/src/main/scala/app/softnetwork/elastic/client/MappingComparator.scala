package app.softnetwork.elastic.client

import com.google.gson._
import com.typesafe.scalalogging.StrictLogging

//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object MappingComparator extends StrictLogging {

  private def parseJsonToMap(jsonString: String): Map[String, JsonElement] = {
    Try(
      new JsonParser()
        .parse(jsonString)
        .getAsJsonObject
        .get("properties")
        .getAsJsonObject
        .entrySet()
        .asScala
        .map { entry =>
          // Convert the mapping properties to a Map[String, JsonElement]
          entry.getKey -> entry.getValue
        }
        .toMap
    ) match {
      case Success(jsonMap) => jsonMap
      case Failure(f) =>
        logger.error(s"Failed to parse JSON mapping $jsonString: ${f.getMessage}", f)
        Map.empty[String, JsonElement]
    }
  }

  private def compareJsonMappings(
    oldMap: Map[String, JsonElement],
    newMap: Map[String, JsonElement]
  ): Map[String, (Option[JsonElement], Option[JsonElement])] = {
    val allKeys = oldMap.keySet union newMap.keySet
    allKeys.flatMap { key =>
      (oldMap.get(key), newMap.get(key)) match {
        case (Some(oldVal), Some(newVal)) if oldVal == newVal =>
          None // equal
        case (a, b) =>
          Some(key -> (a, b)) // mismatch or missing
      }
    }.toMap
  }

  private def hasDifferences(
    diff: Map[String, (Option[JsonElement], Option[JsonElement])]
  ): Boolean =
    diff.nonEmpty

  private def formatDiff(diff: Map[String, (Option[JsonElement], Option[JsonElement])]): String = {
    diff
      .map {
        case (key, (Some(oldV), Some(newV))) =>
          s"Field [$key] changed:\n  old: $oldV\n  new: $newV"
        case (key, (Some(oldV), None)) =>
          s"Field [$key] removed:\n  old: $oldV"
        case (key, (None, Some(newV))) =>
          s"Field [$key] added:\n  new: $newV"
        case _ => ""
      }
      .mkString("\n")
  }

  /** Compares two JSON mappings and logs the differences.
    *
    * @param oldMapping
    *   - the old mapping as a JSON string
    * @param newMapping
    *   - the new mapping as a JSON string
    * @return
    *   true if there are differences, false otherwise
    */
  def isMappingDifferent(oldMapping: String, newMapping: String): Boolean = {
    val oldMap = parseJsonToMap(oldMapping)
    val newMap = parseJsonToMap(newMapping)
    val diff = compareJsonMappings(oldMap, newMap)
    if (hasDifferences(diff)) {
      logger.info(formatDiff(diff))
      true
    } else {
      false
    }
  }
}
