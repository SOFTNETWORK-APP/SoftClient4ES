package app.softnetwork.elastic

import app.softnetwork.elastic.sql.Value
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

package object schema {

  private[schema] def extractOptions(
    node: JsonNode,
    ignoredKeys: Set[String] = Set.empty
  ): Map[String, Value[_]] = {
    node
      .properties()
      .asScala
      .flatMap { entry =>
        val key = entry.getKey
        val value = entry.getValue

        if (ignoredKeys.contains(key)) {
          None
        } else {
          Value(value).map(key -> Value(_))
        }
      }
      .toMap
  }

  final case class Field(
    name: String,
    `type`: String,
    null_value: Option[Value[_]] = None,
    fields: List[Field] = Nil,
    options: Map[String, Value[_]] = Map.empty
  )

  object Field {
    def apply(name: String, node: JsonNode): Field = {
      val tpe = Option(node.get("type")).map(_.asText()).getOrElse("object")

      val nullValue =
        Option(node.get("null_value")).flatMap(Value(_)).map(Value(_))

      val fields =
        Option(node.get("fields")) // multi-fields
          .orElse(Option(node.get("properties"))) // object/nested fields
          .map(_.properties().asScala.map { entry =>
            val name = entry.getKey
            val value = entry.getValue
            apply(name, value)
          }.toList)
          .getOrElse(Nil)

      val options =
        extractOptions(node, ignoredKeys = Set("type", "null_value", "fields", "properties"))

      Field(
        name = name,
        `type` = tpe,
        null_value = nullValue,
        fields = fields,
        options = options
      )

    }
  }

  final case class Mappings(
    fields: List[Field] = Nil,
    primaryKey: List[String] = Nil,
    partitionBy: Option[EsPartitionBy] = None,
    options: Map[String, Value[_]] = Map.empty
  )

  object Mappings {
    def apply(root: JsonNode): Mappings = {
      val mappings = root.path("mappings")
      val fields = Option(mappings.get("properties"))
        .orElse(Option(mappings.path("_doc").get("properties")))
        .map(_.properties().asScala.map { entry =>
          val name = entry.getKey
          val value = entry.getValue
          Field(name, value)
        }.toList)
        .getOrElse(Nil)

      val options = extractOptions(mappings, ignoredKeys = Set("properties", "_doc"))
      val meta = options.get("_meta")
      val primaryKey: List[String] = meta
        .map {
          case m: Map[_, _] =>
            m.asInstanceOf[Map[String, Value[_]]].get("primary_key") match {
              case Some(pk: Value[_]) =>
                pk.value match {
                  case list: List[_] => list.map(_.toString)
                  case str: String   => List(str)
                  case _             => List.empty
                }
              case _ => List.empty
            }
          case _ => List.empty
        }
        .getOrElse(List.empty)

      val partitionBy: Option[EsPartitionBy] = meta.flatMap {
        case m: Map[_, _] =>
          m.asInstanceOf[Map[String, Value[_]]].get("partition_by") match {
            case Some(pb: Value[_]) =>
              pb.value match {
                case map: Map[_, _] =>
                  val partitionMap = map.asInstanceOf[Map[String, String]]
                  val column = partitionMap.getOrElse("column", "date")
                  val granularity = partitionMap.getOrElse("granularity", "d")
                  Some(EsPartitionBy(column, granularity))
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }

      Mappings(
        fields = fields,
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        options = options
      )
    }

  }

  final case class EsPartitionBy(
    column: String,
    granularity: String // "d", "M", "y", etc.
  )

  final case class Settings(
    options: Map[String, Value[_]] = Map.empty
  )

  object Settings {
    def apply(settings: JsonNode): Settings = {
      val index = settings.path("settings").path("index")

      val options = extractOptions(index)

      Settings(
        options = options
      )
    }
  }

  final case class Index(
    name: String,
    mappings: Mappings,
    settings: Settings,
    pipeline: Option[JsonNode] = None
  )

  object Index {
    def apply(
      name: String,
      settings: JsonNode,
      mappings: JsonNode,
      pipeline: Option[JsonNode]
    ): Index = {
      Index(
        name = name,
        mappings = Mappings(mappings),
        settings = Settings(settings),
        pipeline = pipeline
      )
    }
  }
}
