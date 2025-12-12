package app.softnetwork.elastic

import app.softnetwork.elastic.sql.Value
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

package object schema {
  final case class EsField(
    name: String,
    `type`: String,
    null_value: Option[Value[_]] = None,
    fields: List[EsField] = Nil,
    options: Map[String, Value[_]] = Map.empty
  )

  object EsField {
    def apply(name: String, node: JsonNode): EsField = {
      val tpe = Option(node.get("type")).map(_.asText()).getOrElse("object")

      val nullValue =
        Option(node.get("null_value")).flatMap(Value(_)).map(Value(_))

      val fields =
        Option(node.get("fields"))
          .map(_.properties().asScala.map { entry =>
            val name = entry.getKey
            val value = entry.getValue
            apply(name, value)
          }.toList)
          .getOrElse(Nil)

      val options = extractOptions(node)

      EsField(
        name = name,
        `type` = tpe,
        null_value = nullValue,
        fields = fields,
        options = options
      )

    }

    private[this] def extractOptions(node: JsonNode): Map[String, Value[_]] = {
      val ignoredKeys = Set("type", "null_value", "fields")

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

  }

  final case class EsMapping(
    fields: List[EsField] = Nil
  )

  object EsMapping {
    def apply(root: JsonNode): EsMapping = {
      val fields = Option(root.path("mappings").path("properties"))
        .map(_.properties().asScala.map { entry =>
          val name = entry.getKey
          val value = entry.getValue
          EsField(name, value)
        }.toList)
        .getOrElse(Nil)

      EsMapping(fields = fields)
    }
  }

  final case class EsPartitionMeta(
    column: String,
    granularity: String // "d", "M", "y", etc.
  )

  final case class EsDdlMeta(
    primary_key: List[String] = Nil,
    partition_by: Option[EsPartitionMeta] = None,
    default_pipeline: Option[String] = None,
    final_pipeline: Option[String] = None
  )

  object EsDdlMeta {
    def apply(settings: JsonNode): EsDdlMeta = {
      val index = settings.path("settings").path("index")

      val defaultPipeline = Option(index.get("default_pipeline")).map(_.asText())
      val finalPipeline = Option(index.get("final_pipeline")).map(_.asText())

      val ddlNode = index
        .path("meta")
        .path("ddl")

      if (ddlNode.isMissingNode)
        return EsDdlMeta(
          default_pipeline = defaultPipeline,
          final_pipeline = finalPipeline
        )

      val primaryKey = Option(ddlNode.path("primary_key"))
        .filter(_.isArray)
        .map(_.elements().asScala.map(_.asText()).toList)
        .getOrElse(Nil)

      val partitionBy =
        Option(ddlNode.path("partition_by")).filter(_.isObject).map { partitionNode =>
          val column = Option(partitionNode.get("column"))
            .map(_.asText())
            .getOrElse("date")
          val granularity = Option(partitionNode.get("granularity"))
            .map(_.asText())
            .getOrElse("d")
          EsPartitionMeta(column, granularity)
        }

      EsDdlMeta(
        primary_key = primaryKey,
        partition_by = partitionBy,
        default_pipeline = defaultPipeline,
        final_pipeline = finalPipeline
      )
    }
  }

  final case class EsIndexMeta(
    name: String,
    mapping: EsMapping,
    ddlMeta: EsDdlMeta,
    defaultPipeline: Option[JsonNode] = None // existing "DDL default" pipeline
  )

  object EsIndexMeta {
    def apply(
      name: String,
      settings: JsonNode,
      mappings: JsonNode,
      defaultPipeline: Option[JsonNode]
    ): EsIndexMeta = {
      val mapping = EsMapping(mappings)
      val ddlMeta = EsDdlMeta(settings)

      EsIndexMeta(
        name = name,
        mapping = mapping,
        ddlMeta = ddlMeta,
        defaultPipeline = defaultPipeline
      )
    }
  }
}
