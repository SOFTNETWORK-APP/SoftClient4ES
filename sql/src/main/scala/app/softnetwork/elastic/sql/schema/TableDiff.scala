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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.Value
import app.softnetwork.elastic.sql.`type`.{SQLArray, SQLType, SQLTypeUtils}
import app.softnetwork.elastic.sql.query._

sealed trait DiffSafety
case object Safe extends DiffSafety
case object UnsafeReindex extends DiffSafety
case object Impossible extends DiffSafety

sealed trait AlterTableStatementDiff {
  def stmt: AlterTableStatement
  def safety: DiffSafety
}

sealed trait ColumnDiff extends AlterTableStatementDiff

case class ColumnAdded(column: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AddColumn(column)
  override def safety: DiffSafety = Safe
}
case class ColumnRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumn(name)
  override def safety: DiffSafety = UnsafeReindex
}
// case class ColumnRenamed(oldName: String, newName: String) extends ColumnDiff

/** @param fromMappingType
  *   SCHEMA-7 (A-61) -- the column's Elasticsearch type before the change (`Column.mappingType`);
  *   empty when not known
  * @param toMappingType
  *   its Elasticsearch type after the change; empty when not known
  */
case class ColumnTypeChanged(name: String, from: SQLType, to: SQLType)(
  val fromMappingType: String = "",
  val toMappingType: String = ""
) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnType(name, to)

  /** SCHEMA-6 (D5): a change of an array column's element type -- or between a list and a scalar of
    * another Elasticsearch type -- stays `Impossible`, as on 0.24.0, where every array column read
    * `ANY` and `canConvert(ANY, _)` was false. Declaring an existing field a list of its own type
    * is no `ColumnTypeChanged` at all (`Column.diff` compares Elasticsearch types).
    *
    * SCHEMA-7 (A-62): a `constant_keyword` column keeps its type -- its value is in the mapping,
    * not in `_source`, so a reindex would read NULL on every row ([[reason]]).
    */
  override def safety: DiffSafety =
    if (reason.isDefined) Impossible
    else if (from.isInstanceOf[SQLArray] || to.isInstanceOf[SQLArray]) Impossible
    else if (SQLTypeUtils.canConvert(from, to))
      UnsafeReindex
    else
      Impossible

  /** Why the change cannot be made at all, when it is not a matter of the two SQL types. */
  def reason: Option[String] =
    if (fromMappingType == "constant_keyword" && toMappingType != fromMappingType)
      Some(
        s"column $name is a constant_keyword field: its value is held by the mapping, not by the " +
        "documents, so copying them into another type would leave it NULL on every row"
      )
    else None
}

case class ColumnDefaultSet(name: String, value: Value[_]) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnDefault(name, value)
  override def safety: DiffSafety = Safe
}
case class ColumnDefaultRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnDefault(name)
  override def safety: DiffSafety = Safe
}

case class ColumnScriptSet(name: String, script: ScriptProcessor) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnScript(name, script)
  override def safety: DiffSafety = Safe
}
case class ColumnScriptRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnScript(name)
  override def safety: DiffSafety = Safe
}

case class ColumnCommentSet(name: String, comment: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnComment(name, comment)
  override def safety: DiffSafety = Safe
}
case class ColumnCommentRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnComment(name)
  override def safety: DiffSafety = Safe
}

case class ColumnNotNullSet(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnNotNull(name)
  override def safety: DiffSafety = Safe
}
case class ColumnNotNullRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnNotNull(name)
  override def safety: DiffSafety = Safe
}

case class ColumnOptionSet(name: String, key: String, value: Value[_]) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnOption(name, key, value)
  override def safety: DiffSafety = if (MappingsRules.isSafe(key)) Safe else UnsafeReindex
}
case class ColumnOptionRemoved(name: String, key: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnOption(name, key)
  override def safety: DiffSafety = if (MappingsRules.isSafe(key)) Safe else UnsafeReindex
}

case class FieldAdded(column: String, field: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnField(column, field)
  override def safety: DiffSafety = Safe
}
case class FieldRemoved(column: String, fieldName: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnField(column, fieldName)
  override def safety: DiffSafety = Safe
}
case class FieldAltered(column: String, field: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnField(column, field)
  override def safety: DiffSafety = Safe
}

sealed trait MappingDiff extends AlterTableStatementDiff

case class MappingSet(key: String, value: Value[_]) extends MappingDiff {
  override def stmt: AlterTableStatement = AlterTableMapping(key, value)
  override def safety: DiffSafety = if (MappingsRules.isSafe(key)) Safe else UnsafeReindex
}
case class MappingRemoved(key: String) extends MappingDiff {
  override def stmt: AlterTableStatement = DropTableMapping(key)
  override def safety: DiffSafety = if (MappingsRules.isSafe(key)) Safe else UnsafeReindex
}

sealed trait SettingDiff extends AlterTableStatementDiff

case class SettingSet(key: String, value: Value[_]) extends SettingDiff {
  override def stmt: AlterTableStatement = AlterTableSetting(key, value)
  override def safety: DiffSafety =
    if (SettingsRules.isDynamic(key)) Safe
    else UnsafeReindex
}
case class SettingRemoved(key: String) extends SettingDiff {
  override def stmt: AlterTableStatement = DropTableSetting(key)
  override def safety: DiffSafety =
    if (SettingsRules.isDynamic(key)) Safe
    else UnsafeReindex
}

sealed trait AliasDiff extends AlterTableStatementDiff

case class AliasSet(key: String, value: Value[_]) extends AliasDiff {
  override def stmt: AlterTableStatement = AlterTableAlias(key, value)
  override def safety: DiffSafety = Safe
}
case class AliasRemoved(key: String) extends AliasDiff {
  override def stmt: AlterTableStatement = DropTableAlias(key)
  override def safety: DiffSafety = Safe
}

sealed trait AlterPipelineStatementDiff {
  def stmt: AlterPipelineStatement
  def safety: DiffSafety = Safe
  def processor: IngestProcessor
  def pipelineType: IngestPipelineType = processor.pipelineType
}

sealed trait PipelineDiff extends AlterPipelineStatementDiff

case class ProcessorAdded(processor: IngestProcessor) extends PipelineDiff {
  override def stmt: AlterPipelineStatement = AddPipelineProcessor(processor)
}
case class ProcessorRemoved(processor: IngestProcessor) extends PipelineDiff {
  override def stmt: AlterPipelineStatement =
    DropPipelineProcessor(processor.processorType, processor.column)
}
case class ProcessorTypeChanged(
  actual: IngestProcessorType,
  desired: IngestProcessorType
)
sealed trait ProcessorPropertyDiff
case class ProcessorPropertyAdded(key: String, value: Any) extends ProcessorPropertyDiff
case class ProcessorPropertyRemoved(key: String) extends ProcessorPropertyDiff
case class ProcessorPropertyChanged(key: String, from: Any, to: Any) extends ProcessorPropertyDiff
case class ProcessorDiff(
  typeChanged: Option[ProcessorTypeChanged],
  propertyDiffs: List[ProcessorPropertyDiff]
)
case class ProcessorChanged(
  from: IngestProcessor,
  to: IngestProcessor,
  diff: ProcessorDiff
) extends PipelineDiff {
  override def stmt: AlterPipelineStatement = AlterPipelineProcessor(to)
  override def pipelineType: IngestPipelineType = from.pipelineType
  override def processor: IngestProcessor = to
}

/** What an ALTER TABLE does to one live pipeline. See [[IngestPipeline.rewrite]].
  *
  * @param diff
  *   the changes to the processors the table declares; a processor it does not declare never
  *   appears here
  * @param written
  *   the pipeline to install when `diff` is non-empty: the declared processors, with the undeclared
  *   ones kept verbatim before or after them
  * @param undeclared
  *   the live processors the table did not declare before the statement, as stored, in live order
  * @param stored
  *   the live pipeline document as Elasticsearch returned it: what a rewrite keeps besides the
  *   processors (`on_failure`, `version`, `_meta`, `description`)
  * @param previous
  *   what elasticsql may last have written as this pipeline: the previous declaration, the same
  *   with the kept processors in place, and the pipeline as read. A stored `description` equal to
  *   the one elasticsql renders for one of them is elasticsql's own, and is rewritten
  */
final case class PipelineRewrite(
  diff: List[PipelineDiff],
  written: IngestPipeline,
  undeclared: Seq[IngestProcessor],
  stored: String,
  previous: Seq[IngestPipeline]
)

case class TableDiff(
  columns: List[ColumnDiff],
  mappings: List[MappingDiff],
  settings: List[SettingDiff],
  pipeline: List[PipelineDiff],
  aliases: List[AliasDiff]
) {
  def isEmpty: Boolean =
    columns.isEmpty && mappings.isEmpty && settings.isEmpty && pipeline.isEmpty

  def alterTable(tableName: String, ifExists: Boolean): Option[AlterTable] = {
    if (isEmpty) {
      None
    } else {
      val statements = columns.map(_.stmt) ++
        mappings.map(_.stmt) ++
        settings.map(_.stmt) ++
        aliases.map(_.stmt)
      Some(
        AlterTable(
          tableName,
          ifExists,
          statements
        )
      )
    }
  }

  def defaultPipeline: List[PipelineDiff] =
    pipeline.filter(_.pipelineType == IngestPipelineType.Default)

  def finalPipeline: List[PipelineDiff] =
    pipeline.filter(_.pipelineType == IngestPipelineType.Final)

  def createPipeline(name: String, pipelineType: IngestPipelineType): Option[CreatePipeline] = {
    val pipelineProcessors = pipeline.filter(_.pipelineType == pipelineType).map(_.processor)
    if (pipelineProcessors.isEmpty) {
      None
    } else {
      Some(
        CreatePipeline(
          name,
          pipelineType,
          ifNotExists = true,
          orReplace = false,
          pipelineProcessors
        )
      )
    }
  }

  def alterPipeline(name: String, pipelineType: IngestPipelineType): Option[AlterPipeline] = {
    val pipelineStatements = pipeline.filter(_.pipelineType == pipelineType).map(_.stmt)
    if (pipelineStatements.isEmpty) {
      None
    } else {
      Some(
        AlterPipeline(
          name,
          ifExists = true,
          pipelineStatements
        )
      )
    }
  }

  def safety: DiffSafety = {
    val safeties =
      columns.map(_.safety) ++
      mappings.map(_.safety) ++
      settings.map(_.safety) ++
      pipeline.map(_.safety) ++
      aliases.map(_.safety)

    if (safeties.contains(Impossible)) Impossible
    else if (safeties.contains(UnsafeReindex)) UnsafeReindex
    else Safe
  }

  def impossible: Boolean = safety == Impossible

  def requiresReindex: Boolean = safety == UnsafeReindex

  def safe: Boolean = safety == Safe

}
