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
import app.softnetwork.elastic.sql.`type`.SQLType
import app.softnetwork.elastic.sql.query._

sealed trait AlterTableStatementDiff {
  def stmt: AlterTableStatement
}

sealed trait ColumnDiff extends AlterTableStatementDiff

case class ColumnAdded(column: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AddColumn(column)
}
case class ColumnRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumn(name)
}
// case class ColumnRenamed(oldName: String, newName: String) extends ColumnDiff

case class ColumnTypeChanged(name: String, from: SQLType, to: SQLType) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnType(name, to)
}

case class ColumnDefaultSet(name: String, value: Value[_]) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnDefault(name, value)
}
case class ColumnDefaultRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnDefault(name)
}

case class ColumnScriptSet(name: String, script: ScriptProcessor) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnScript(name, script)
}
case class ColumnScriptRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnScript(name)
}

case class ColumnCommentSet(name: String, comment: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnComment(name, comment)
}
case class ColumnCommentRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnComment(name)
}

case class ColumnNotNullSet(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnNotNull(name)
}
case class ColumnNotNullRemoved(name: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnNotNull(name)
}

case class ColumnOptionSet(name: String, key: String, value: Value[_]) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnOption(name, key, value)
}
case class ColumnOptionRemoved(name: String, key: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnOption(name, key)
}

case class FieldAdded(column: String, field: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnField(column, field)
}
case class FieldRemoved(column: String, fieldName: String) extends ColumnDiff {
  override def stmt: AlterTableStatement = DropColumnField(column, fieldName)
}
case class FieldAltered(column: String, field: Column) extends ColumnDiff {
  override def stmt: AlterTableStatement = AlterColumnField(column, field)
}

sealed trait MappingDiff extends AlterTableStatementDiff

case class MappingSet(key: String, value: Value[_]) extends MappingDiff {
  override def stmt: AlterTableStatement = AlterTableMapping(key, value)
}
case class MappingRemoved(key: String) extends MappingDiff {
  override def stmt: AlterTableStatement = DropTableMapping(key)
}

sealed trait SettingDiff extends AlterTableStatementDiff

case class SettingSet(key: String, value: Value[_]) extends SettingDiff {
  override def stmt: AlterTableStatement = AlterTableSetting(key, value)
}
case class SettingRemoved(key: String) extends SettingDiff {
  override def stmt: AlterTableStatement = DropTableSetting(key)
}

sealed trait AliasDiff extends AlterTableStatementDiff

case class AliasSet(key: String, value: Value[_]) extends AliasDiff {
  override def stmt: AlterTableStatement = AlterTableAlias(key, value)
}
case class AliasRemoved(key: String) extends AliasDiff {
  override def stmt: AlterTableStatement = DropTableAlias(key)
}

sealed trait AlterPipelineStatementDiff {
  def stmt: AlterPipelineStatement
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
}

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

  def alterPipeline(name: String, ifExists: Boolean): Option[AlterPipeline] = {
    val pipelineStatements = pipeline.map(_.stmt)
    if (pipelineStatements.isEmpty) {
      None
    } else {
      Some(
        AlterPipeline(
          name,
          ifExists,
          pipelineStatements
        )
      )
    }
  }
}
