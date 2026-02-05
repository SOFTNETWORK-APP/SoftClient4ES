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

package app.softnetwork.elastic.sql.watcher

import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.{
  BooleanValue,
  ByteValue,
  DdlToken,
  DoubleValue,
  FloatValue,
  Identifier,
  IntValue,
  LongValue,
  ObjectValue,
  ShortValue,
  Value
}
import com.fasterxml.jackson.databind.JsonNode

import scala.util.Try

sealed trait WatcherCondition extends DdlToken {
  def node: JsonNode
}

case object AlwaysWatcherCondition extends WatcherCondition {
  override def sql: String = " ALWAYS"

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val alwaysNode = mapper.createObjectNode()
    node.set("always", alwaysNode)
    node
  }
}

case object NeverWatcherCondition extends WatcherCondition {
  override def sql: String = " NEVER"

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val neverNode = mapper.createObjectNode()
    node.set("never", neverNode)
    node
  }
}

object CompareWatcherCondition {
  def apply(
    left: String,
    operator: ComparisonOperator,
    right: Either[Value[_], Identifier]
  ): CompareWatcherCondition = {
    right match {
      case Right(script) if script.script.isEmpty =>
        throw new IllegalArgumentException(
          s"Invalid date math script in watcher condition: $script"
        )
      case _ => // OK
    }

    new CompareWatcherCondition(left, operator, right)
  }

  /** Safe constructor */
  def safe(
    left: String,
    operator: ComparisonOperator,
    right: Either[Value[_], Identifier]
  ): Try[CompareWatcherCondition] = Try(apply(left, operator, right))
}

class CompareWatcherCondition private (
  val left: String,
  val operator: ComparisonOperator,
  val right: Either[Value[_], Identifier]
) extends WatcherCondition {

  override def validate(): Either[String, Unit] = Right(())

  override def sql: String = {
    val rightStr = right match {
      case Left(value)   => value.ddl
      case Right(script) => script.sql
    }
    s" WHEN $left $operator $rightStr"
  }

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val compareNode = mapper.createObjectNode()
    val rightNode = mapper.createObjectNode()
    val operatorStr = operator match {
      case EQ => "eq"
      case NE => "neq"
      case GT => "gt"
      case GE => "gte"
      case LT => "lt"
      case LE => "lte"
    }
    right match {
      case Left(value) =>
        value match {
          case ByteValue(v)    => rightNode.put(operatorStr, v)
          case ShortValue(v)   => rightNode.put(operatorStr, v)
          case IntValue(v)     => rightNode.put(operatorStr, v)
          case LongValue(v)    => rightNode.put(operatorStr, v)
          case DoubleValue(v)  => rightNode.put(operatorStr, v)
          case FloatValue(v)   => rightNode.put(operatorStr, v)
          case BooleanValue(v) => rightNode.put(operatorStr, v)
          case _               => rightNode.put(operatorStr, value.ddl)
        }
      case Right(script) =>
        rightNode.put(operatorStr, script.script.get)
    }
    compareNode.set(left, rightNode)
    node.set("compare", compareNode)
    node
  }
}

case class ScriptWatcherCondition(
  script: String,
  lang: String = "painless",
  params: Map[String, Value[_]] = Map.empty
) extends WatcherCondition {
  override def sql: String = {
    val base = s" WHEN SCRIPT '$script' USING LANG '$lang'"
    if (params.nonEmpty) {
      val paramsStr = ObjectValue(params).ddl
      s"$base WITH PARAMS $paramsStr RETURNS TRUE"
    } else {
      s"$base RETURNS TRUE"
    }
  }

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val scriptNode = mapper.createObjectNode()
    scriptNode.put("source", script)
    scriptNode.put("lang", lang)
    if (params.nonEmpty) {
      val paramsNode = ObjectValue(params).toJson
      scriptNode.set("params", paramsNode)
    }
    node.set("script", scriptNode)
    node
  }
}
