/*
 * Copyright 2015 SOFTNETWORK
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

package app.softnetwork.elastic.sql.`type`

sealed trait SQLType {
  def typeId: String
  override def toString: String = typeId
}

trait SQLAny extends SQLType

trait SQLNull extends SQLAny

trait SQLTemporal extends SQLType

trait SQLDate extends SQLTemporal
trait SQLTime extends SQLTemporal
trait SQLDateTime extends SQLTemporal
trait SQLTimestamp extends SQLDateTime

trait SQLNumeric extends SQLType

trait SQLTinyInt extends SQLNumeric
trait SQLSmallInt extends SQLNumeric
trait SQLInt extends SQLNumeric
trait SQLBigInt extends SQLNumeric
trait SQLDouble extends SQLNumeric
trait SQLReal extends SQLNumeric

trait SQLLiteral extends SQLType
trait SQLVarchar extends SQLLiteral
trait SQLChar extends SQLLiteral

trait SQLBool extends SQLType

trait SQLArray extends SQLType { def elementType: SQLType }

trait SQLStruct extends SQLType
