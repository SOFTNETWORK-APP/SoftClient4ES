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

package app.softnetwork.elastic.sql.`type`

sealed trait SQLType {
  def typeId: String
  override def toString: String = typeId

  /** The type-family predicates, asked of the type itself rather than re-spelled as
    * `isInstanceOf[SQL...]` at each site.
    *
    * 🔴 They live HERE, on the sealed trait, because the families are what this file DECLARES: a
    * predicate written beside the hierarchy cannot fall out of step with it, and a new member of a
    * family is answered for by every caller at once. Before issue #382 the same four questions were
    * spelled out at 22 sites across six files (`function/cond`, `query/Where`, `SQLTypeUtils`,
    * `operator/math/ArithmeticExpression`, `sql/package`), and the ones that disagreed are exactly
    * where the defects were -- `leastCommonSuperType` had already learnt that `contains(Varchar)`
    * misses `Text` and `Keyword` while `isInstanceOf[SQLVarchar]` does not, and that lesson had to
    * be relearnt in `coerce` a hundred lines below.
    *
    * ⚠️ [[isText]] is `SQLVarchar` and NOT `SQLChar`, which is a sibling under `SQLLiteral` rather
    * than a subtype. That is what every one of the 22 sites meant, so the extraction preserves it
    * exactly; whether `CHAR` should be text is a separate decision with its own blast radius, and
    * making it here would have changed behaviour under cover of a refactor.
    */
  def isText: Boolean = this.isInstanceOf[SQLVarchar]

  def isNumber: Boolean = this.isInstanceOf[SQLNumeric]

  def isTemporal: Boolean = this.isInstanceOf[SQLTemporal]

  def isBoolean: Boolean = this.isInstanceOf[SQLBool]

  /** No type is known here: an unresolved column (`SQLTypes.Any`, what every column reports before
    * a schema is attached) or a NULL literal. A rule that REFUSES must treat this as "cannot judge"
    * and accept, or it rejects legitimate SQL on every schema-less path.
    */
  def isUnknown: Boolean = this.isInstanceOf[SQLAny]
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

sealed trait EsqlType extends SQLType

trait EsqlText extends EsqlType with SQLVarchar

trait EsqlKeyword extends EsqlType with SQLVarchar

trait EsqlGeoPoint extends EsqlType

trait SQLVarBinary extends SQLType
