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

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}

/** How ONE date position of a set operation's leg is read, decided BEFORE any value: from the type
  * its branch declares, its date field's mapping `format` and the Elasticsearch major the statement
  * runs on -- never from a value, so the answer cannot depend on the order the rows come in.
  *
  *   - [[DateRoute.Typed]]: the declared type, every row -- a date read by its format, as that
  *     major indexed it, or a computed value. A value core cannot read there fails the statement,
  *     at any row (`SetOperationValues.Unreadable.Fail`).
  *   - [[DateRoute.AsStored]]: `VARCHAR`, the value as stored, every row -- where core cannot read
  *     every value of the format for sure, which the format alone says
  *     ([[MappedDateFormat.unread]]), or where the major is not known.
  */
private[client] sealed trait DateRoute

private[client] object DateRoute {

  /** Read by its format, or as a computed value: the declared type, every row. */
  case object Typed extends DateRoute

  /** `VARCHAR` as stored, every row, decided before any row; `reason` names the format and why. */
  final case class AsStored(reason: String) extends DateRoute

  /** `Typed` for a type that is not a date read by its format -- the positions
    * `SetOperationValues.readAs` reads with one: a `DATE`, a `TIMESTAMP` or a `TIME` that has a
    * format, Elasticsearch's default included -- else by the major `dates` names (its `dialect`,
    * asked here, once per statement) and what the format reads ([[MappedDateFormat.unread]]):
    * `AsStored` when that major is not known, or when core does not read every value of the format
    * for sure.
    */
  def of(
    sqlType: SQLType,
    format: Option[String],
    dates: SetOperationValues.DateReading
  ): DateRoute =
    (sqlType, format) match {
      case (SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time, Some(spec)) =>
        dates.dialect match {
          case None =>
            AsStored(
              s"the date format '$spec' is read as the Elasticsearch version does, " +
              dates.elasticVersion.fold("which is not known")(v => s"and core reads dates of no $v")
            )
          case Some(dialect) =>
            MappedDateFormat(spec, dialect, dates.clock).unread.fold[DateRoute](Typed) { why =>
              AsStored(
                s"core cannot read every value of the date format '$spec' as ${dialect.label} " +
                s"does: $why"
              )
            }
        }
      case _ => Typed
    }
}
