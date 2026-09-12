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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.schema.{Column, Schema}
import app.softnetwork.elastic.sql.`type`.{SQLTemporal, SQLTime, SQLType}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}
import scala.util.Try
import scala.util.matching.Regex

/** Issue #276 -- resolve the string literals a WHERE clause compares against a `date`-mapped column
  * into a spelling the column's mapping `format` accepts.
  *
  * A BI tool renders a timestamp as `'2026-06-04 00:00:00.000000'` (the SQL standard spelling);
  * Elasticsearch's default `date` format (`strict_date_optional_time||epoch_millis`) accepts only
  * the `T`-separated ISO-8601 form, so the range/term query built from the verbatim literal was
  * rejected with a raw `search_phase_execution_exception`. The rewrite happens ONCE per statement,
  * on the AST, against the MAPPED type -- never per hit and never through a "does this string look
  * like a date?" heuristic in the bridge (softclient4es-arrow#139's `tryParseAsDateTime` lesson).
  *
  * What is rewritten: the `StringValue` operand of a `GenericExpression` whose operator is one of
  * `= <> != >= > <= <`, both bounds of a literal `BETWEEN`, and every member of a string `IN` list
  * -- when the left-hand side is a FUNCTION-FREE identifier that resolves through `schema.find` to
  * a temporal column other than `TIME`, and belongs to the statement's own source (an identifier
  * qualified with a cross-index JOIN alias is left alone: the schema in hand is not its table's).
  * `LIKE` / `RLIKE` patterns, function-wrapped columns (`YEAR(ts) = 2026` runs as a Painless
  * script, not a range query), `HAVING`, `CASE` conditions and `keyword`/`text` columns are never
  * touched.
  *
  * The rule per literal, in order (see [[FieldFormat]] for the format vocabulary):
  *   1. a number (`epoch_millis` / `epoch_second`) or `now`-anchored date math is forwarded
  *      verbatim; a `||` date-math literal has its HEAD normalised by this same rule and its math
  *      kept (`2026-06-04 10:30:15||+1d` -> `2026-06-04T10:30:15||+1d`) and is never rejected;
  *   1. a literal one of the mapping's CUSTOM patterns already parses is forwarded verbatim -- a
  *      `format: "yyyy-MM-dd HH:mm:ss"` column parses the space form TODAY and must keep working;
  *   1. if the mapping accepts an ISO optional-time built-in: a literal of the RECOGNISED calendar
  *      shape -- `yyyy-MM-dd`, optionally followed by a `T` or ONE SPACE, a time `HH[:mm[:ss[.f]]]`
  *      and a zone (`Z` or an offset `+01` / `+0100` / `+01:00` / `+01:00:00`) -- has its date and
  *      time validated with `java.time`; the space form is then rewritten with a `T` (fraction
  *      digits and offset kept, a lower-case `z` upper-cased); an ISO form is forwarded verbatim. A
  *      literal that merely STARTS like an ISO date (a signed or 5-digit year, `2026-6-4`,
  *      `T1:02:03`, an ordinal or week date, a zone id such as `UTC` or `GMT+1`, an offset in
  *      another shape, any tail this recogniser does not model) is forwarded verbatim:
  *      Elasticsearch decides;
  *   1. a literal is REJECTED -- with a message naming the literal, the field and the format --
  *      only when it cannot be an ISO date at all (it does not even start with a year) or carries
  *      an INVALID recognised calendar/time component (`2026-02-30`, `T24:00:00`), and only when
  *      every alternative of the format is one whose grammar this recogniser approximates closely
  *      enough: the STRICT ISO optional-time names and the epoch names. Under the non-strict
  *      `date_optional_time`, a custom pattern or an unrecognised built-in nothing is rejected.
  *
  * The accept set was measured against the real Elasticsearch parsers of 6.8.23, 7.17.29, 8.18.3
  * and 9.0.3 (`DateFormatter.forPattern(spec).toDateMathParser()`): every REJECT above fails on all
  * four, every VERBATIM is a no-op, and every value the rewrite can produce -- a `T` form with an
  * optional fraction and an optional `Z` / `+01` / `+0100` / `+01:00` / `+01:00:00` -- is accepted
  * on all four. Zone shapes some majors reject (`+010000` on 7.17, `GMT+1` on 6.8/7.17, the mixed
  * `+0100:00` / `+01:0000` everywhere) are never produced: their inputs are forwarded verbatim.
  *
  * The schema-absent path (no schema attached, statement over several indices, wildcard source,
  * schema lookup failure) is the CALLER's decision and means "forward verbatim" -- this object
  * never guesses a type.
  */
object TemporalLiterals {

  /** Elasticsearch's default `date` mapping format. */
  val DefaultDateFormat: String = "strict_date_optional_time||epoch_millis"

  /** Longest literal echoed back in a rejection message; longer ones are cut head + tail. */
  val MaxLiteralExcerpt: Int = 120

  /** The ISO optional-time built-ins: date mandatory, `T`-separated time optional. The space-form
    * rewrite applies under any of them (the lenient parser accepts the `T` form too -- measured).
    */
  private val IsoOptionalTimeFormats: Set[String] =
    Set("strict_date_optional_time", "date_optional_time", "strict_date_optional_time_nanos")

  /** The STRICT names -- the only ones whose grammar the recogniser below approximates closely
    * enough to REJECT a literal. The non-strict `date_optional_time` also takes `2026-6-4`,
    * `2026-06-04T1:02:03` and `12026-06-04` (measured on ES 6.8 / 7.17 / 8.18 / 9.0).
    */
  private val StrictIsoFormats: Set[String] =
    Set("strict_date_optional_time", "strict_date_optional_time_nanos")

  private val EpochFormats: Set[String] = Set("epoch_millis", "epoch_second")

  /** A built-in format NAME (`basic_date_time`, `date_hour_minute`, ...) as opposed to a custom
    * pattern, which always carries an upper-case letter, a separator or a quote.
    */
  private val BuiltInName: Regex = "^[a-z][a-z0-9_]*$".r

  /** A number, read leniently: ES 6.8's Joda parser also takes `+1` and `1e3`. */
  private val NumericLiteral: Regex = "^[+-]?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?$".r

  /** Elasticsearch date math anchored on `now`: `now`, `now-1d`, `now+1h/h`, `now/d`. */
  private val NowDateMath: Regex = "^now(?:[+-]\\d+[yMwdhHms]|/[yMwdhHms])*$".r

  /** Starts like an ISO date: an optionally signed year of at least four digits. */
  private val DateLikeStart: Regex = "^[+-]?\\d{4}".r

  private val IsoTime = "\\d{2}(?::\\d{2}(?::\\d{2}(?:[.,]\\d{1,9})?)?)?"

  /** `Z` or an offset in one of the FOUR shapes every Elasticsearch major accepts after a time:
    * `+01`, `+0100`, `+01:00`, `+01:00:00`. Anything else (`+010000`, a mixed `+0100:00`, a zone id
    * such as `UTC` or `GMT+1` -- accepted only by some majors) is left to the date-like fallback,
    * i.e. forwarded verbatim, never rewritten.
    */
  private val IsoZone = "Z|z|[+-]\\d{2}(?::\\d{2}(?::\\d{2})?|\\d{2})?"

  /** The RECOGNISED calendar shape: a strict `yyyy-MM-dd`, optionally followed by a `T` or ONE
    * space, an optional time and an optional zone. Groups: date, separator, time, zone.
    */
  private val CalendarLiteral: Regex =
    ("^(\\d{4}-\\d{2}-\\d{2})(?:([ T])(" + IsoTime + ")?(" + IsoZone + ")?)?$").r

  /** A `date` column's effective mapping `format`, split on `||`. */
  final case class FieldFormat(spec: String) {

    val alternatives: Seq[String] =
      spec.split("\\|\\|").toSeq.map(_.trim).filter(_.nonEmpty)

    private def isIso(alternative: String): Boolean = IsoOptionalTimeFormats.contains(alternative)

    private def isStrictIso(alternative: String): Boolean = StrictIsoFormats.contains(alternative)

    private def isEpoch(alternative: String): Boolean = EpochFormats.contains(alternative)

    /** At least one alternative accepts the `T`-separated ISO form we normalise to. */
    val acceptsIsoOptionalTime: Boolean = alternatives.exists(isIso)

    /** Every alternative is a STRICT ISO optional-time or an epoch name -- the ONLY case in which a
      * literal may be rejected rather than forwarded verbatim.
      */
    val fullyUnderstood: Boolean =
      alternatives.nonEmpty && alternatives.forall(a => isStrictIso(a) || isEpoch(a))

    /** The alternatives that are custom `DateTimeFormatter` patterns (not built-in names). */
    val customPatterns: Seq[String] =
      alternatives.filterNot(a =>
        isIso(a) || isEpoch(a) || BuiltInName.pattern.matcher(a).matches()
      )

    private lazy val customFormatters: Seq[DateTimeFormatter] =
      customPatterns.flatMap { pattern =>
        // Elasticsearch 7.x accepted a leading `8` to select the java.time parser during the Joda
        // migration; it is not part of the pattern.
        val javaPattern =
          if (pattern.length > 1 && pattern.charAt(0) == '8') pattern.substring(1) else pattern
        Try(DateTimeFormatter.ofPattern(javaPattern)).toOption
      }

    /** True when one of the custom patterns parses the literal -- Elasticsearch will too. */
    def acceptsAsCustom(literal: String): Boolean =
      customFormatters.exists(formatter => Try(formatter.parse(literal)).isSuccess)
  }

  object FieldFormat {

    /** The column's explicit `format` option, else Elasticsearch's default. */
    def of(column: Column): FieldFormat =
      column.options.get("format") match {
        case Some(value) =>
          FieldFormat(Option(value.value).map(_.toString).getOrElse(DefaultDateFormat))
        case None => FieldFormat(DefaultDateFormat)
      }
  }

  /** `now`-anchored or `||`-suffixed date math is resolved by Elasticsearch itself. */
  def isDateMath(literal: String): Boolean =
    NowDateMath.pattern.matcher(literal).matches() || literal.contains("||")

  /** Normalise ONE literal compared against `field`.
    *
    * @return
    *   `Right(None)` to forward the literal verbatim, `Right(Some(v))` to replace it by `v`,
    *   `Left(reason)` to reject the statement with a message naming the literal and the field.
    */
  def normalizeLiteral(
    literal: String,
    field: String,
    format: FieldFormat
  ): Either[String, Option[String]] = {
    if (
      NumericLiteral.pattern.matcher(literal).matches() ||
      NowDateMath.pattern.matcher(literal).matches()
    ) Right(None)
    else {
      val math = literal.indexOf("||")
      if (math >= 0) {
        // Date math anchored on a date: normalise the HEAD by the same rule, keep the math, and
        // never reject -- the math tail is Elasticsearch's to judge.
        normalizeLiteral(literal.substring(0, math), field, format) match {
          case Right(Some(head)) => Right(Some(head + literal.substring(math)))
          case _                 => Right(None)
        }
      } else if (format.acceptsAsCustom(literal)) Right(None)
      else if (!format.acceptsIsoOptionalTime) Right(None)
      else
        literal match {
          case CalendarLiteral(date, separator, time, zone) =>
            calendar(literal, date, Option(separator), Option(time), Option(zone), field, format)
          case _ if DateLikeStart.findPrefixOf(literal).isDefined =>
            // Starts like an ISO date but carries a shape this recogniser does not model: let
            // Elasticsearch decide, never reject.
            Right(None)
          case _ => rejectOrForward(literal, field, format)
        }
    }
  }

  private def calendar(
    literal: String,
    date: String,
    separator: Option[String],
    time: Option[String],
    zone: Option[String],
    field: String,
    format: FieldFormat
  ): Either[String, Option[String]] =
    if (!validDate(date) || !time.forall(validTime)) rejectOrForward(literal, field, format)
    else
      (separator, time) match {
        case (Some(" "), Some(t)) =>
          val z = zone.map(z => if (z == "z") "Z" else z).getOrElse("")
          Right(Some(date + "T" + t + z))
        case _ => Right(None)
      }

  private def rejectOrForward(
    literal: String,
    field: String,
    format: FieldFormat
  ): Either[String, Option[String]] =
    if (format.fullyUnderstood) Left(rejection(literal, field, format)) else Right(None)

  private def validDate(date: String): Boolean = Try(LocalDate.parse(date)).isSuccess

  private def validTime(time: String): Boolean = {
    val isoTime = time.replace(',', '.')
    val parseableTime = if (isoTime.length == 2) isoTime + ":00" else isoTime
    Try(LocalTime.parse(parseableTime)).isSuccess
  }

  /** The literal as echoed in a rejection: control characters and line separators collapsed to a
    * space (the message reaches a log record, a `SQLException` and a terminal), the length bounded
    * head + tail so a hostile or accidental multi-kilobyte literal cannot flood either.
    */
  private[query] def excerpt(literal: String): String = {
    val flat = literal.replaceAll("[\\p{Cntrl}\\u0085\\u2028\\u2029]+", " ")
    if (flat.length <= MaxLiteralExcerpt) flat
    else {
      val head = MaxLiteralExcerpt * 2 / 3
      val tail = MaxLiteralExcerpt - head - 3
      flat.substring(0, head) + "..." + flat.substring(flat.length - tail)
    }
  }

  private def rejection(literal: String, field: String, format: FieldFormat): String =
    s"Cannot parse '${excerpt(literal)}' as a date/time value for date field '$field' " +
    s"(mapping format '${format.spec}'): expected an ISO-8601 date or timestamp " +
    "('yyyy-MM-dd', 'yyyy-MM-ddTHH:mm:ss[.fraction][zone]'), the SQL spelling " +
    "'yyyy-MM-dd HH:mm:ss[.fraction][zone]', epoch milliseconds, or a date-math expression"

  /** `DATE` / `DATETIME` / `TIMESTAMP` / `TEMPORAL` -- every `date` mapping resolves to one of
    * these. `TIME` is excluded: Elasticsearch has no time-only mapping and a bare time is not an
    * ISO optional-time literal. `date_nanos` resolves to `ANY` on the AST and is therefore never a
    * candidate (excluded by construction -- see the unit test that pins it).
    */
  private def isTemporalColumn(dataType: SQLType): Boolean = dataType match {
    case _: SQLTime     => false
    case _: SQLTemporal => true
    case _              => false
  }

  /** Only a bare column reaches `rangeQuery` / `termQuery` with the literal as value. */
  private def plainColumn(identifier: GenericIdentifier): Boolean =
    identifier.functions.isEmpty && identifier.name.nonEmpty

  /** The identifier's column in `schema`, unless the identifier belongs to a cross-index JOIN
    * source (`joinSources`): that table's mapping is not the one in hand.
    *
    * 🔴 `joinSources` must be keyed the way `Identifier.table` is — i.e. `From.joinSourceKeys`, not
    * the bare `joinAliases` source names. `Identifier.table` IS a `From.tableAliases` key, and
    * since story 21.2 that key is the QUALIFIED reference whenever a bare index name is ambiguous
    * inside one FROM. Comparing a qualified `table` against a bare set silently stops this guard
    * firing, and the literal is then resolved against the wrong index's schema.
    *
    * And `joinSources` must NOT contain the main table's own key (story BIDC-8): on a self-join
    * (`FROM idx a JOIN idx b`) the join source IS the main index, whose mapping IS the one in hand,
    * so every column of BOTH legs resolves against it. Before BIDC-8 the first leg's qualifier did
    * not resolve at all, so the question never arose.
    */
  private def temporalColumn(
    identifier: GenericIdentifier,
    schema: Schema,
    joinSources: Set[String]
  ): Option[Column] =
    if (identifier.table.exists(joinSources.contains)) None
    else schema.find(identifier.name).filter(column => isTemporalColumn(column.dataType))

  private def rangeOrEquality(operator: ComparisonOperator): Boolean = operator match {
    case EQ | NE | DIFF | GE | GT | LE | LT => true
    case _                                  => false
  }

  /** Resolve every temporal literal of `search`'s WHERE clause against `schema`.
    *
    * @return
    *   `Right(search)` (the SAME instance when nothing changed), or `Left(reason)`.
    */
  def apply(search: SingleSearch, schema: Schema): Either[String, SingleSearch] =
    search.where.flatMap(_.criteria) match {
      case Some(criteria) =>
        // `joinSourceKeys`, NOT `joinAliases.values.map(_._1)`: the comparison below is against
        // `Identifier.table`, which is a `tableAliases` KEY (story 21.2 AD-6'). Minus the main
        // table's own key, so a self-join's second leg resolves against the mapping in hand (story
        // BIDC-8). See `temporalColumn`.
        val joinSources: Set[String] = search.from.joinSourceKeys - search.from.mainTableKey
        rewrite(criteria, schema, joinSources).map { rewritten =>
          if (rewritten eq criteria) search
          else search.copy(where = Some(Where(Some(rewritten))))
        }
      case None => Right(search)
    }

  private def rewrite(
    criteria: Criteria,
    schema: Schema,
    joinSources: Set[String]
  ): Either[String, Criteria] =
    criteria match {
      case p: Predicate =>
        for {
          left  <- rewrite(p.leftCriteria, schema, joinSources)
          right <- rewrite(p.rightCriteria, schema, joinSources)
        } yield
          if ((left eq p.leftCriteria) && (right eq p.rightCriteria)) p
          else p.copy(leftCriteria = left, rightCriteria = right)

      case n: ElasticNested =>
        rewrite(n.criteria, schema, joinSources)
          .map(c => if (c eq n.criteria) n else n.copy(criteria = c))

      case n: ElasticChild =>
        rewrite(n.criteria, schema, joinSources)
          .map(c => if (c eq n.criteria) n else n.copy(criteria = c))

      case n: ElasticParent =>
        rewrite(n.criteria, schema, joinSources)
          .map(c => if (c eq n.criteria) n else n.copy(criteria = c))

      case e: GenericExpression =>
        (e.identifier, e.operator, e.value) match {
          case (id: GenericIdentifier, op: ComparisonOperator, literal: StringValue)
              if rangeOrEquality(op) && plainColumn(id) =>
            temporalColumn(id, schema, joinSources) match {
              case Some(column) =>
                normalizeLiteral(literal.value, id.name, FieldFormat.of(column)).map {
                  case Some(normalized) => e.copy(value = StringValue(normalized))
                  case None             => e
                }
              case None => Right(e)
            }
          case _ => Right(e)
        }

      case b: BetweenExpr =>
        (b.identifier, b.fromTo) match {
          case (id: GenericIdentifier, LiteralFromTo(from, to)) if plainColumn(id) =>
            temporalColumn(id, schema, joinSources) match {
              case Some(column) =>
                val format = FieldFormat.of(column)
                for {
                  lower <- normalizeLiteral(from.value, id.name, format)
                  upper <- normalizeLiteral(to.value, id.name, format)
                } yield
                  if (lower.isEmpty && upper.isEmpty) b
                  else
                    b.copy(fromTo =
                      LiteralFromTo(
                        StringValue(lower.getOrElse(from.value)),
                        StringValue(upper.getOrElse(to.value))
                      )
                    )
              case None => Right(b)
            }
          case _ => Right(b)
        }

      case in: InExpr[_, _] =>
        (in.identifier, in.values) match {
          case (id: GenericIdentifier, strings: StringValues) if plainColumn(id) =>
            temporalColumn(id, schema, joinSources) match {
              case Some(column) =>
                val format = FieldFormat.of(column)
                val zero: Either[String, (Seq[StringValue], Boolean)] = Right((Seq.empty, false))
                strings.values
                  .foldLeft(zero) {
                    case (Left(reason), _) => Left(reason)
                    case (Right((acc, changed)), value) =>
                      normalizeLiteral(value.value, id.name, format).map {
                        case Some(normalized) => (acc :+ StringValue(normalized), true)
                        case None             => (acc :+ value, changed)
                      }
                  }
                  .map { case (values, changed) =>
                    val result: Criteria =
                      if (changed) InExpr(id, StringValues(values), in.maybeNot) else in
                    result
                  }
              case None => Right(in)
            }
          case _ => Right(in)
        }

      case other => Right(other)
    }
}
