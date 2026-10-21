package app.softnetwork.elastic.sql.function.time

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.time.TimeField
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.temporal.{ChronoField, IsoFields, TemporalField}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import scala.reflect.runtime.{universe => ru}

/** Issue #368 -- `LAST_DAY` and `EPOCHDAY` called a method that does not exist for the type a
  * `date` doc-value actually has, so neither had ever worked on any supported Elasticsearch.
  *
  * `doc['<date field>'].value` is a `java.time.ZonedDateTime`. `LAST_DAY` emitted
  * `<arg>.withDayOfMonth(<arg>.lengthOfMonth())` and `lengthOfMonth()` is declared on `LocalDate`
  * only; `EPOCHDAY` emitted `.get(ChronoField.EPOCH_DAY)` and `get` returns an `int`, which
  * `java.time` refuses for a field whose range does not fit one. Both failed the whole query as an
  * opaque `search_phase_execution_exception`.
  *
  * 🔴 Inserting a `.toLocalDate()` -- the fix the issue proposed -- is NOT enough, and this spec
  * exists partly to keep that from being tried again. The operand reaching `toPainlessCall` is a
  * `ZonedDateTime` for a bare column but ALREADY a `LocalDate` wherever a cast or a `SQLTypes.Date`
  * coercion ran first, and `LocalDate` has no `toLocalDate()`. MEASURED on real Elasticsearch
  * 8.18.3: `LAST_DAY(CURRENT_DATE)`, `LAST_DAY(CURRENT_TIMESTAMP)`, `LAST_DAY(CAST(d AS DATE))` and
  * the whole no-schema path (which is what the two `SQLQuerySpec` bridge fixtures pin) all reach it
  * with a `LocalDate` and all WORKED before the fix -- so `.toLocalDate()` would have swapped one
  * broken receiver for another rather than fixing anything.
  *
  * 🔴 Every emission pinned below was EXECUTED as the `source` of a `script_fields` entry over a
  * real index holding `d = 2025-02-10`, on real Elasticsearch **6.8.23, 7.17.29, 8.18.3 and 9.0.3**
  * -- "this is valid Painless" is a claim only Elasticsearch settles, and a byte pin proves the
  * bytes did not move, not that they RUN (story 21.8's lesson, which is how these two survived at
  * least four releases while being documented in five places). Results, identical on all four:
  * {{{
  * shape                            before                  after
  * LAST_DAY(d)                      shard failure           2025-02-28T00:00:00.000Z
  * EPOCHDAY(d)                      shard failure           20129
  * DAY(LAST_DAY(d))                 shard failure           28
  * EPOCHDAY(CAST(d AS DATE))        shard failure           20129   <- not in the issue
  * EPOCHDAY(CURRENT_DATE)           shard failure           20453   <- not in the issue
  * LAST_DAY(CURRENT_DATE)           2025-12-31              2025-12-31   (unchanged)
  * LAST_DAY(CURRENT_TIMESTAMP)      2025-12-31              2025-12-31   (unchanged)
  * LAST_DAY(DATE_TRUNC(d, MONTH))   2025-02-28              2025-02-28   (unchanged)
  * YEAR(d)                          2025                    2025         (control, unchanged)
  * }}}
  * ⚠️ `LAST_DAY(CAST(d AS DATE))` over a SCHEMA-CARRYING parse failed, before AND after this
  * issue's fix, with `dynamic method [java.time.LocalDate, toInstant/0] not found`: the consuming
  * function converted the cast's `LocalDate` from the COLUMN's type a second time. That is fixed by
  * reading the operand's rendered source type (`SQLTypeUtils.renderedSourceType`), pinned in the
  * "function over a CAST" section below.
  */
class DateDocValueEmissionSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("dn", SQLTypes.Any)
    )
  )

  /** A schema-CARRYING parse: without it every identifier's type stays `Any`, a coercion arm that
    * does not run in production inserts itself, and a broken emission looks fixed (#306 / 21.5).
    */
  private def scriptOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(PainlessContextType.Query)
        val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  // -- LAST_DAY -----------------------------------------------------------------------------------

  "LAST_DAY over a date column" should "adjust the receiver instead of calling a LocalDate-only method" in {
    // The doc value is made the DATE LAST_DAY answers first, through the UTC normalisation
    // (Elasticsearch 6.8's `JodaCompatibleZonedDateTime` needs it), then adjusted.
    scriptOf("SELECT LAST_DAY(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "(param1 == null) ? null : " +
    "param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(TemporalAdjusters.lastDayOfMonth())"
  }

  it should "emit the same adjuster for a timestamp column" in {
    // ...and over a TIMESTAMP too: LAST_DAY is a DATE whatever its operand, so the time of day is
    // dropped, where the adjuster alone kept it (`2024-02-29T12:00:00Z` for a DATE column).
    scriptOf("SELECT LAST_DAY(ts) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "(param1 == null) ? null : " +
    "param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(TemporalAdjusters.lastDayOfMonth())"
  }

  it should "keep working where the operand is already a LocalDate" in {
    // The shapes `.toLocalDate()` would have broken. Both executed on all four majors.
    scriptOf("SELECT LAST_DAY(CURRENT_DATE) FROM t") should endWith(
      "param1.with(TemporalAdjusters.lastDayOfMonth())"
    )
    scriptOf("SELECT LAST_DAY(CURRENT_TIMESTAMP) FROM t") should endWith(
      "param1.toLocalDate().with(TemporalAdjusters.lastDayOfMonth())"
    )
  }

  /** LAST_DAY is a DATE whatever its operand, as DuckDB types `last_day(TIMESTAMP)`: it reports
    * DATE and renders a `LocalDate`, so a CASE beside a cast to DATE is a DATE too.
    */
  it should "report the DATE it renders" in {
    val resolved = Parser("SELECT LAST_DAY(ts) FROM t") match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head.identifier
      case other                   => fail(s"expected a SingleSearch, got $other")
    }
    resolved.reportedType shouldBe SQLTypes.Date
    resolved.renderedType shouldBe SQLTypes.Date
  }

  it should "leave a date_nanos operand as it is, whose type is unknown" in {
    // `date_nanos` types `Any` (BIDC-4): its receiver is not guessed, as before (by-name residual)
    scriptOf("SELECT LAST_DAY(dn) FROM t") shouldBe
    "def param1 = (doc['dn'].size() == 0 ? null : doc['dn'].value); " +
    "(param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth())"
  }

  /** 🔴 The MECHANISM, not the bytes: the reason `lengthOfMonth()` shipped broken is that nobody
    * asked whether the receiver has it. The pipeline hands this call a `ZonedDateTime`, a
    * `LocalDate` or a `LocalDateTime` depending on which coercions ran, so the adjuster LAST_DAY
    * emits must resolve on ALL THREE -- asked of the JDK, which is the only authority on that.
    * Restoring `withDayOfMonth`/`lengthOfMonth` reddens this test on `ZonedDateTime`.
    *
    * Over a bare column the receiver is now the doc value made a DATE, so the methods called on the
    * parameter are the conversion's (`toInstant`, on the doc value) and the adjuster's (`with`);
    * both are asked of the JDK, the conversion of the doc value's class alone.
    */
  it should "emit only methods that exist on every receiver type the pipeline can produce" in {
    val emitted = scriptOf("SELECT LAST_DAY(d) FROM t")
    withClue(s"the doc value is converted before the adjuster: $emitted ") {
      emitted should include(".toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(")
    }
    Seq(classOf[ZonedDateTime], classOf[LocalDate], classOf[LocalDateTime]).foreach { receiver =>
      withClue(s"with is not declared on ${receiver.getName}: ") {
        receiver.getMethods.exists(_.getName == "with") shouldBe true
      }
    }

    // 🔴 EVERY call on EVERY bound operand, not the first one on the first. The defect was in the
    // INNER call -- `param1.withDayOfMonth(param1.lengthOfMonth())` -- whose outer `withDayOfMonth`
    // is perfectly fine on all three receivers. A `findFirstMatchIn` here PASSED with the broken
    // emission restored, which is why this is spelled out: a mechanism test that cannot see the
    // defect it exists for is a proxy, not a guard.
    //
    // ⚠️ What it does NOT see, stated rather than implied: the method NAME is checked, not the
    // signature, and a call on a static receiver (`TemporalAdjusters.lastDayOfMonth()`) or on a
    // literal is invisible to it. Those are settled by `DateFunctionExecutionSpec`, which executes
    // the emission on a real Elasticsearch. This check is the cheap one that fails at the right
    // place with the right message.
    val methods = """param\d+\.(\w+)\(""".r.findAllMatchIn(emitted).map(_.group(1)).toList
    withClue(s"no method call on the operand in: $emitted") { methods should not be empty }

    Seq(classOf[ZonedDateTime]).foreach { receiver =>
      methods.foreach { method =>
        withClue(s"$method is not declared on ${receiver.getName} (emitted: $emitted): ") {
          receiver.getMethods.exists(_.getName == method) shouldBe true
        }
      }
    }
  }

  // -- a function over CAST(<date column> AS DATE|TIME) -------------------------------------------

  /** 🔴 The consuming function converted its argument from the COLUMN's type, not from what the
    * argument renders: a schema-resolved identifier's `baseType` is its column's runtime type
    * (TIMESTAMP for a `date` field) whatever functions wrap it, so over `CAST(d AS DATE)` the
    * TIMESTAMP -> DATE conversion ran a second time onto the `LocalDate` the cast had produced, and
    * `LocalDate` has no `toInstant()`. MEASURED on Elasticsearch 8.18.3 and 6.8.23: the shard
    * fails, an HTTP 400 at a concrete index and 0 rows with HTTP 200 at a pattern or a list whose
    * other indices hold no document.
    */
  "a function over CAST(<date column> AS DATE)" should "convert its argument once" in {
    scriptOf("SELECT DATE_FORMAT(CAST(d AS DATE), '%Y-%m') FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param3 = DateTimeFormatter.ofPattern(\"yyyy-MM\"); " +
    "(param1 == null) ? null : param3.format(param2)"

    scriptOf("SELECT LAST_DAY(CAST(d AS DATE)) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "(param1 == null) ? null : param2.with(TemporalAdjusters.lastDayOfMonth())"
  }

  it should "emit exactly ONE conversion of the column, whatever the consumer" in {
    Seq(
      "SELECT DATE_FORMAT(CAST(d AS DATE), '%Y-%m') FROM t"     -> ".toLocalDate()",
      "SELECT LAST_DAY(CAST(d AS DATE)) FROM t"                 -> ".toLocalDate()",
      "SELECT DATE_FORMAT(CAST(ts AS DATE), '%Y-%m-%d') FROM t" -> ".toLocalDate()",
      "SELECT DATE_FORMAT(CAST(ts AS TIME), '%H:%i') FROM t"    -> ".toLocalTime()"
    ).foreach { case (sql, narrowing) =>
      val emitted = scriptOf(sql)
      withClue(s"[$sql] $emitted: ") {
        emitted.sliding(".toInstant()".length).count(_ == ".toInstant()") shouldBe 1
        emitted.sliding(narrowing.length).count(_ == narrowing) shouldBe 1
      }
    }
  }

  /** The JDK class of what `doc['f'].value` hands a QUERY script for each column of [[schema]]. */
  private val docValueClass: Map[String, Class[_]] =
    Map(
      "d"    -> classOf[ZonedDateTime],
      "ts"   -> classOf[ZonedDateTime],
      "name" -> classOf[String],
      "n"    -> classOf[java.lang.Long]
    )

  /** What a `date` doc value IS, per major. On Elasticsearch 7+ a `java.time.ZonedDateTime`. On 6.8
    * an `org.elasticsearch.script.JodaCompatibleZonedDateTime`, which is NOT that class: it lacks
    * methods `ZonedDateTime` declares, and `toLocalTime()` is the one MEASURED absent on 6.8.23
    * (see `painlessUtcZonedDateTime`). So 6.8 is modelled as `ZonedDateTime` minus the methods
    * measured absent -- a method it lacks that was never measured is NOT caught here, which is what
    * `DateFunctionExecutionSpec` (executed on every major) is for.
    */
  private case class ReceiverModel(major: String, dateDocValueLacks: Set[String])
  private val Es7Plus = ReceiverModel("7+", Set.empty)
  private val Es68 = ReceiverModel("6.8", Set("toLocalTime"))

  private val staticClass: Map[String, Class[_]] = Map(
    "DateTimeFormatter" -> classOf[DateTimeFormatter],
    "ZoneId"            -> classOf[ZoneId],
    "LocalDate"         -> classOf[LocalDate],
    "LocalTime"         -> classOf[LocalTime],
    "LocalDateTime"     -> classOf[LocalDateTime],
    "ZonedDateTime"     -> classOf[ZonedDateTime],
    "Instant"           -> classOf[Instant],
    "TemporalAdjusters" -> classOf[TemporalAdjusters],
    "ChronoUnit"        -> classOf[ChronoUnit],
    "ChronoField"       -> classOf[ChronoField],
    "String"            -> classOf[String]
  )

  /** Every method call of a query `script` checked against the class of the value it is called ON:
    * the class of a `def paramN` is that of its longest call chain (a guard's bare `paramN` loses
    * to the conversion beside it), a doc value's comes from [[docValueClass]] and `model`, a static
    * receiver's from [[staticClass]]. Answers the calls that do not resolve, and how many did.
    *
    * ⚠️ It checks RECEIVERS, not arguments: `DATE_FORMAT(d, …)` passes here on the 6.8 model, and
    * 6.8 refuses it because the formatter's ARGUMENT is the doc value.
    */
  private def receiverCheck(script: String, model: ReceiverModel = Es7Plus): (List[String], Int) = {
    // the class of a parameter, and whether it is a RAW date doc value (no call applied yet)
    val env = scala.collection.mutable.Map.empty[String, (Class[_], Boolean)]
    val unresolved = scala.collection.mutable.ListBuffer.empty[String]
    var resolved = 0

    def closing(s: String, open: Int): Int = {
      var depth = 0
      var i = open
      var quote: Option[Char] = None
      while (i < s.length) {
        val c = s.charAt(i)
        quote match {
          case Some(q) => if (c == q) quote = None
          case None =>
            if (c == '\'' || c == '"') quote = Some(c)
            else if (c == '(') depth += 1
            else if (c == ')') { depth -= 1; if (depth == 0) return i }
        }
        i += 1
      }
      s.length - 1
    }

    val receiver = """^(?:doc\['(\w+)'\]\.value|\((param\d+)\)|(param\d+)\b|([A-Z]\w*))""".r
    val member = """^\.(\w+)""".r

    /** The class of `expr`'s longest call chain at THIS level; call arguments are checked too. */
    def typeOf(expr: String): Option[(Class[_], Boolean)] = {
      var best: Option[(Int, Class[_], Boolean)] = None
      var i = 0
      while (i < expr.length) {
        val c = expr.charAt(i)
        val boundary =
          i == 0 || !(expr.charAt(i - 1).isLetterOrDigit || expr.charAt(i - 1) == '.' ||
          expr.charAt(i - 1) == '_')
        if (c == '\'' || c == '"') {
          val end = expr.indexOf(c.toInt, i + 1)
          i = if (end < 0) expr.length else end + 1
        } else
          receiver.findFirstMatchIn(expr.substring(i)).filter(_ => boundary) match {
            case Some(m) =>
              val start: Option[(Class[_], Boolean)] =
                Option(m.group(1))
                  .flatMap(docValueClass.get)
                  .map(k => (k, k == classOf[ZonedDateTime]))
                  .orElse(Option(m.group(2)).orElse(Option(m.group(3))).flatMap(env.get))
                  .orElse(Option(m.group(4)).flatMap(staticClass.get).map(k => (k, false)))
              var cls: Option[Class[_]] = start.map(_._1)
              var raw = start.exists(_._2)
              val static = m.group(4) != null
              val known = cls.isDefined
              var calls = 0
              var j = i + m.end
              var chaining = known
              while (chaining && j < expr.length) member.findFirstMatchIn(expr.substring(j)) match {
                case Some(n) =>
                  val name = n.group(1)
                  val after = j + n.end
                  if (after < expr.length && expr.charAt(after) == '(') {
                    val end = closing(expr, after)
                    typeOf(expr.substring(after + 1, end))
                    cls.foreach { k =>
                      if (raw && model.dateDocValueLacks.contains(name)) {
                        unresolved += s"$name on the ${model.major} date doc value"; cls = None
                      } else
                        k.getMethods.filter(_.getName == name).toList match {
                          case Nil =>
                            unresolved += s"$name on ${k.getName}"; cls = None
                          case found =>
                            // a generic interface's BRIDGE (`ChronoLocalDate toLocalDate()`) is
                            // not the class the call returns
                            resolved += 1
                            cls = Some(found.find(!_.isBridge).getOrElse(found.head).getReturnType)
                        }
                    }
                    raw = false
                    calls += 1
                    j = end + 1
                  } else {
                    cls = cls.flatMap(k => k.getFields.find(_.getName == name).map(_.getType))
                    raw = false
                    j = after
                  }
                case None => chaining = false
              }
              if (known && !(static && calls == 0))
                cls.foreach(k => if (best.forall(_._1 <= calls)) best = Some((calls, k, raw)))
              i = math.max(j, i + 1)
            case None => i += 1
          }
      }
      best.map(b => (b._2, b._3))
    }

    val statements = scala.collection.mutable.ListBuffer.empty[String]
    var start = 0
    var depth = 0
    script.indices.foreach { k =>
      script.charAt(k) match {
        case '('               => depth += 1
        case ')'               => depth -= 1
        case ';' if depth == 0 => statements += script.substring(start, k).trim; start = k + 1
        case _                 =>
      }
    }
    statements += script.substring(start).trim
    val declaration = """^def (param\d+) = (.*)$""".r
    statements.foreach {
      case declaration(name, rhs) => typeOf(rhs).foreach(t => env(name) = t)
      case other                  => typeOf(other)
    }
    (unresolved.toList, resolved)
  }

  /** 🔴 The MECHANISM: the emitted calls must exist on the class of the value each is called ON --
    * asked of the JDK, over schema-attached shapes whose function takes a CHAINED argument, on the
    * receivers of BOTH model majors. The check of every receiver at once (above) cannot see this
    * defect: `toInstant()` is right on the column's `ZonedDateTime` and wrong on the cast's
    * `LocalDate`, and only the receiver decides.
    */
  it should "emit only calls that resolve on the class of their receiver" in {
    // the checker sees the defect it exists for: the double conversion emitted before this fix
    receiverCheck(
      "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
      "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
      "def param3 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
      "(param1 == null) ? null : param3.with(TemporalAdjusters.lastDayOfMonth())"
    )._1 shouldBe List("toInstant on java.time.LocalDate")

    // ...and, on the 6.8 model, a TIME narrowing applied straight to the raw doc value -- through a
    // parameter or inline -- while the normalised one resolves
    Seq(
      "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); param1.toLocalTime()",
      "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value.toLocalTime()); param1"
    ).foreach { script =>
      receiverCheck(script, Es68)._1 shouldBe List("toLocalTime on the 6.8 date doc value")
      receiverCheck(script, Es7Plus)._1 shouldBe empty
    }
    receiverCheck(
      "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
      "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalTime() : null)",
      Es68
    )._1 shouldBe empty

    Seq(
      "DATE_FORMAT(CAST(d AS DATE), '%Y-%m')",
      "LAST_DAY(CAST(d AS DATE))",
      "DAY(LAST_DAY(CAST(d AS DATE)))",
      "DATE_FORMAT(CAST(ts AS DATE), '%Y-%m-%d')",
      "LAST_DAY(CAST(ts AS DATE))",
      "DATE_FORMAT(CAST(ts AS TIME), '%H:%i')",
      "DATE_FORMAT(DATE_TRUNC(CAST(d AS DATE), MONTH), '%Y-%m')",
      "DATE_FORMAT(DATE_ADD(CAST(d AS DATE), INTERVAL 1 DAY), '%Y-%m')",
      "DATE_FORMAT(CAST(name AS DATE), '%Y')",
      "DATE_FORMAT(DATE_PARSE(name, '%Y-%m-%d'), '%Y')",
      // a CASE over casts: DATE, TIME, a literal ELSE, no ELSE
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS DATE) END",
      "CASE WHEN n > 0 THEN CAST(ts AS TIME) ELSE CAST(d AS TIME) END",
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST('2024-01-01' AS DATE) END",
      "CASE WHEN n > 0 THEN CAST(ts AS TIME) ELSE CAST('12:00:00' AS TIME) END",
      "CASE WHEN n > 0 THEN CAST(d AS DATE) END",
      "CASE n WHEN 1 THEN CAST(ts AS TIME) END",
      // controls: right before the fix, and kept right
      "DATETIME_FORMAT(CAST(ts AS TIME), '%H:%i')",
      "YEAR(CAST(d AS DATE))",
      "EPOCHDAY(CAST(d AS DATE))",
      "DATE_TRUNC(CAST(d AS DATE), MONTH)",
      "LAST_DAY(DATE_TRUNC(d, MONTH))",
      "LAST_DAY(d)",
      "DATE_FORMAT(d, '%Y-%m')"
    ).foreach { e =>
      val emitted = scriptOf(s"SELECT $e FROM t")
      Seq(Es7Plus, Es68).foreach { model =>
        val (unresolved, resolved) = receiverCheck(emitted, model)
        withClue(s"[$e] on ${model.major}: $emitted: ") {
          resolved should be > 0
          unresolved shouldBe empty
        }
      }
    }
  }

  /** 🔴 A CASE keeps 0.23.0's conversion of its results: each is converted FROM its `baseType` to
    * the CASE's `out`, and both read a schema-resolved column's type whatever functions wrap it.
    * These pins are the bytes a CASE rendered before the fix of a function over a cast, so that fix
    * cannot move a CASE: a flat CASE over two casts, a CASE beside an unmapped-type column (no
    * `doc['dn'].value.toLocalDate()`), and a CASE nested in a THEN -- each answers right.
    *
    * A CASE beside a DATE literal or a bare timestamp column is deliberately NOT pinned: it still
    * answers a `LocalDate` in one branch and a `ZonedDateTime` in the other (two classes in one
    * column), and typing a CASE from what its results render, deferred, is what fixes it.
    */
  "a CASE" should "convert its results as before" in {
    // over two casts
    scriptOf(
      "SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS DATE) END FROM t"
    ) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param5"

    // an unmapped-type ELSE: its doc value is NOT narrowed
    scriptOf("SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE dn END FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['dn'].size() == 0 ? null : doc['dn'].value); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param4"

    // a CASE nested in a THEN
    scriptOf(
      "SELECT CASE WHEN n > 0 THEN CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE CAST(ts AS DATE) END " +
      "ELSE CAST(ts AS DATE) END FROM t"
    ) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 1); " +
    "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param6 = param1 == null ? false : (param1 > 0); " +
    "param6 ? param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : " +
    "param5 : param5"
  }

  // -- EPOCHDAY, and the accessor rule behind it ---------------------------------------------------

  "EPOCHDAY" should "read a field whose range does not fit an int with getLong" in {
    scriptOf("SELECT EPOCHDAY(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : " +
    "doc['d'].value.toInstant().atZone(ZoneId.of('Z')).getLong(ChronoField.EPOCH_DAY)); param1"
  }

  it should "leave every other field on get" in {
    scriptOf("SELECT YEAR(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : " +
    "doc['d'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"
  }

  // -- the INGEST (processor) context ------------------------------------------------------------

  /** The processor emission for the same two functions, through the production seam
    * (`CreateTable(...).schema.columns` -- `Table.update()` is what resolves them; the raw parsed
    * column list carries the UNRESOLVED processors and pinning it looks like an inconsistent fix).
    */
  private def processorSource(sql: String, column: String): String =
    Parser(sql) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == column)
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$column] in [$sql]"))
      case other => fail(s"[$sql] expected a CreateTable, got $other")
    }

  /** 🔴 Both functions were broken in an INGEST script too, and this is the venue where it hurts
    * most: a processor carries `ignore_failure: true`, so the throw is SWALLOWED and the computed
    * column is simply ABSENT -- no error, anywhere. MEASURED by deploying these exact sources as
    * real ingest pipelines on ES 6.8.23 and 8.18.3 over `{"created":"2025-02-10"}`:
    * {{{
    * LAST_DAY  before: c <<absent>>    after: c = 2025-02-28T00:00:00.000Z
    * EPOCHDAY  before: c <<absent>>    after: c = 20129
    * }}}
    */
  "the ingest processor emission" should "carry the same two fixes" in {
    processorSource(
      "CREATE TABLE t (created DATE, c DATE SCRIPT AS (LAST_DAY(created)))",
      "c"
    ) should endWith(
      "ctx.c = (param1 == null) ? null : param2.with(TemporalAdjusters.lastDayOfMonth())"
    )

    processorSource(
      "CREATE TABLE t (created DATE, c BIGINT SCRIPT AS (EPOCHDAY(created)))",
      "c"
    ) should include(".getLong(ChronoField.EPOCH_DAY)")
  }

  /** 🔴 The mechanism again, and the reason `TimeField.accessor` lives on the FIELD: `get` is
    * refused for exactly those fields whose value RANGE does not fit an `int`
    * (`TemporalAccessor.get`'s contract, which `LocalDate.get0` implements by throwing
    * `UnsupportedTemporalTypeException`). So the JDK -- not a list kept here -- decides which
    * accessor each field takes, and the next field with that property is caught on arrival.
    *
    * The population is the SEALED `TimeField` hierarchy walked to its leaves, so the compiler
    * guarantees it is complete; the count is asserted so a silent empty walk cannot pass.
    */
  "every TimeField" should "declare the accessor its JDK range requires" in {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    def leaves(sym: ru.ClassSymbol): Set[ru.ClassSymbol] = {
      val subs = sym.knownDirectSubclasses.map(_.asClass)
      if (subs.isEmpty) Set(sym) else subs.flatMap(leaves)
    }

    val fields: List[TimeField] =
      leaves(ru.typeOf[TimeField].typeSymbol.asClass).toList.map { c =>
        withClue(s"$c is not a case object: ") { c.isModuleClass shouldBe true }
        mirror.reflectModule(c.module.asModule).instance.asInstanceOf[TimeField]
      }

    fields should have size 15

    val jdkField: String => TemporalField = {
      case "QUARTER_OF_YEAR"         => IsoFields.QUARTER_OF_YEAR
      case "WEEK_OF_WEEK_BASED_YEAR" => IsoFields.WEEK_OF_WEEK_BASED_YEAR
      case name                      => ChronoField.valueOf(name)
    }

    fields.foreach { f =>
      val expected = if (jdkField(f.timeField).range().isIntValue) ".get" else ".getLong"
      withClue(s"${f.sql} (ChronoField.${f.timeField}): ") { f.accessor shouldBe expected }
    }

    // ...and the rule is not vacuous: it partitions the set rather than agreeing with everything.
    fields.count(_.accessor == ".getLong") shouldBe 1
  }
}
