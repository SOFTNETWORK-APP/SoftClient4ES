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

package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  query,
  Expr,
  GenericIdentifier,
  Identifier,
  LiteralParam,
  PainlessContext,
  PainlessOperandForm,
  PainlessScript,
  StringValue,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{
  SQLAny,
  SQLBool,
  SQLNumeric,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes,
  SQLVarchar
}
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.schema.Column
import app.softnetwork.elastic.sql.query.{
  Criteria,
  CriteriaWithConditionalFunction,
  Expression,
  TemporalLiterals
}

package object cond {

  sealed trait ConditionalOp extends PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext] = None): String = sql
  }

  case object Coalesce extends Expr("COALESCE") with ConditionalOp
  case object IsNull extends Expr("ISNULL") with ConditionalOp
  case object IsNotNull extends Expr("ISNOTNULL") with ConditionalOp
  case object NullIf extends Expr("NULLIF") with ConditionalOp {

    /** Every `NULLIF` in an operand's chain whose two arguments do not RENDER comparable types, as
      * the rejection each one earns (issue #382).
      *
      * 🔴 Asked of a SCHEMA-RESOLVED expression and of nothing else, for the same reason
      * [[Case.conditionsOf]] 's consumer is: without a schema a bare column's type is `Any`, which
      * matches everything, so `NULLIF(s, 0)` is indistinguishable at parse time from a legal
      * `NULLIF(n, 0)`. `NullIf.validate()` runs there and cannot see it.
      *
      * ONE walk, [[functionsOf]], shared with `Case` -- a `NULLIF` nested in another `NULLIF`'s
      * argument, in a `CASE` branch or under `ABS(...)` is the same `NULLIF`.
      */
    def mismatchesOf(chain: FunctionChain): Seq[String] =
      functionsOf(chain).collect { case n: NullIf => n }.flatMap(_.typeMismatchError)
  }
  case object Greatest extends Expr("GREATEST") with ConditionalOp
  case object Least extends Expr("LEAST") with ConditionalOp

  case object Case extends Expr("CASE") with ConditionalOp {

    /** The `WHEN` conditions of every `CASE` in an operand's chain, as the [[Criteria]] they are
      * (issue #384, item 1).
      *
      * 🔴 ONE walk, because two places need it and they must not drift: a SELECT-list / ORDER BY
      * `CASE` in a SEARCH (`SingleSearch.validateResolved`) and a computed column's stored
      * expression in DDL (`schema.validateScriptReferences`). A comparison written inside a CASE is
      * the same comparison as one written in WHERE, and it used to be checked in neither.
      *
      * ⚠️ `Case.args` deliberately EXCLUDES the conditions (it returns the expression, the THEN
      * results and the ELSE), so they are unreachable through the chain and have to be read off
      * `conditions` -- which is exactly why nothing had ever looked at them.
      */
    def conditionsOf(chain: FunctionChain): Seq[Criteria] =
      functionsOf(chain).collect { case c: Case => c }.flatMap {
        _.conditions.collect { case (criteria: Criteria, _) => criteria }
      }

  }

  /** Every function reachable from `chain`: the chain itself, the arguments of every `FunctionN` in
    * it, and -- because `Case.args` deliberately excludes them -- a `CASE`'s `THEN` results and its
    * `CASE <expr>` operand.
    *
    * 🔴 ARGUMENTS as well as the chain, and that is not a refinement -- it is the difference
    * between refusing a wrong answer and shipping one. `transformFunctions` walks the chain APPLIED
    * TO an operand, so it sees `CASE WHEN ... END` but not the CASE inside `ABS(CASE WHEN ... END)`
    * or `(CASE WHEN ... END) + 1`. MEASURED (issue #384): the wrapped form was accepted and stored
    * `c = 0.0` for every document, while the bare form was correctly refused -- found by review,
    * one function away from the shape the check did see.
    *
    * 🔴 ONE walk for every post-resolution rule that asks "where is a node of kind K in this
    * expression?" -- [[Case.conditionsOf]] (issue #384) and [[NullIf.mismatchesOf]] (issue #382).
    * Two walks would be two answers to the same question, and the second one would inherit none of
    * the corrections the first one was given.
    */
  private[elastic] def functionsOf(chain: FunctionChain): Seq[Function] = {
    def from(c: FunctionChain, depth: Int): Seq[Function] =
      if (depth > MaxNesting) Nil
      else
        FunctionUtils.transformFunctions(c).flatMap { fn =>
          (fn +: (fn match {
            case cs: Case =>
              cs.conditions.collect { case (_, result: FunctionChain) => result }.flatMap {
                from(_, depth + 1)
              } ++
                cs.expression.toSeq
                  .collect { case f: FunctionChain => f }
                  .flatMap(from(_, depth + 1))
            case _ => Nil
          })) ++ (fn match {
            case n: FunctionN[_, _] =>
              n.args.collect { case f: FunctionChain => f }.flatMap(from(_, depth + 1))
            case _ => Nil
          })
        }

    from(chain, 0)
  }

  /** A depth stop for [[functionsOf]]. A CASE inside a CASE inside a function is already beyond
    * anything measured; the bound exists so a cyclic or pathological AST cannot hang validation,
    * not because the depth is meaningful.
    */
  private val MaxNesting = 12

  case object WHEN extends Expr("WHEN") with TokenRegex
  case object THEN extends Expr("THEN") with TokenRegex
  case object ELSE extends Expr("ELSE") with TokenRegex
  case object END extends Expr("END") with TokenRegex

  sealed trait ConditionalFunction[In <: SQLType]
      extends TransformFunction[In, SQLBool]
      with FunctionWithIdentifier {
    def conditionalOp: ConditionalOp

    override def fun: Option[PainlessScript] = Some(conditionalOp)

    override def outputType: SQLBool = SQLTypes.Boolean

  }

  case class IsNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} == null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNULL requires exactly one argument")
      }

    override def update(request: query.SingleSearch): IsNull = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case class IsNotNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNotNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} != null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNOTNULL requires exactly one argument")
      }

    override def update(request: query.SingleSearch): IsNotNull = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case class Coalesce(values: List[PainlessScript])
      extends TransformFunction[SQLAny, SQLType]
      with FunctionWithIdentifier {
    def operator: ConditionalOp = Coalesce

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLType =
      baseType //SQLTypeUtils.leastCommonSuperType(args.map(_.baseType))

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

    // Reprend l’idée de SQLValues mais pour n’importe quel token
    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      if (values.isEmpty) Left("COALESCE requires at least one argument")
      else Right(())
    }

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case Nil => throw new IllegalArgumentException("COALESCE requires at least one argument")
        // 🔴 `COALESCE(x)` IS `x`. Without this arm the fold below took `values.length - 1 = 0`
        // arguments, so the `" : "` join produced nothing and the tail appended a closing paren that
        // opened nowhere: `String.valueOf( : param1))`, which Painless cannot parse. Degenerate SQL,
        // legal SQL, and UNREACHABLE until issue #367 stopped predicates from dropping the operand's
        // transform — `WHERE COALESCE(d) = 'x'` compared the raw column before and emits this now.
        case single :: Nil => single.trim
        // 🔴 A RIGHT fold, because the old one opened `values.length - 1` parentheses and closed
        // exactly ONE: at two arguments that balances by luck, and at three or more it emitted
        // `(a != null ? a : (b != null ? b : c)` — Painless cannot parse it, so `COALESCE(a, b, c)`
        // was a shard failure in EVERY venue, SELECT included, and no test had ever looked. Found by
        // the delimiter check in `PredicateTransformSurvivalSpec`. Byte-identical at two arguments,
        // which is what every existing fixture pins.
        case _ =>
          callArgs.init.foldRight(callArgs.last.trim) { (arg, rest) =>
            s"(${arg.trim} != null ? ${arg.trim} : $rest)"
          }
      }

    override def nullable: Boolean = values.forall(_.nullable)

    override def update(request: query.SingleSearch): Coalesce = {
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
    }
  }

  /** A rendering that may be spliced into [[NullIf]] 's templates exactly as it stands: a Painless
    * NAME, or a LITERAL (a number, a quoted string, `true` / `false` / `null` -- all of which the
    * name pattern already covers). Everything else is COMPOUND and must be bound, so the pattern is
    * deliberately narrow: answering "simple" about a compound rendering is the defect, while
    * answering "compound" about a name costs one redundant local. Compiled ONCE, here, rather than
    * per AST node.
    */
  private val simpleOperand =
    """(?:[A-Za-z_][A-Za-z0-9_]*|-?\d+(?:\.\d+)?[lLfFdD]?|"(?:[^"\\]|\\.)*")""".r

  case class NullIf(expr1: PainlessScript, expr2: PainlessScript)
      extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = NullIf

    override def args: List[PainlessScript] = List(expr1, expr2)

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    /** The type an argument's rendering really PRODUCES -- the question [[receiverIsTime]] three
      * lines below already asks of the receiver, asked here of BOTH arguments (issue #382).
      *
      * 🔴 `FunctionN.argTypes` answers `_.out`, and a schema-resolved `Identifier.out` reports the
      * COLUMN's type, not the chain's. So `NULLIF(CAST(s AS BIGINT), 0)` over a KEYWORD column
      * reported `List(KEYWORD, BIGINT)`, `leastCommonSuperType` came out VARCHAR, and
      * [[toPainlessCall]] took its `case SQLTypes.Varchar` arm -- whose `$arg1 != null` guard over
      * a primitive numeric literal Elasticsearch refuses to compile:
      * {{{
      * def param3 = param2 == null || (0 != null && param2.compareTo(0) == 0) ? null : param2;
      *   class_cast_exception: Cannot cast from [int] to [java.lang.Object].
      * }}}
      * MEASURED on ES 8.18.3, and identical for `CAST`, `TRY_CAST` and `::`. With the chain's type
      * the arm is the numeric one, `param2 == 0 ? null : param2`, which compiles and answers `null`
      * / `125` / `null` for `"0"` / `"125"` / a missing column.
      *
      * This is [[app.softnetwork.elastic.sql.operator.math.ArithmeticExpression]] 's `argTypeOf`
      * (the same issue's OQ-2) in a second function: `argTypes` is the single input to
      * [[baseType]], `baseType` feeds `out`, and `out` is what [[toPainlessCall]] switches on -- so
      * one derivation fixes every venue, and it also stops the expression DESCRIBING itself as a
      * string to a CTAS target mapping.
      *
      * ⚠️ This derivation answers which ARM a comparable pair takes. It says nothing about a pair
      * that is not comparable at all (`NULLIF(s, 0)`, where the operand really is a KEYWORD): no
      * arm of [[toPainlessCall]] is right for those, and they are REFUSED by [[typeMismatchError]],
      * which reads the very same derivation.
      */
    private def argTypeOf(arg: PainlessScript): SQLType = arg match {
      case i: Identifier => i.chainType
      case other         => other.out
    }

    override def argTypes: List[SQLType] = args.map(argTypeOf)

    /** The rejection this `NULLIF` earns when its two arguments RENDER types SQL does not compare
      * (issue #382, the LEAD RULING of 2026-09-23).
      *
      * 🔴 A type mismatch is not an emission problem; it is a type ERROR, and the engine says so
      * instead of building a script around it. Every arm of [[toPainlessCall]] is wrong for a
      * TEXT-against-NUMBER pair and each is wrong in its own way, all MEASURED on ES 8.18.3:
      *   - the string arm's `compareTo` throws PER DOCUMENT (`wrong_method_type_exception: cannot
      *     convert MethodHandle(String,String)int to (Object,int)Object`), and in a computed column
      *     `ScriptProcessor.ignoreFailure` defaults to `true`, so the throw is SWALLOWED and the
      *     column silently DISAPPEARS -- HTTP 200 over a document missing the value it asked for,
      *     the #205 family;
      *   - its `$arg1 != null` guard over a primitive numeric literal does not even compile
      *     (`class_cast_exception: Cannot cast from [int] to [java.lang.Object]`), which is how
      *     `NULLIF(CAST(n AS KEYWORD), 0)` used to fail;
      *   - the numeric arm's `==` compiles for any pair and answers `false` for a `Long` against a
      *     `String`, so the `NULLIF` would never null anything -- silently.
      *
      * Emitting nothing at all was considered and rejected for the same reason: an absent column is
      * the silent failure, not an escape from it.
      *
      * '''The rule is deliberately NARROW''' (lead, 2026-09-23) -- exactly TEXT against NUMBER,
      * plus the temporal-literal edge below. `SQLTypeUtils.matches` was implemented first and
      * MEASURED to refuse 540 further shapes in the same corpus, among them TEMPORAL-against-NUMBER
      * (`NULLIF(d, 0)`) and every BOOLEAN pair (`NULLIF(b, 0)`, `NULLIF(b, 'x')`). Those emit today
      * exactly what they emitted before and are RECORDED as a known boundary, not refused: no
      * emission for them was measured to be wrong, and a validator that refuses what it has not
      * measured is the same mistake in the other direction.
      *
      * The comparison is between what each argument RENDERS ([[argTypeOf]]), never between the
      * COLUMNS' declared types: `NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))` reports
      * `List(TIMESTAMP, DATE)` on `out` and `List(DATE, DATE)` on the chain, and it WORKS -- asking
      * `out` would refuse a shape this engine executes correctly today. And a cast can CREATE the
      * mismatch as well as cure it: `NULLIF(CAST(n AS KEYWORD), 0)` renders TEXT against NUMBER
      * over a numeric column, and IS refused.
      *
      * 🔴 A `NULL` operand is never refused, and STRUCTURALLY so rather than by a guard: SQL says
      * `NULLIF(a, NULL)` is UNKNOWN, so the answer is `a`, and `SQLTypes.Null` is neither an
      * `SQLVarchar` nor an `SQLNumeric`, so no widening of either predicate can start refusing it.
      * An explicit short-circuit was written first and MEASURED to change no verdict in the whole
      * corpus -- dead by construction, so it is the invariant that is recorded, not the code.
      * `NULLIF(s, NULL)` and `NULLIF(NULL, 0)` pin it.
      */
    /** Can [[toPainlessCall]] express a comparison between these two renderings at all?
      *
      * 🔴 DERIVED FROM THE ARMS, not written alongside them. The arms are: both TEXT (`compareTo`),
      * both TEMPORAL (`isEqual` / `compareTo`), and a final `==` that Painless applies to any pair
      * without complaining. An earlier spelling listed the ONE pair it had measured -- TEXT against
      * NUMBER -- and everything else fell through to `==`. That is how a TEMPORAL or BOOLEAN
      * operand against a NUMBER or a text LITERAL reached `==`: Painless answers `false` for
      * `ZonedDateTime == String` and for `Boolean == Long`, never throwing, so `NULLIF` returned
      * its first argument for EVERY row, HTTP 200, in every venue -- the silent always-false this
      * very rule refuses TEXT-against-NUMBER to avoid. A predicate written beside the arms says
      * nothing when a new arm appears; one derived FROM them cannot drift.
      *
      * ⚠️ UNKNOWN accepts. An unresolved column reports `Any` (no schema attached: a wildcard or
      * multi-index FROM, a mapping that failed to load), and refusing there would reject legitimate
      * SQL on every schema-less path while the resolved spelling of the same statement is fine. A
      * NULL operand accepts for the same reason, and SQL agrees: `NULLIF(a, NULL)` is `a`.
      */
    private def comparable(left: SQLType, right: SQLType): Boolean =
      left.isUnknown || right.isUnknown ||
      (left.isText && right.isText) ||
      (left.isTemporal && right.isTemporal) ||
      (left.isNumber && right.isNumber) ||
      (left.isBoolean && right.isBoolean)

    /** A TEMPORAL operand against a text one. Refused -- but by [[temporalLiteralError]] FIRST,
      * because it can say something far more useful than the type names.
      *
      * 🔴 Both halves are refusals today, and the reason is the EMITTER, not the types.
      * [[toPainlessCall]] has no way to compare a `ZonedDateTime` with a `String`: the pair folds
      * to VARCHAR, misses the text arm (one side is not text), misses the temporal arm (`out` is
      * not temporal) and lands on `==`, which Painless evaluates to `false` for every document
      * without ever throwing. `NULLIF(created_at, '2025-01-01')` therefore returned `created_at`
      * for every row, HTTP 200, in every venue -- measured, and the reason this rule exists.
      *
      * Making it WORK needs a `String -> temporal` coercion `SQLTypeUtils.coerce` does not have
      * (its temporal arms are all temporal-to-temporal), one per subtype, in three venues, on four
      * Elasticsearch majors -- and Elasticsearch DATE MATH (`now-1d`) has no Painless equivalent at
      * all, so that spelling can never be coerced and would have to be refused by name. That is a
      * story, not a guard, and it is scoped as one. Until it lands the answer is LOUD.
      */
    private def temporalAgainstText(left: SQLType, right: SQLType): Boolean =
      (left.isTemporal && right.isText) || (left.isText && right.isTemporal)

    def typeMismatchError: Option[String] = {
      val left = argTypeOf(expr1)
      val right = argTypeOf(expr2)
      if (temporalAgainstText(left, right))
        // The resolver goes first so a MALFORMED literal is named as such (`'x'` is not a date for
        // this field's format) rather than reported as a bare type mismatch; a WELL-FORMED one
        // falls through to a message that says what is actually wrong -- the engine, not the SQL.
        temporalLiteralError(expr1, left, expr2, right)
          .orElse(temporalLiteralError(expr2, right, expr1, left))
          .orElse(
            Some(
              s"$sql compares ${left.typeId} with ${right.typeId}: comparing a temporal value " +
              "with a string is not supported yet, so cast the string to the column's type"
            )
          )
      else if (!comparable(left, right))
        Some(
          s"$sql compares ${left.typeId} with ${right.typeId}: NULLIF requires two arguments of " +
          "comparable types, so cast one of them"
        )
      else None
    }

    /** The edge the type rule CANNOT decide: a string literal against a date-mapped column.
      *
      * 🔴 MEASURED: `NULLIF(d, '2025-01-01')` and `NULLIF(d, 'x')` present IDENTICALLY as
      * `TIMESTAMP` against `VARCHAR`, so no rule written over types alone can accept the first and
      * refuse the second -- and refusing both would reject legitimate SQL. The lead's ruling is to
      * ask the SAME machinery a `WHERE` clause already asks (issue #276's
      * [[TemporalLiterals.normalizeLiteral]]), against the column's own mapping `format`, so there
      * is ONE date-literal grammar in the engine and not two.
      *
      * It is reachable from both seams because it is a pure function of (literal, field, format)
      * and the resolved identifier carries its [[Column]]. Only the VERDICT is used: `NULLIF` does
      * not rewrite its operand the way `WHERE` does, so a space-form literal is accepted and
      * forwarded exactly as before -- recorded on #382, not changed here.
      *
      * `TIME` is excluded by [[TemporalLiterals.isTemporalColumn]] (Elasticsearch has no time-only
      * mapping), and a function-wrapped operand on EITHER side is excluded: a cast or a function
      * makes the comparison one between what the CHAINS render, not one against the mapped field.
      */
    private def temporalLiteralError(
      column: PainlessScript,
      columnType: SQLType,
      literal: PainlessScript,
      literalType: SQLType
    ): Option[String] =
      if (!columnType.isTemporal || !literalType.isText) None
      else
        for {
          col   <- mappedColumn(column).filter(c => TemporalLiterals.isTemporalColumn(c.dataType))
          value <- stringLiteral(literal)
          field <- Some(column).collect { case id: Identifier => id.name }
          reason <- TemporalLiterals
            .normalizeLiteral(value, field, TemporalLiterals.FieldFormat.of(col))
            .left
            .toOption
        } yield reason

    /** The mapped column a BARE operand names, or `None` for anything else.
      *
      * 🔴 The parser wraps EVERY operand in an `Identifier`, literals included
      * (`Parser.functionAsIdentifier`), so "is this a column?" is not `isInstanceOf[Identifier]` --
      * it is "does it carry a name and no functions, and did the schema attach a [[Column]] to
      * it?". MEASURED: a first version asked the type only, found `expr2` to be a
      * `GenericIdentifier(name = "", functions = List(StringValue))`, and the whole temporal edge
      * silently never fired.
      */
    private def mappedColumn(operand: PainlessScript): Option[Column] = operand match {
      case id: GenericIdentifier if id.name.nonEmpty && id.functions.isEmpty => id.col
      case _                                                                 => None
    }

    /** The text of a BARE string literal operand -- `'2025-01-01'`, not `CAST('x' AS DATE)`. */
    private def stringLiteral(operand: PainlessScript): Option[String] = operand match {
      case id: GenericIdentifier if id.name.isEmpty =>
        id.functions match {
          case List(value: StringValue) => Some(value.value)
          case _                        => None
        }
      case value: StringValue => Some(value.value)
      case _                  => None
    }

    /** Whether the value `expr1` RENDERS is a `java.time.LocalTime`. `out` and `expr1.out` both
      * report the column's type; `Identifier.chainType` reports the chain's (#367).
      */
    private[this] def receiverIsTime: Boolean = expr1 match {
      case i: Identifier => i.chainType == SQLTypes.Time
      case other         => other.out == SQLTypes.Time
    }

    private[this] def checkIfExpressionNullable(expr: PainlessScript): Boolean = expr match {
      case f: FunctionChain if f.functions.nonEmpty => true
      case _                                        => false
    }

    override def checkIfNullable: Boolean =
      false //checkIfExpressionNullable(expr1) || checkIfExpressionNullable(expr2)

    /** Bind an argument's rendering to a prologue local unless it is already a simple operand.
      *
      * 🔴 Issue #382. `arg0` appears THREE times in the guarded template and `arg1` twice, both
      * spliced RAW -- and `&&`, `||` and `==` all bind tighter than `?:`, so a COMPOUND rendering
      * re-associates across the template. Two measured consequences, both on ES 8.18.3:
      * {{{
      * NULLIF(UPPER(s), 'X')                         -- arg0 renders `(p == null) ? null : p.toUpperCase()`
      *   (p == null) ? null : p.toUpperCase() == null || ("X" != null && (p == null) ? null : ...)
      *   illegal_argument_exception: Cannot cast null to a primitive type [boolean].   LOUD
      *
      * NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)  -- the NUMERIC arm, `$arg0 == $arg1 ? null : $arg0`
      *   param2 ? 1 : 2 == 1 ? null : param2 ? 1 : 2
      *   reads as `param2 ? 1 : ((2 == 1) ? null : …)`, so n = 5 stored c = 1 where SQL says NULL.
      *   HTTP 200 -- SILENT, the #205 family.                                          SILENT
      * }}}
      * The silent one is why this cannot be narrowed to the arms that fail loudly: the numeric arm
      * is correct only when the compound rendering happens to be a `guard ? null : value`, which is
      * luck, not a rule. Binding is applied at the ONE place both templates read their operands
      * from, so the two cannot drift.
      *
      * Binding also evaluates the operand ONCE (#373 item 9's "duplicated guarded expression"
      * family) -- `NULLIF(UPPER(s), 'X')` called `toUpperCase()` three times per document.
      *
      * ⚠️ With NO context there is no prologue to hoist into. Parentheses are the whole of the
      * association fix, so a context-free EXPRESSION gets them; a rendering that is not an
      * expression at all (a safe cast's `try` / `catch`) is beyond their help and is left exactly
      * as it was -- pre-existing, and not this repair's to change.
      *
      * 🔴 The `Some(ctx)` branch binds UNCONDITIONALLY, and that is NOT the shape
      * [[app.softnetwork.elastic.sql.operator.math.ArithmeticExpression]] uses: its `operandOf`
      * asks `PainlessOperandForm.placeable` BEFORE binding and falls back to the operand's
      * registered parameter when the rendering is a statement. It can be asked to bind a `try` /
      * `catch`; this site cannot, and the INVARIANT is why -- '''with a context, a safe cast has
      * already hoisted itself''', so what arrives in `callArgs` is the parameter NAME (`TRY_CAST(s
      * AS BIGINT)` renders `def safe1 = null; try {...} catch {...} def param2 = safe1;` into the
      * prologue and hands this method `param2`). A `placeable` guard here would therefore be an arm
      * no input can reach. The invariant is not asserted by reading the code: `NullIfOperandSpec`
      * checks it over the whole operand corpus, in every venue.
      */
    private[this] def operand(rendered: String, context: Option[PainlessContext]): String = {
      val trimmed = rendered.trim
      // An EMPTY rendering is what an AGGREGATE answers in a script venue (`NULLIF(COUNT(*), 0)`),
      // and the surrounding emission is already degenerate there. Binding it would only add `def
      // nif1 = ;` to it, so it is left exactly as it was -- pre-existing, recorded on #382.
      if (trimmed.isEmpty || simpleOperand.pattern.matcher(trimmed).matches()) trimmed
      else
        context match {
          case Some(ctx)                                      => ctx.bindLocal(trimmed, "nif")
          case None if PainlessOperandForm.placeable(trimmed) => s"($trimmed)"
          case None                                           => trimmed
        }
    }

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(rendered0, rendered1) =>
          val arg0 = operand(rendered0, context)
          val arg1 = operand(rendered1, context)
          // 🔴 The SECOND argument is guarded too (issue #373, found by review) -- the same hole
          // `checkCase` had, one method up in this file. `NULLIF(CAST(d AS TIME), CAST(ts AS
          // TIME))` called the comparison with a null `arg1` for any document missing `ts`:
          // MEASURED on ES 8.18, `null_pointer_exception: Cannot read field "hour" because
          // "other" is null`.
          //
          // 🔴 And the guard means NOT-EQUAL, not null. SQL says `NULLIF(a, b)` is NULL when
          // `a = b`; with `b` NULL the comparison is UNKNOWN, so the rows are NOT equal and the
          // answer is `a`. Short-circuiting `arg1 == null` to the NULL branch would invert that.
          def nullIf(comparison: String): String =
            s"$arg0 == null || ($arg1 != null && $comparison) ? null : $arg0"
          val expr =
            out match {
              // 🔴 `String.compareTo` takes a `String` and NOTHING else, so this arm is right only
              // when BOTH operands render text -- and `leastCommonSuperType` answers VARCHAR for a
              // MIXED pair too (`[BOOLEAN, KEYWORD]`, `[TIMESTAMP, KEYWORD]`). Without the guard
              // `NULLIF(b, CAST(n AS KEYWORD))` and `NULLIF(d, CAST(i AS VARCHAR))` emitted
              // `param1.compareTo(param2)` over a `Boolean` / `ZonedDateTime` receiver and threw
              // PER DOCUMENT on ES 8.18.3 -- which in a computed column `ignoreFailure` SWALLOWS,
              // so the column silently disappears. MEASURED: 32 corpus rows, all of them shapes
              // that answered a value before the type fix of this same issue moved the supertype
              // from the column's type to the chain's. They fall to the `==` arm below, which is
              // what they emitted before and which Painless applies to any pair without throwing.
              //
              // The TEXT-against-NUMBER half of the same population is refused instead, by
              // [[typeMismatchError]] -- `==` there would be a silent always-false.
              case SQLTypes.Varchar if argTypes.forall(_.isText) =>
                nullIf(s"$arg0.compareTo($arg1) == 0")
              // 🔴 Keyed on what the RECEIVER's chain RENDERS, not on `out` (issue #373).
              // `LocalTime` has no `isEqual`, and MEASURED on ES 8.18 before this,
              // `NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME))` failed the shard in a real
              // `script_fields`. Neither `out` nor `expr1.out` says so: both are TIMESTAMP, the
              // COLUMN's type -- `chainType` is the derivation that describes the rendering
              // (#367), and it answers TIME. Two wrong keys were tried before measuring it.
              case _: SQLTemporal if receiverIsTime =>
                nullIf(s"$arg0.compareTo($arg1) == 0")
              case _: SQLTemporal => nullIf(s"$arg0.isEqual($arg1)")
              // `==` is null-safe in Painless, so a numeric / boolean pair needs no guard.
              case _ => s"$arg0 == $arg1 ? null : $arg0"
            }
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(expr)) match {
                case Some(e) => return e
                case _       =>
              }
            case _ =>
          }
          expr
        case _ => throw new IllegalArgumentException("NULLIF requires exactly two arguments")
      }
    }

    override def validate(): Either[String, Unit] = {
      for {
        _ <- expr1.validate()
        _ <- expr2.validate()
        _ <- Validator.validateTypesMatching(expr1.out, expr2.out)
      } yield ()
    }

    override def update(request: query.SingleSearch): NullIf = {
      this.copy(
        expr1 = expr1 match {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        },
        expr2 = expr2 match {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        }
      )
    }
  }

  case class Case(
    expression: Option[PainlessScript],
    conditions: List[(PainlessScript, PainlessScript)],
    default: Option[PainlessScript]
  ) extends TransformFunction[SQLAny, SQLAny] {
    override def args: List[PainlessScript] = expression.toList ++
      conditions.map { case (_, res) => res } ++
      default.toList

    override def inputType: SQLAny = SQLTypes.Any

    override def outputType: SQLAny = SQLTypes.Any

    override def sql: String = {
      val exprPart = expression.map(e => s"$Case ${e.sql}").getOrElse(Case.sql)
      val whenThen = conditions
        .map { case (cond, res) => s"$WHEN ${cond.sql} $THEN ${res.sql}" }
        .mkString(" ")
      val elsePart = default.map(d => s" $ELSE ${d.sql}").getOrElse("")
      s"$exprPart $whenThen$elsePart $END"
    }

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      // Story 22.2 (AD-6) — a CASE-WHEN condition is parsed by `case_condition`, which shares
      // `whereCriteria` with WHERE, so a subquery node is grammatically reachable here. It is
      // reached at PARSE time (`Field.validate` -> `FunctionChain.validate` ->
      // `Validator.validateChain` -> `functions.map(_.validate())`), and the check below passes a
      // criteria (its `out` IS BOOLEAN), so without this arm the statement parses and dies later in
      // the node's `painless` throw — a rendering-time internal error where the analyst wants a
      // named rejection.
      val subquery = conditions.collectFirst {
        case (c: Criteria, _) if c.subqueries.nonEmpty => c
      }
      if (subquery.isDefined)
        Left(
          s"A subquery is not supported in a CASE WHEN condition: ${subquery.get.sql}. " +
          "Filter in WHERE, or compute the flag in a separate query."
        )
      else if (conditions.isEmpty) Left("CASE WHEN requires at least one condition")
      else if (
        expression.isEmpty && conditions.exists { case (cond, _) => cond.out != SQLTypes.Boolean }
      )
        Left("CASE WHEN conditions must be of type BOOLEAN")
      else if (
        expression.isDefined && conditions.exists { case (cond, _) =>
          !SQLTypeUtils.matches(cond.out, expression.get.out)
        }
      )
        Left("CASE WHEN conditions must be of the same type as the expression")
      else Right(())
    }

    /** Whether the CASE EXPRESSION -- the receiver of every `WHEN` comparison -- renders a
      * `java.time.LocalTime`. `out` reports the CASE's result type and cannot say (issue #373).
      */
    private[this] def receiverIsTime: Boolean = expression.exists {
      case i: Identifier => i.chainType == SQLTypes.Time
      case other         => other.out == SQLTypes.Time
    }

    /** @param candidateNullable
      *   whether `c` is a BOUND parameter that can hold `null`.
      *
      * 🔴 The CANDIDATE needs a guard too (issue #373). `CASE <expr> WHEN <column> THEN …` emitted
      * `e != null && e.isEqual(c) ? v` and called the comparison with `c == null` for any document
      * missing that column -- MEASURED on ES 8.18: `NullPointerException:
      * ChronoLocalDate.toEpochDay() because "other" is null`. `compareTo` on the `Varchar` arm has
      * exactly the same hole.
      *
      * ⚠️ The candidate is ALWAYS bound before it is guarded, so `c != null` costs one reference
      * and never a second rendering of its chain. See the call site for why no predicate decides
      * it: none of the three the AST offers can see a function-wrapped column's nullability.
      */
    private[this] def checkCase(
      e: String,
      c: String,
      v: String,
      candidateNullable: Boolean
    ): String = {
      val guard = if (candidateNullable) s"$e != null && $c != null" else s"$e != null"
      // 🔴 A TIME is decided by what the RECEIVER RENDERS, not by `out` (issue #373, found by
      // review). `out` is the CASE's RESULT type -- the least common supertype of the expression,
      // the conditions and the results -- and it reports TIMESTAMP for
      // `CASE CAST(d AS TIME) WHEN CAST(ts AS TIME) …` whose operands are both `LocalTime`, so an
      // arm keyed on it was DEAD and that shape still emitted `isEqual`. MEASURED on ES 8.18:
      // `dynamic method [java.time.LocalTime, isEqual/1] not found`, the very failure this PR's
      // first commit exists to remove. `Identifier.chainType` (#367) is the derivation that
      // describes the rendering -- the same key `NULLIF` needed.
      if (receiverIsTime) s"$guard && $e.compareTo($c) == 0 ? $v"
      else
        out match {
          case SQLTypes.Varchar =>
            s"$guard && $e.compareTo($c) == 0 ? $v"
          case _: SQLTemporal =>
            s"$guard && $e.isEqual($c) ? $v"
          // `==` is null-safe in Painless, so a numeric / boolean candidate needs no guard.
          case _ => s"$e == $c ? $v"
        }
    }

    /** A result rendering that can sit opposite a `null` branch.
      *
      * 🔴 Story BIDC-8 (round 10, MEDIUM-3). With no `ELSE`, SQL says the missing branch is NULL —
      * but Painless types a ternary from its BRANCHES, so `param2 ? 1 : null` is
      * `class_cast_exception: Cannot cast from [int] to [java.lang.Object]` (MEASURED live on ES
      * 8.18.3, and MEASURED again to confirm it is the absence of a `return` that makes it fatal:
      * the same expression compiles behind an explicit `return`). `(def)` on the result side is the
      * spelling that compiles in BOTH positions — also measured. Applied only when there is no
      * default, so every existing emission is byte-identical.
      */
    private def boxWhenNoDefault(rendered: String): String =
      // 🔴 Round 11 — PARENTHESISED. `(def)$rendered` casts only the first token, so a result that
      // is itself a ternary came out as `param4 ? (def)param3 ? (def)1 : null : null`, casting the
      // nested CONDITION rather than the value. Benign today (a `def` condition still works) and
      // wrong as written.
      if (default.isEmpty) s"(def)($rendered)" else rendered

    override def painless(context: Option[PainlessContext] = None): String = {
      context match {
        case Some(ctx) =>
          var cases =
            expression match {
              case Some(expr) => // case with expression to evaluate
                val e = SQLTypeUtils.coerce(expr, out, context)
                val expParam = ctx.addParam(
                  LiteralParam(e)
                )
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, out, context)
                    val r =
                      boxWhenNoDefault(res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      })
                    expParam match {
                      case Some(e) =>
                        // 🔴 ALWAYS bound, ALWAYS guarded (issue #373, found by review). The
                        // earlier version asked `cond.nullable`, and MEASURED that is false for a
                        // function-wrapped column -- `UPPER(other)` reports `nullable = false`
                        // while rendering `(param3 == null) ? null : param3.toUpperCase()` -- so
                        // `CASE UPPER(name) WHEN UPPER(other) …` was neither bound nor guarded and
                        // threw `null_pointer_exception: Cannot read field "value"` on ES 8.18.
                        //
                        // No predicate decides it any more, because none of the three I measured
                        // is reliable: the wrapper's `name` is EMPTY, its `nullable` is false, and
                        // its function is not a `FunctionWithIdentifier`, so the inner column is
                        // not reachable from here. The unconditional rule is provably safe
                        // instead: binding is a no-op when the candidate is already a name
                        // (`addParam` returns the existing one), and `c != null` on a candidate
                        // that cannot be null is redundant bytes, never a different answer.
                        ctx.addParam(LiteralParam(c)) match {
                          case Some(bound) => checkCase(e, bound, r, candidateNullable = true)
                          case _           => checkCase(e, c, r, candidateNullable = false)
                        }
                      case _ =>
                        checkCase(e, c, r, candidateNullable = false)
                    }
                  }
                  .mkString(" : ")
              case _ =>
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, SQLTypes.Boolean, context)
                    val r =
                      boxWhenNoDefault(res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      })
                    if (!cond.isInstanceOf[CriteriaWithConditionalFunction[_]] && cond.nullable) {
                      ctx.addParam(LiteralParam(c)) match {
                        case Some(c) => s"$c ? $r"
                        case _       => s"$c ? $r"
                      }
                    } else {
                      s"$c ? $r"
                    }
                  }
                  .mkString(" : ")
            }
          default match {
            case Some(df) =>
              val d = SQLTypeUtils.coerce(df, out, context)
              if (df.nullable) {
                ctx.addParam(LiteralParam(d)) match {
                  case Some(d) => cases = s"$cases : $d"
                  case _       => cases = s"$cases : $d"
                }
              } else {
                cases = s"$cases : $d"
              }
            // 🔴 Story BIDC-8 (round 10, MEDIUM-3). A `CASE … THEN x END` with NO `ELSE` used to
            // leave the ternary truncated — `param2 ? 1` — which Elasticsearch rejects at compile
            // time: `unexpected token ['<EOF>'] was expecting one of [':']` (MEASURED live on
            // 8.18.3). SQL says the missing ELSE is NULL, so that is what the else-branch emits.
            // Pre-existing; it sits on the surface this story's CASE gate exercises, and the gate
            // missed it because every fixture supplied an ELSE.
            case _ => cases = s"$cases : null"
          }

          cases

        case _ =>
          throw new IllegalArgumentException(s"Painless context is required for $sql")
      }
    }

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String =
      s"$base${painless(context)}"

    override def nullable: Boolean =
      conditions.exists { case (_, res) => res.nullable } || default.forall(_.nullable)

    override def update(request: query.SingleSearch): Case = {
      this.copy(
        expression = expression.map {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        },
        conditions = conditions.map { case (cond, res) =>
          val newCond = cond match {
            case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
            case other         => other
          }
          val newRes = res match {
            case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
            case other         => other
          }
          (newCond, newRes)
        },
        default = default.map {
          case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
          case other         => other
        }
      )
    }
  }

  /** N-ary numeric reducer shared by `GREATEST` / `LEAST`.
    *
    * Emits a right-folded nested ternary so a NULL argument is skipped and the whole expression
    * yields NULL only when every argument is NULL: pairwise(x, y) = (x == null ? y : (y == null ? x
    * : Math.{max|min}(x, y)))
    *
    * Output is `SQLNumeric` because `Math.max` / `Math.min` only operate on numeric primitives. The
    * base type narrows to the widest input numeric.
    */
  sealed trait NumericReducer
      extends TransformFunction[SQLAny, SQLNumeric]
      with FunctionWithIdentifier {
    def values: List[PainlessScript]
    def operator: ConditionalOp
    protected def mathFn: String // "Math.max" | "Math.min"

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes) match {
      case n: SQLNumeric => n
      case _             => outputType
    }

    override def sql: String = s"$operator(${values.map(_.sql).mkString(", ")})"

    override def checkIfNullable: Boolean = false

    override def validate(): Either[String, Unit] =
      if (values.isEmpty) Left(s"$operator requires at least one argument")
      else
        // Accept numeric args and still-unresolved args (SQLAny / NULL, which
        // extends SQLAny): a bare field has no known type until it is resolved
        // against the index mapping, so we must not reject it here. Only reject
        // args whose type is *definitively* non-numeric (string, temporal,
        // boolean, …) — those would emit Math.{max,min}() on a non-numeric and
        // fail at ES runtime.
        values.find { v =>
          v.out match {
            case _: SQLNumeric => false
            case _: SQLAny     => false
            case _             => true
          }
        } match {
          case Some(nonNumeric) =>
            Left(
              s"$operator requires numeric arguments but got ${nonNumeric.out} for ${nonNumeric.sql}"
            )
          case None => Right(())
        }

    override def nullable: Boolean = values.forall(_.nullable)

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      // Pair each rendered arg with its nullability so non-nullable args (e.g.
      // numeric literals, which render inline as primitives) are NOT guarded
      // with `== null` — Painless rejects `<primitive> == null` at compile time.
      // Right-fold pairwise: pairwise(a, pairwise(b, pairwise(c, …))).
      // The combined sub-tree is itself nullable only when BOTH sides can be
      // null, which lets us drop the guard on parent levels too.
      def fold(args: List[(String, Boolean)]): (String, Boolean) =
        args match {
          case Nil =>
            throw new IllegalArgumentException(s"$operator requires at least one argument")
          case (s, n) :: Nil => (s.trim, n)
          case (s, xNullable) :: rest =>
            val x = s.trim
            val (y, yNullable) = fold(rest)
            val expr =
              (xNullable, yNullable) match {
                case (true, true)   => s"($x == null ? $y : ($y == null ? $x : $mathFn($x, $y)))"
                case (true, false)  => s"($x == null ? $y : $mathFn($x, $y))"
                case (false, true)  => s"($y == null ? $x : $mathFn($x, $y))"
                case (false, false) => s"$mathFn($x, $y)"
              }
            // result is null only if every branch can be null
            (expr, xNullable && yNullable)
        }

      callArgs match {
        case Nil =>
          throw new IllegalArgumentException(s"$operator requires at least one argument")
        case x :: Nil => x
        case _        => fold(callArgs.zip(values.map(_.nullable)))._1
      }
    }
  }

  case class Greatest(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Greatest
    override protected def mathFn: String = "Math.max"

    override def update(request: query.SingleSearch): Greatest =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }

  case class Least(values: List[PainlessScript]) extends NumericReducer {
    override def operator: ConditionalOp = Least
    override protected def mathFn: String = "Math.min"

    override def update(request: query.SingleSearch): Least =
      this.copy(values = values.map {
        case u: Updateable => u.update(request).asInstanceOf[PainlessScript]
        case other         => other
      })
  }
}
