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

import app.softnetwork.elastic.sql.operator.{AND, EQ}
import app.softnetwork.elastic.sql.{
  asString,
  renderName,
  Alias,
  Expr,
  Identifier,
  NamePart,
  Source,
  Token,
  TokenRegex,
  Updateable
}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

case object From extends Expr("FROM") with TokenRegex

sealed trait JoinType extends TokenRegex

case object InnerJoin extends Expr("INNER") with JoinType

// ANSI SQL allows the optional `OUTER` keyword after LEFT / RIGHT / FULL — `LEFT OUTER JOIN`
// is the same as `LEFT JOIN`. The `words` override extends the parser-side regex to accept
// both forms while keeping the SQL round-trip rendering (`sql` value) on the short form.
// Longer alternatives come first so regex alternation prefers them on longest-match.
case object LeftJoin extends Expr("LEFT") with JoinType {
  override def words: List[String] = List("LEFT\\s+OUTER", "LEFT")
}

case object RightJoin extends Expr("RIGHT") with JoinType {
  override def words: List[String] = List("RIGHT\\s+OUTER", "RIGHT")
}

case object FullJoin extends Expr("FULL") with JoinType {
  override def words: List[String] = List("FULL\\s+OUTER", "FULL")
}

case object CrossJoin extends Expr("CROSS") with JoinType

case object On extends Expr("ON") with TokenRegex

case class JoinKey(table: String, tableAlias: String, field: String) extends Token {
  def sql: String = s"$tableAlias.$field"
  lazy val key: String = s"${table}_$field"
}

object JoinKey {
  def apply(identifier: Identifier): Option[JoinKey] = {
    identifier.table match {
      case Some(ta) if identifier.name.nonEmpty =>
        Some(JoinKey(ta, identifier.tableAlias.getOrElse(ta), identifier.name))
      case _ => None
    }
  }
}

case class JoinKeyMatch(left: JoinKey, right: JoinKey) extends Token {
  def sql: String = s"${left.sql} = ${right.sql}"

  lazy val joinKeys: Seq[JoinKey] = Seq(left, right)
}

object JoinKeyMatch {
  def apply(expression: Expression): Option[JoinKeyMatch] = {
    if (expression.operator != EQ) return None
    (expression.identifier, expression.maybeValue) match {
      case (leftId: Identifier, Some(rightId: Identifier)) =>
        (JoinKey(leftId), JoinKey(rightId)) match {
          case (Some(leftKey), Some(rightKey)) =>
            Some(JoinKeyMatch(leftKey, rightKey))
          case _ => None
        }
      case _ => None
    }
  }
}

case class On(criteria: Criteria) extends Updateable {
  override def sql: String = s" $On $criteria"
  def update(request: SingleSearch): On = this.copy(criteria = criteria.update(request))

  private def extractJoinKeyMatches(criteria: Criteria): Seq[JoinKeyMatch] = {
    criteria match {
      case e: Expression if e.operator == EQ => JoinKeyMatch(e).toSeq
      case p: Predicate if p.operator == AND =>
        extractJoinKeyMatches(p.leftCriteria) ++ extractJoinKeyMatches(p.rightCriteria)
      case _ => Seq.empty
    }
  }

  /* Extracted join keys from the ON criteria */
  lazy val joinKeyMatches: Seq[JoinKeyMatch] = extractJoinKeyMatches(criteria)

  private def validateOnCriteria(criteria: Criteria): Either[String, Unit] = {
    criteria match {
      case e: Expression if e.operator == EQ =>
        if (e.identifier.name.isEmpty) Left(s"ON clause $this identifier cannot be empty")
        else if (e.identifier.functions.nonEmpty)
          Left(s"ON clause $this cannot use functions in equality expressions")
        else if (e.maybeNot.isDefined)
          Left(s"ON clause $this cannot use NOT operator in equality expressions")
        else if (e.maybeValue.isEmpty)
          Left(s"ON clause $this equality expressions must compare two identifiers")
        else {
          e.maybeValue.get match {
            case id: Identifier =>
              if (id.name.isEmpty)
                Left(s"ON clause $this identifier cannot be empty")
              else if (id.functions.nonEmpty)
                Left(s"ON clause $this cannot use functions in equality expressions")
              else Right(())
            case _ => Left(s"ON clause $this equality expressions must compare two identifiers")
          }
        }
      case p: Predicate =>
        p.operator match {
          case AND =>
            for {
              _ <- validateOnCriteria(p.leftCriteria)
              _ <- validateOnCriteria(p.rightCriteria)
            } yield ()
          case _ => Left(s"ON clause $this must use AND predicate operator")
        }
      case _ => Left(s"ON clause $this must use either equality operator or AND predicate")
    }
  }

  override def validate(): Either[String, Unit] = {
    for {
      _ <- super.validate()
      _ <- criteria.validate()
      _ <- validateOnCriteria(criteria)
    } yield ()
  }
}

case object Join extends Expr("JOIN") with TokenRegex

sealed trait Join extends Updateable {
  def source: Source
  def joinType: Option[JoinType]
  def on: Option[On]
  def alias: Option[Alias]
  override def sql: String =
    s" ${asString(joinType)} $Join $source${asString(alias)}${asString(on)}"

  override def update(request: SingleSearch): Join

  /** The ON requirement lives on `StandardJoin` alone (story 21.2 AD-7).
    *
    * This method used to carry a `joinType.isDefined && on.isEmpty && joinType.get != CrossJoin`
    * arm, and that predicate was unsatisfiable for EVERY input, before and after AD-7: `Unnest`
    * fixes `joinType = None` so `joinType.isDefined` is never true for it, and `StandardJoin`
    * overrides `validate` and returns on its own `on` match before delegating here, so by the time
    * this runs either `on` is defined or the join is a CROSS JOIN. AD-7 made the exemption real by
    * reproducing it in `StandardJoin.validate`; the copy here was left behind as dead code and is
    * deleted rather than kept as a second, unreachable statement of the same rule.
    */
  override def validate(): Either[String, Unit] = source.validate()
}

case object Unnest extends Expr("UNNEST") with TokenRegex

case class Unnest(
  identifier: Identifier,
  limit: Option[Limit],
  alias: Option[Alias] = None,
  parent: Option[Unnest] = None
) extends Source
    with Join {
  override def sql: String = s"$Join $Unnest($identifier)${asString(alias)}"
  def update(request: SingleSearch): Unnest = {
    val updated = this.copy(
      identifier = identifier.withNested(true).update(request),
      limit = limit.orElse(request.limit)
    )
    updated.identifier.tableAlias match {
      case Some(alias) if updated.identifier.nested =>
        request.unnests.get(alias) match {
          case Some(parent) /*if parent.path != updated.path*/ =>
            val unnest = updated.copy(parent = Some(parent))
            request.unnests += unnest.alias.map(_.alias).getOrElse(unnest.name) -> unnest
            return unnest
          case _ =>
        }
      case _ =>
    }
    updated
  }

  override val name: String = {
    val parts = identifier.name.split('.')
    if (parts.length <= 1) identifier.name
    else parts.tail.mkString(".")
  }

  def innerHitsName: String = alias.map(_.alias).getOrElse(name)

  def path: String = parent match {
    case Some(p) => s"${p.path}.$name"
    case None    => name
  }

  override def source: Source = identifier

  override def joinType: Option[JoinType] = None

  override def on: Option[On] = None

  override def validate(): Either[String, Unit] =
    for {
      _ <- super.validate()
      _ <-
        if (identifier.name.contains('.')) Right(())
        else Left(s"UNNEST identifier $identifier must be a nested field")
    } yield ()

}

case class StandardJoin(
  source: Source,
  joinType: Option[JoinType], // INNER JOIN by default
  on: Option[On],
  alias: Option[Alias] = None,
  /** The JOIN source as the ordered part list the statement wrote — same contract as `Table.parts`
    * (#85, story 21.2 AD-1/AD-5). Empty for a programmatic construction.
    */
  parts: Seq[NamePart] = Nil
) extends Join {

  /** The full dotted reference, un-quoted — the alias-map key when a bare name is ambiguous (AD-6).
    * Equals `source.name` whenever the reference carries no qualifier.
    */
  lazy val qualifiedName: String = Table.qualifiedName(parts, source.name)

  /** Overridden because `Join.sql` renders `$source` through `Identifier.sql`, which quotes per
    * dot-separated part (story 21.2 AD-5). A JOIN source is a TABLE name, whose dots are literal,
    * so it takes `Table.render` — the same whole-lexeme rule the FROM side uses.
    */
  override def sql: String =
    s" ${asString(joinType)} $Join ${Table.render(parts, source.name)}" +
    s"${asString(alias)}${asString(on)}"

  override def update(request: SingleSearch): StandardJoin = {
    // The source of a JOIN is a TABLE name, not a column expression — `Identifier.update`
    // would treat the bare table name as `alias.column` and strip the (nonexistent) column
    // part, leaving `name=""`. Only the ON clause needs to be updated for column resolution.
    this.copy(on = on.map(_.update(request)))
  }

  /** 🔴 FIX (story 21.2 AD-7) — the ON requirement now honours the `CrossJoin` exemption that
    * `Join.validate` has always carried but could never reach. This arm returned BEFORE
    * `Join.validate` ran, so `SELECT o.id FROM orders o CROSS JOIN customers c` was rejected with
    * *"Standard JOIN CROSS JOIN customers AS c requires an ON clause"* while the exemption sat
    * there as dead code.
    *
    * `joinType.contains(CrossJoin)`, never `on.isEmpty` alone: a bare `JOIN x` with no ON
    * (`joinType = None`) must STAY rejected, and `Join.validate`'s own guard skips it
    * (`joinType.isDefined` is false there), so this arm is its only rejection.
    */
  override def validate(): Either[String, Unit] = {
    for {
      _ <- on match {
        case Some(o)                              => o.validate()
        case None if joinType.contains(CrossJoin) => Right(()) // CROSS JOIN needs no ON clause
        case None => Left(s"Standard JOIN $this requires an ON clause")
      }
      _ <- super.validate()
    } yield ()
  }
}

object Table {

  /** The full dotted reference, un-quoted, qualifier parts included. Equals `name` whenever the
    * reference carries no qualifier — and for a programmatic construction, which has no `parts`.
    */
  def qualifiedName(parts: Seq[NamePart], name: String): String =
    if (parts.isEmpty) name else parts.map(_.value).mkString(".")

  /** Renders a table reference from its ordered part list (story 21.2 AD-1 rule 5).
    *
    * The rules and the reasons live with the ONE implementation, `sql.renderName` — story 21.7
    * needed the same rendering for every DDL/DML statement that names an object, and a second copy
    * here is exactly the "one key, two derivations" drift story 21.3 paid for four times. This
    * alias is kept because `Table.render` is the name the FROM/JOIN surface documents.
    */
  def render(parts: Seq[NamePart], name: String): String = renderName(parts, name)
}

case class Table(
  name: String,
  tableAlias: Option[Alias] = None,
  joins: Seq[Join] = Nil,
  /** The table reference as the ordered part list the statement wrote — never split, never
    * role-assigned, never dropped (#85, story 21.2 AD-1). Empty for a programmatic construction.
    *
    * 🔴 `name` stays byte-for-byte the value it has always had and is what every consumer reads
    * (`SingleSearch.sources = from.tables.map(_.name)`). `parts` is new information read by NOTHING
    * in `sql`, `core`, `bridge` or any ES client: it exists for a resolver that knows the venue —
    * jdbc/Flight read one qualifier as the schema (= the cluster; the catalog is the constant
    * `elasticsearch`), Federation reads it as a server alias, and a resolver that knows the
    * registered catalogs may split a quoted lexeme's internal dots (BigQuery's `proj.ds.tbl`).
    * Today Federation re-derives this with a regex over raw SQL (`JoinPlanner.extractCatalogList`,
    * #85).
    *
    * ⚠️ A consumer that resolves it must treat an UNREGISTERED qualifier as "no qualifier", not as
    * an error: after story 20.3 the JDBC driver advertises the cluster name as the schema, so a BI
    * tool's qualifier is routinely a name no registry knows.
    */
  parts: Seq[NamePart] = Nil
) extends Source {

  /** The full dotted reference (qualifier parts included), un-quoted — the alias-map key when a
    * bare name is ambiguous (AD-6). Equals `name` whenever the reference carries no qualifier.
    */
  lazy val qualifiedName: String = Table.qualifiedName(parts, name)

  override def sql: String =
    s"${Table.render(parts, name)}${asString(tableAlias)} ${joins.map(_.sql).mkString(" ")}".trim
  def update(request: SingleSearch): Table =
    this.copy(joins = joins.map(_.update(request)))

  override def validate(): Either[String, Unit] =
    for {
      _ <- tableAlias match {
        case Some(a) if a.alias.isEmpty => Left(s"Table $name alias cannot be empty")
        case _                          => Right(())
      }
      _ <- joins.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    } yield ()

  lazy val joinedTables: Seq[String] = joins.collect { case sj: StandardJoin =>
    sj.source.name
  }

  lazy val enrichmentRequired: Boolean = joinedTables.nonEmpty

}

case class From(tables: Seq[Table]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val unnests: Seq[Unnest] = joins.collect { case u: Unnest => u }

  /** Every TABLE-shaped reference in this FROM as (bare name, qualified reference), in statement
    * order: the FROM tables first, then every standard JOIN leg. UNNEST is deliberately absent —
    * its operand is a column path, not a table.
    */
  private lazy val tableReferences: Seq[(String, String)] =
    tables.map(t => t.name -> t.qualifiedName) ++ joins.collect { case sj: StandardJoin =>
      sj.source.name -> sj.qualifiedName
    }

  /** Bare index names this FROM uses for MORE THAN ONE distinct qualified reference — the only case
    * where keying the alias maps by the bare name silently drops an alias (story 21.2 AD-6).
    */
  private lazy val ambiguousTableNames: Set[String] =
    tableReferences
      .groupBy(_._1)
      .collect { case (name, refs) if refs.map(_._2).distinct.size > 1 => name }
      .toSet

  /** 🔴 FIX (story 21.2 AD-6) — the alias-map key.
    *
    * MEASURED before this story: `SELECT a FROM "prod_us".orders o, "prod_eu".orders p` produced
    * `ListMap(orders -> p)`. Both tables stripped to the bare index `orders`, the `ListMap` kept
    * the last, and alias `o` was **silently gone** — so `o.a` resolved against nothing and queried
    * a field literally named `o.a`. This story makes the backtick and JOIN spellings of that
    * statement reachable too, i.e. exactly the cross-qualifier shape Federation exists for.
    *
    * The key is therefore the QUALIFIED reference — but **only for a bare name this FROM uses more
    * than once under different qualifiers**, which is the only shape where the bare key has no
    * correct answer. Keying every qualified table by its qualified name unconditionally was the
    * simpler spelling and was rejected on measurement: `tableAliases` is read by KEY and by REVERSE
    * lookup outside this file, and both readings expect the bare INDEX name — softclient4es-
    * extensions' `JoinDependencyGraph`/`FieldAnalyzer` look tables up in a `schemas` map keyed by
    * the bare index name, so under an unconditional qualified key a MATERIALIZED VIEW over `FROM
    * "elastic".orders o JOIN …` — which parses today — would silently plan with no fields. "No
    * regression" is half of the ruling, not a footnote to it.
    *
    * ⚠️ **Narrowing the rule does not by itself keep the rest of `sql` in step — a companion change
    * was needed and is `joinSourceKeys` below.** `Identifier.update` (`sql/package.scala`) reverse-
    * looks-up this map and puts the KEY into `Identifier.table`, so in the ambiguous branch that
    * value is the qualified reference. Anything inside `sql` that compares `Identifier.table`
    * against an index name must therefore speak the same key language: `TemporalLiterals` did not,
    * and its cross-index join-leg guard stopped firing for `FROM orders o JOIN "prod_eu".orders p`
    * until it was routed through `joinSourceKeys`. **Any new consumer of `Identifier.table`
    * inherits that obligation.**
    */
  private def aliasKey(name: String, qualified: String): String =
    if (ambiguousTableNames.contains(name)) qualified else name

  lazy val tableAliases: ListMap[String, String] = ListMap(
    tables
      .flatMap((table: Table) =>
        table.tableAlias match {
          case Some(alias) if alias.alias.nonEmpty =>
            Some(aliasKey(table.name, table.qualifiedName) -> alias.alias)
          case _ => Some(aliasKey(table.name, table.qualifiedName) -> table.name)
        }
      ): _*
  ) ++ unnestAliases.map(unnest => unnest._2._1 -> unnest._1) ++ joinReferences.map {
    case (alias, (name, qualified)) =>
      // The SAME key rule as the tables above, which is what keeps two same-name JOIN legs under
      // different qualifiers apart here: MEASURED, `FROM x JOIN "a".orders p JOIN "b".orders q`
      // yields `ListMap(x -> x, a.orders -> p, b.orders -> q)`. Keying them by the bare name is
      // what would collapse them, even though `joinAliases` (keyed by the ALIAS) keeps them apart.
      aliasKey(name, qualified) -> alias
  }

  /** The join-leg index names AS THIS MAP KEYS THEM — `aliasKey` applied to every standard JOIN
    * leg, so a consumer holding an `Identifier.table` (which IS a `tableAliases` key) can compare
    * against it.
    *
    * 🔴 It exists because `joinAliases`' bare `sj.source.name` and `Identifier.table` are the same
    * thing only while the key is bare. MEASURED before this accessor, on `SELECT o.a, p.b FROM
    * orders o JOIN "prod_eu".orders p ON o.cid = p.id`: `identifier.table` was `prod_eu.orders`
    * while the bare set was `{orders}`, so `TemporalLiterals`' guard — "this column belongs to a
    * cross-index JOIN source, its mapping is not the one in hand" — stopped firing and the literal
    * was resolved against the WRONG index's schema.
    */
  lazy val joinSourceKeys: Set[String] =
    joinReferences.values.map { case (name, qualified) => aliasKey(name, qualified) }.toSet

  lazy val aliasesToTable: ListMap[String, String] = tableAliases.map(_.swap)

  lazy val joins: Seq[Join] = tables.flatMap(_.joins)

  /** alias -> (bare index name, qualified reference) for every standard JOIN leg. Keyed by the
    * ALIAS and built exactly like `joinAliases` below, so two legs sharing an alias collapse here
    * the same way they always have; it exists only to carry the qualified reference the alias-map
    * key needs (AD-6).
    */
  private lazy val joinReferences: ListMap[String, (String, String)] = ListMap(
    joins.collect { case sj: StandardJoin =>
      sj.alias.map(_.alias).getOrElse(sj.source.name) -> (sj.source.name, sj.qualifiedName)
    }: _*
  )

  lazy val joinAliases: ListMap[String, (String, Option[On])] = ListMap(
    joins.collect { case sj: StandardJoin =>
      (
        sj.alias
          .map(_.alias)
          .getOrElse(
            sj.source.name
          ),
        (sj.source.name, sj.on)
      )
    }: _*
  )

  lazy val unnestAliases: ListMap[String, (String, Option[Limit])] = ListMap(
    unnests
      .map(u => // extract unnest info
        (u.alias.map(_.alias).getOrElse(u.name), (u.name, u.limit))
      ): _*
  )

  def update(request: SingleSearch): From =
    this.copy(tables = tables.map(_.update(request)))

  override def validate(): Either[String, Unit] = {
    if (tables.isEmpty) {
      Left("At least one table is required in FROM clause")
    } else if (tables.count(_.joins.nonEmpty) > 1) {
      Left("Only one table with joins is supported in FROM clause")
    } else {
      for {
        _ <- tables.map(_.validate()).filter(_.isLeft) match {
          case Nil    => Right(())
          case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
        }
      } yield ()
    }
  }

  lazy val mainTable: Table = tables.head

  lazy val joinedTables: Seq[String] = tables.flatMap(_.joinedTables)

  lazy val enrichmentRequired: Boolean = joinedTables.nonEmpty
}

case class NestedElement(
  path: String,
  innerHitsName: String,
  size: Option[Int],
  children: Seq[NestedElement] = Nil, // TODO remove and use parent instead
  sources: Seq[String] = Nil,
  parent: Option[NestedElement]
) {
  lazy val root: NestedElement = {
    parent match {
      case Some(p) => p.root
      case None    => this
    }
  }

  lazy val level: Int = {
    parent match {
      case Some(p) => 1 + p.level
      case None    => 0
    }
  }

  lazy val nestedPath: String = {
    parent match {
      case Some(p) => s"${p.nestedPath}>$innerHitsName"
      case None    => innerHitsName
    }
  }
}

object NestedElements {

  def buildNestedTrees(nestedElements: Seq[NestedElement]): Seq[NestedElement] = {
    if (nestedElements.isEmpty) return Nil
    val nestedParentsPath: collection.mutable.Map[String, (NestedElement, Seq[NestedElement])] =
      collection.mutable.Map.empty

    val distinctNestedElements = nestedElements.groupBy(_.path).map(_._2.head).toList

    val distinctNestedElementsByRoot =
      distinctNestedElements
        .groupBy(_.root.path)
        .map(tree => tree._1 -> tree._2.sortBy(_.level).reverse)

    @tailrec
    def getNestedParents(
      n: NestedElement,
      parents: Seq[NestedElement]
    ): NestedElement = {
      n.parent match {
        case Some(p) =>
          if (!nestedParentsPath.contains(p.path)) {
            p.copy(children = Nil)
            nestedParentsPath += p.path -> (p, Seq(n))
            getNestedParents(p, p +: parents)
          } else {
            nestedParentsPath += p.path -> (p, nestedParentsPath(p.path)._2 :+ n)
            p
          }
        case _ => n
      }
    }

    val nestedParents =
      distinctNestedElementsByRoot.values.flatten
        .map(de => getNestedParents(de, Seq.empty))
        .toSeq
        .distinct

    def innerBuildNestedTree(n: NestedElement): NestedElement = {
      val children = nestedParentsPath.get(n.path).map(_._2).getOrElse(Seq.empty)
      if (children.nonEmpty) {
        val updatedChildren = children.map(innerBuildNestedTree)
        n.copy(children = updatedChildren.groupBy(_.path).map(_._2.head).toSeq)
      } else {
        n
      }
    }

    if (nestedParents.nonEmpty) {
      val trees = nestedParents.map(innerBuildNestedTree)
      trees
    } else {
      distinctNestedElements
    }
  }

  def walkNestedTree(n: NestedElement)(f: NestedElement => Unit): Unit = {
    f(n)
    n.children.foreach(child => walkNestedTree(child)(f))
  }
}
