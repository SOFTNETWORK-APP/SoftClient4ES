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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch.SetOperationType
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** ONE rule types a set operation at every depth -- a column, a field of a struct, the element of a
  * list, a field of a struct inside a struct -- and this spec checks it against ONE table, written
  * here, independently of the code: for every set of two or three types, the rule's answer, and
  * beside it DuckDB 1.5.5's (MEASURED: the same at every depth, whatever the order). Every pair and
  * every triple, in every order, under `UNION ALL`, `UNION`, `INTERSECT` and `EXCEPT`, at every
  * depth: 13,120 statements.
  */
class SetOperationTypeRuleSpec extends AnyFlatSpec with Matchers {

  private val types: Seq[String] =
    Seq("INT", "BIGINT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "TIME", "VARBINARY", "VARCHAR")

  private val declared: Map[String, SQLType] = Map(
    "INT"       -> SQLTypes.Int,
    "BIGINT"    -> SQLTypes.BigInt,
    "DOUBLE"    -> SQLTypes.Double,
    "BOOLEAN"   -> SQLTypes.Boolean,
    "DATE"      -> SQLTypes.Date,
    "TIMESTAMP" -> SQLTypes.Timestamp,
    "TIME"      -> SQLTypes.Time,
    "VARBINARY" -> SQLTypes.VarBinary,
    "VARCHAR"   -> SQLTypes.Keyword
  )

  /** Each type at each depth: `c_<type>` a column, `f_<type>` a struct's field `a`, `e_<type>` a
    * list's element, `s_<type>` the field `c` of a struct field `p`; `NULL` a NULL literal.
    */
  private val schema = Table(
    "t",
    columns = types.toList.flatMap { name =>
      val t = declared(name)
      List(
        Column(s"c_$name", t),
        Column(s"f_$name", SQLTypes.Struct, multiFields = List(Column("a", t))),
        Column(s"e_$name", SQLTypes.Array(t)),
        Column(
          s"s_$name",
          SQLTypes.Struct,
          multiFields = List(Column("p", SQLTypes.Struct, multiFields = List(Column("c", t))))
        )
      )
    }
  )

  private val depths: Seq[(String, String)] =
    Seq("column" -> "c", "field" -> "f", "element" -> "e", "nested" -> "s")

  /** THE TABLE: a set of types, the rule's answer for one node holding them (REFUSED, or the type),
    * and DuckDB 1.5.5's answer for the same node (FAILS, or the type), MEASURED at every depth.
    */
  private val table: Map[Set[String], (String, String)] =
    """
      |INT+INT                      INT        INT
      |INT+BIGINT                   BIGINT     BIGINT
      |INT+DOUBLE                   DOUBLE     DOUBLE
      |INT+BOOLEAN                  INT        INT
      |INT+DATE                     REFUSED    FAILS
      |INT+TIMESTAMP                REFUSED    FAILS
      |INT+TIME                     VARCHAR    FAILS
      |INT+VARBINARY                VARCHAR    FAILS
      |INT+VARCHAR                  VARCHAR    VARCHAR
      |INT+NULL                     INT        INT
      |BIGINT+BIGINT                BIGINT     BIGINT
      |BIGINT+DOUBLE                DOUBLE     DOUBLE
      |BIGINT+BOOLEAN               BIGINT     BIGINT
      |BIGINT+DATE                  REFUSED    FAILS
      |BIGINT+TIMESTAMP             REFUSED    FAILS
      |BIGINT+TIME                  VARCHAR    FAILS
      |BIGINT+VARBINARY             VARCHAR    FAILS
      |BIGINT+VARCHAR               VARCHAR    VARCHAR
      |BIGINT+NULL                  BIGINT     BIGINT
      |DOUBLE+DOUBLE                DOUBLE     DOUBLE
      |DOUBLE+BOOLEAN               DOUBLE     DOUBLE
      |DOUBLE+DATE                  REFUSED    FAILS
      |DOUBLE+TIMESTAMP             REFUSED    FAILS
      |DOUBLE+TIME                  VARCHAR    FAILS
      |DOUBLE+VARBINARY             VARCHAR    FAILS
      |DOUBLE+VARCHAR               VARCHAR    VARCHAR
      |DOUBLE+NULL                  DOUBLE     DOUBLE
      |BOOLEAN+BOOLEAN              BOOLEAN    BOOLEAN
      |BOOLEAN+DATE                 REFUSED    FAILS
      |BOOLEAN+TIMESTAMP            REFUSED    FAILS
      |BOOLEAN+TIME                 VARCHAR    FAILS
      |BOOLEAN+VARBINARY            VARCHAR    FAILS
      |BOOLEAN+VARCHAR              VARCHAR    VARCHAR
      |BOOLEAN+NULL                 BOOLEAN    BOOLEAN
      |DATE+DATE                    DATE       DATE
      |DATE+TIMESTAMP               TIMESTAMP  TIMESTAMP
      |DATE+TIME                    VARCHAR    FAILS
      |DATE+VARBINARY               VARCHAR    FAILS
      |DATE+VARCHAR                 VARCHAR    VARCHAR
      |DATE+NULL                    DATE       DATE
      |TIMESTAMP+TIMESTAMP          TIMESTAMP  TIMESTAMP
      |TIMESTAMP+TIME               VARCHAR    FAILS
      |TIMESTAMP+VARBINARY          VARCHAR    FAILS
      |TIMESTAMP+VARCHAR            VARCHAR    VARCHAR
      |TIMESTAMP+NULL               TIMESTAMP  TIMESTAMP
      |TIME+TIME                    TIME       TIME
      |TIME+VARBINARY               VARCHAR    FAILS
      |TIME+VARCHAR                 VARCHAR    VARCHAR
      |TIME+NULL                    TIME       TIME
      |VARBINARY+VARBINARY          VARBINARY  VARBINARY
      |VARBINARY+VARCHAR            VARBINARY  VARBINARY
      |VARBINARY+NULL               VARBINARY  VARBINARY
      |VARCHAR+VARCHAR              VARCHAR    VARCHAR
      |VARCHAR+NULL                 VARCHAR    VARCHAR
      |NULL+NULL                    NULL       INT
      |INT+BIGINT+DOUBLE            DOUBLE     DOUBLE
      |INT+BIGINT+BOOLEAN           BIGINT     BIGINT
      |INT+BIGINT+DATE              REFUSED    FAILS
      |INT+BIGINT+TIMESTAMP         REFUSED    FAILS
      |INT+BIGINT+TIME              VARCHAR    FAILS
      |INT+BIGINT+VARBINARY         VARCHAR    FAILS
      |INT+BIGINT+VARCHAR           VARCHAR    VARCHAR
      |INT+BIGINT+NULL              BIGINT     BIGINT
      |INT+DOUBLE+BOOLEAN           DOUBLE     DOUBLE
      |INT+DOUBLE+DATE              REFUSED    FAILS
      |INT+DOUBLE+TIMESTAMP         REFUSED    FAILS
      |INT+DOUBLE+TIME              VARCHAR    FAILS
      |INT+DOUBLE+VARBINARY         VARCHAR    FAILS
      |INT+DOUBLE+VARCHAR           VARCHAR    VARCHAR
      |INT+DOUBLE+NULL              DOUBLE     DOUBLE
      |INT+BOOLEAN+DATE             REFUSED    FAILS
      |INT+BOOLEAN+TIMESTAMP        REFUSED    FAILS
      |INT+BOOLEAN+TIME             VARCHAR    FAILS
      |INT+BOOLEAN+VARBINARY        VARCHAR    FAILS
      |INT+BOOLEAN+VARCHAR          VARCHAR    VARCHAR
      |INT+BOOLEAN+NULL             INT        INT
      |INT+DATE+TIMESTAMP           REFUSED    FAILS
      |INT+DATE+TIME                REFUSED    FAILS
      |INT+DATE+VARBINARY           VARCHAR    FAILS
      |INT+DATE+VARCHAR             VARCHAR    VARCHAR
      |INT+DATE+NULL                REFUSED    FAILS
      |INT+TIMESTAMP+TIME           REFUSED    FAILS
      |INT+TIMESTAMP+VARBINARY      VARCHAR    FAILS
      |INT+TIMESTAMP+VARCHAR        VARCHAR    VARCHAR
      |INT+TIMESTAMP+NULL           REFUSED    FAILS
      |INT+TIME+VARBINARY           VARCHAR    FAILS
      |INT+TIME+VARCHAR             VARCHAR    VARCHAR
      |INT+TIME+NULL                VARCHAR    FAILS
      |INT+VARBINARY+VARCHAR        VARCHAR    FAILS
      |INT+VARBINARY+NULL           VARCHAR    FAILS
      |INT+VARCHAR+NULL             VARCHAR    VARCHAR
      |BIGINT+DOUBLE+BOOLEAN        DOUBLE     DOUBLE
      |BIGINT+DOUBLE+DATE           REFUSED    FAILS
      |BIGINT+DOUBLE+TIMESTAMP      REFUSED    FAILS
      |BIGINT+DOUBLE+TIME           VARCHAR    FAILS
      |BIGINT+DOUBLE+VARBINARY      VARCHAR    FAILS
      |BIGINT+DOUBLE+VARCHAR        VARCHAR    VARCHAR
      |BIGINT+DOUBLE+NULL           DOUBLE     DOUBLE
      |BIGINT+BOOLEAN+DATE          REFUSED    FAILS
      |BIGINT+BOOLEAN+TIMESTAMP     REFUSED    FAILS
      |BIGINT+BOOLEAN+TIME          VARCHAR    FAILS
      |BIGINT+BOOLEAN+VARBINARY     VARCHAR    FAILS
      |BIGINT+BOOLEAN+VARCHAR       VARCHAR    VARCHAR
      |BIGINT+BOOLEAN+NULL          BIGINT     BIGINT
      |BIGINT+DATE+TIMESTAMP        REFUSED    FAILS
      |BIGINT+DATE+TIME             REFUSED    FAILS
      |BIGINT+DATE+VARBINARY        VARCHAR    FAILS
      |BIGINT+DATE+VARCHAR          VARCHAR    VARCHAR
      |BIGINT+DATE+NULL             REFUSED    FAILS
      |BIGINT+TIMESTAMP+TIME        REFUSED    FAILS
      |BIGINT+TIMESTAMP+VARBINARY   VARCHAR    FAILS
      |BIGINT+TIMESTAMP+VARCHAR     VARCHAR    VARCHAR
      |BIGINT+TIMESTAMP+NULL        REFUSED    FAILS
      |BIGINT+TIME+VARBINARY        VARCHAR    FAILS
      |BIGINT+TIME+VARCHAR          VARCHAR    VARCHAR
      |BIGINT+TIME+NULL             VARCHAR    FAILS
      |BIGINT+VARBINARY+VARCHAR     VARCHAR    FAILS
      |BIGINT+VARBINARY+NULL        VARCHAR    FAILS
      |BIGINT+VARCHAR+NULL          VARCHAR    VARCHAR
      |DOUBLE+BOOLEAN+DATE          REFUSED    FAILS
      |DOUBLE+BOOLEAN+TIMESTAMP     REFUSED    FAILS
      |DOUBLE+BOOLEAN+TIME          VARCHAR    FAILS
      |DOUBLE+BOOLEAN+VARBINARY     VARCHAR    FAILS
      |DOUBLE+BOOLEAN+VARCHAR       VARCHAR    VARCHAR
      |DOUBLE+BOOLEAN+NULL          DOUBLE     DOUBLE
      |DOUBLE+DATE+TIMESTAMP        REFUSED    FAILS
      |DOUBLE+DATE+TIME             REFUSED    FAILS
      |DOUBLE+DATE+VARBINARY        VARCHAR    FAILS
      |DOUBLE+DATE+VARCHAR          VARCHAR    VARCHAR
      |DOUBLE+DATE+NULL             REFUSED    FAILS
      |DOUBLE+TIMESTAMP+TIME        REFUSED    FAILS
      |DOUBLE+TIMESTAMP+VARBINARY   VARCHAR    FAILS
      |DOUBLE+TIMESTAMP+VARCHAR     VARCHAR    VARCHAR
      |DOUBLE+TIMESTAMP+NULL        REFUSED    FAILS
      |DOUBLE+TIME+VARBINARY        VARCHAR    FAILS
      |DOUBLE+TIME+VARCHAR          VARCHAR    VARCHAR
      |DOUBLE+TIME+NULL             VARCHAR    FAILS
      |DOUBLE+VARBINARY+VARCHAR     VARCHAR    FAILS
      |DOUBLE+VARBINARY+NULL        VARCHAR    FAILS
      |DOUBLE+VARCHAR+NULL          VARCHAR    VARCHAR
      |BOOLEAN+DATE+TIMESTAMP       REFUSED    FAILS
      |BOOLEAN+DATE+TIME            REFUSED    FAILS
      |BOOLEAN+DATE+VARBINARY       VARCHAR    FAILS
      |BOOLEAN+DATE+VARCHAR         VARCHAR    VARCHAR
      |BOOLEAN+DATE+NULL            REFUSED    FAILS
      |BOOLEAN+TIMESTAMP+TIME       REFUSED    FAILS
      |BOOLEAN+TIMESTAMP+VARBINARY  VARCHAR    FAILS
      |BOOLEAN+TIMESTAMP+VARCHAR    VARCHAR    VARCHAR
      |BOOLEAN+TIMESTAMP+NULL       REFUSED    FAILS
      |BOOLEAN+TIME+VARBINARY       VARCHAR    FAILS
      |BOOLEAN+TIME+VARCHAR         VARCHAR    VARCHAR
      |BOOLEAN+TIME+NULL            VARCHAR    FAILS
      |BOOLEAN+VARBINARY+VARCHAR    VARCHAR    FAILS
      |BOOLEAN+VARBINARY+NULL       VARCHAR    FAILS
      |BOOLEAN+VARCHAR+NULL         VARCHAR    VARCHAR
      |DATE+TIMESTAMP+TIME          VARCHAR    FAILS
      |DATE+TIMESTAMP+VARBINARY     VARCHAR    FAILS
      |DATE+TIMESTAMP+VARCHAR       VARCHAR    VARCHAR
      |DATE+TIMESTAMP+NULL          TIMESTAMP  TIMESTAMP
      |DATE+TIME+VARBINARY          VARCHAR    FAILS
      |DATE+TIME+VARCHAR            VARCHAR    VARCHAR
      |DATE+TIME+NULL               VARCHAR    FAILS
      |DATE+VARBINARY+VARCHAR       VARCHAR    FAILS
      |DATE+VARBINARY+NULL          VARCHAR    FAILS
      |DATE+VARCHAR+NULL            VARCHAR    VARCHAR
      |TIMESTAMP+TIME+VARBINARY     VARCHAR    FAILS
      |TIMESTAMP+TIME+VARCHAR       VARCHAR    VARCHAR
      |TIMESTAMP+TIME+NULL          VARCHAR    FAILS
      |TIMESTAMP+VARBINARY+VARCHAR  VARCHAR    FAILS
      |TIMESTAMP+VARBINARY+NULL     VARCHAR    FAILS
      |TIMESTAMP+VARCHAR+NULL       VARCHAR    VARCHAR
      |TIME+VARBINARY+VARCHAR       VARCHAR    FAILS
      |TIME+VARBINARY+NULL          VARCHAR    FAILS
      |TIME+VARCHAR+NULL            VARCHAR    VARCHAR
      |VARBINARY+VARCHAR+NULL       VARBINARY  VARBINARY
      |""".stripMargin.linesIterator
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { line =>
        val Array(set, rule, duckdb) = line.split("\\s+")
        set.split("\\+").toSet -> (rule -> duckdb)
      }
      .toMap

  private def rule(set: Set[String]): String =
    table.getOrElse(set, fail(s"no row for ${set.mkString("+")}"))._1

  /** The rule's answer for a statement: one node for a run of one union kind; binary nodes, left to
    * right, for `INTERSECT` and `EXCEPT`.
    */
  private def expected(operator: String, items: Seq[String]): String =
    if (operator.startsWith("UNION")) rule(items.toSet)
    else
      items.tail.foldLeft(items.head) { (acc, item) =>
        if (acc == "REFUSED") acc else rule(Set(acc, item))
      }

  private def name(t: SQLType): String =
    t match {
      case SQLTypes.Int       => "INT"
      case SQLTypes.BigInt    => "BIGINT"
      case SQLTypes.Double    => "DOUBLE"
      case SQLTypes.Boolean   => "BOOLEAN"
      case SQLTypes.Date      => "DATE"
      case SQLTypes.Timestamp => "TIMESTAMP"
      case SQLTypes.Time      => "TIME"
      case SQLTypes.VarBinary => "VARBINARY"
      case SQLTypes.Varchar   => "VARCHAR"
      case SQLTypes.Null      => "NULL"
      case other              => other.typeId
    }

  /** The type the code answers at `depth`, or REFUSED. */
  private def answered(sql: String, depth: String): Either[String, String] =
    Parser(sql) match {
      case Right(m: MultiSearch) =>
        MultiSearch.columnTypes(m.requests.map(_.update(Some(schema))), m.resolvedOperators) match {
          case Left(refusal) => Left(refusal)
          case Right(columns) =>
            val result: SetOperationType = columns.head.result
            def inner(t: SetOperationType): Option[SQLType] =
              depth match {
                case "column"  => Some(t.sqlType)
                case "field"   => t.fields.find(_.name == "a").map(_.sqlType)
                case "element" => t.element.map(_.sqlType)
                case _ =>
                  t.fields.find(_.name == "p").flatMap(_.fields.find(_.name == "c")).map(_.sqlType)
              }
            Right(
              if (result.sqlType == SQLTypes.Null) "NULL"
              else inner(result).fold(s"? ${result}")(name)
            )
        }
      case other => fail(s"[$sql] expected a set operation, got $other")
    }

  "The table" should "equal DuckDB wherever DuckDB answers, refuse or say VARCHAR elsewhere" in {
    // rule 1: a date beside a number or a BOOLEAN, with neither text nor a VARBINARY
    def ruleOne(set: Set[String]): Boolean =
      !set("VARCHAR") && !set("VARBINARY") && (set("DATE") || set("TIMESTAMP")) &&
      Seq("INT", "BIGINT", "DOUBLE", "BOOLEAN").exists(set)
    // where DuckDB gives no type and rule 1 does not refuse: VARCHAR (rule 2), listed here
    val ruleTwo: Set[Set[String]] = table.collect { case (set, ("VARCHAR", "FAILS")) =>
      set
    }.toSet
    val wrong = table.toSeq.flatMap { case (set, (r, duckdb)) =>
      val key = set.toSeq.sorted.mkString("+")
      if (duckdb == "FAILS") {
        val want = if (ruleOne(set)) "REFUSED" else "VARCHAR"
        if (r == want) None else Some(s"$key: the rule answers $r where DuckDB fails, not $want")
      } else if (r == duckdb) None
      // a NULL-only node: DuckDB types its NULL literals INTEGER, core NULL
      else if (set == Set("NULL") && duckdb == "INT") None
      else Some(s"$key: the rule answers $r, DuckDB $duckdb")
    }
    wrong shouldBe empty
    table should have size 175
    ruleTwo.size shouldBe 61
    ruleTwo.forall(set => !ruleOne(set)) shouldBe true
  }

  "Every set operation" should "be typed by the table at every depth, order and operator" in {
    val sets = (types :+ "NULL").map(t => Seq(t, t)) ++
      (types :+ "NULL").combinations(2).flatMap(_.permutations) ++
      (types :+ "NULL").combinations(3).flatMap(_.permutations)
    val statements = for {
      (depth, prefix) <- depths
      items           <- sets
      operator        <- Seq("UNION ALL", "UNION", "INTERSECT", "EXCEPT")
    } yield (
      depth,
      operator,
      items,
      items
        .map(i => s"SELECT ${if (i == "NULL") "NULL" else s"${prefix}_$i"} AS k FROM t")
        .mkString(s" $operator ")
    )
    statements.size shouldBe 13120
    val wrong = statements.flatMap { case (depth, operator, items, sql) =>
      val want = expected(operator, items)
      (answered(sql, depth), want) match {
        case (Left(refusal), "REFUSED") =>
          // a field is named by its path; a list's element by its lists
          val named = depth match {
            case "field"   => refusal.contains("field 'a' is")
            case "nested"  => refusal.contains("field 'p.c' is")
            case "element" => refusal.contains("is ARRAY<")
            case _         => !refusal.contains("field '")
          }
          if (named) None else Some(s"[$depth] [$sql] refused as $refusal")
        case (Right(got), w) if got == w => None
        case (got, w)                    => Some(s"[$depth] [$sql] answered $got instead of $w")
      }
    }
    wrong shouldBe empty
  }

  /** The table's rule 2: where DuckDB 1.5.5 gives a node NO type and the rule answers `VARCHAR`. */
  private def untyped(set: Set[String]): Boolean = table.get(set).contains("VARCHAR" -> "FAILS")

  /** What differs between a node of [[MultiSearch.columnTree]] and the table, for a node joining
    * `items` by `operator` -- with the type the rule gives it. A run of one union kind is ONE node
    * over every item; `INTERSECT` and `EXCEPT` nodes are binary, left to right.
    */
  private def nodeErrors(
    node: MultiSearch.SetOperationNode,
    operator: String,
    items: Seq[String]
  ): (Seq[String], String) = {
    val (operands, set, subtree) =
      if (operator.startsWith("UNION") || items.size == 2)
        (items.indices.map(Left(_)), items.toSet, Nil)
      else
        node.operands.headOption match {
          case Some(Right(sub)) =>
            val (errors, before) = nodeErrors(sub, operator, items.init)
            (Seq(Right(sub), Left(items.size - 1)), Set(before, items.last), errors)
          case other => (Nil, items.toSet, Seq(s"a binary node over ${items.size} items is $other"))
        }
    val errors = subtree ++
      (if (node.operator.sql == operator) Nil else Seq(s"operator ${node.operator.sql}")) ++
      (if (node.operands == operands) Nil else Seq(s"operands ${node.operands}")) ++
      (if (node.untyped == Seq(untyped(set))) Nil
       else Seq(s"${set.mkString("+")} untyped ${node.untyped}"))
    (errors, rule(set))
  }

  "Every node of a set operation" should "be typed as columnTypes types it, untyped where DuckDB gives none" in {
    val sets = (types :+ "NULL").map(t => Seq(t, t)) ++
      (types :+ "NULL").combinations(2).flatMap(_.permutations) ++
      (types :+ "NULL").combinations(3).flatMap(_.permutations)
    val statements = for {
      (depth, prefix) <- depths
      items           <- sets
      operator        <- Seq("UNION ALL", "UNION", "INTERSECT", "EXCEPT")
    } yield (
      depth,
      operator,
      items,
      items
        .map(i => s"SELECT ${if (i == "NULL") "NULL" else s"${prefix}_$i"} AS k FROM t")
        .mkString(s" $operator ")
    )
    statements.size shouldBe 13120
    var nodes = 0
    var untypedNodes = 0
    val wrong = statements.flatMap { case (depth, operator, items, sql) =>
      Parser(sql) match {
        case Right(m: MultiSearch) =>
          val requests = m.requests.map(_.update(Some(schema)))
          (
            MultiSearch.columnTree(requests, m.resolvedOperators),
            MultiSearch.columnTypes(requests, m.resolvedOperators)
          ) match {
            case (Left(a), Left(b)) if a == b => Nil
            case (Right(root), Right(columns)) =>
              def count(n: MultiSearch.SetOperationNode): Unit = {
                nodes += 1
                if (n.untyped.exists(identity)) untypedNodes += 1
                n.operands.foreach(_.foreach(count))
              }
              count(root)
              (if (root.columns == columns.map(_.result)) Nil
               else Seq(s"root ${root.columns} instead of ${columns.map(_.result)}")) ++
              nodeErrors(root, operator, items)._1
                .map(e => s"[$depth] [$sql] $e")
            case (tree, types) => Seq(s"[$depth] [$sql] tree $tree, types $types")
          }
        case other => fail(s"[$sql] expected a set operation, got $other")
      }
    }
    wrong shouldBe empty
    // every node of every statement the rule answers, and those DuckDB gives no type -- counted
    // from the table alone
    (nodes, untypedNodes) shouldBe ((14016, 5136))
  }
}
