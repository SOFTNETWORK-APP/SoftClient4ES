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

import app.softnetwork.elastic.sql.operator.{
  EXCEPT,
  INTERSECT,
  INTERSECT_ALL,
  SetOperator,
  UNION,
  UNION_DISTINCT
}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch.{
  SetOperationNode,
  SetOperationType,
  StructField
}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A set operation's NODES, as DuckDB 1.5.5 types them ([[MultiSearch.columnTree]]): each node's
  * operator and operands, the type it gives each column, and where DuckDB gives it NO type -- the
  * nodes the relational engine must cast itself. Every expectation below is DuckDB 1.5.5's, RUN
  * over `n = 5`, `x = 5.0`, `s = 'a'`.
  */
class SetOperationColumnTreeSpec extends AnyFlatSpec with Matchers {

  private val schema = Table(
    "t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("bg", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("s", SQLTypes.Keyword),
      Column("bin", SQLTypes.VarBinary),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("t1", SQLTypes.Time),
      Column("t2", SQLTypes.Time),
      Column("oi", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Int))),
      Column(
        "ot",
        SQLTypes.Struct,
        multiFields = List(Column("a", SQLTypes.Time), Column("c", SQLTypes.Keyword))
      ),
      Column("ol", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.BigInt))),
      Column("si", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.Int))),
      Column("st", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.Time))),
      Column("sl", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.BigInt))),
      Column("ai", SQLTypes.Array(SQLTypes.Int)),
      Column("atm", SQLTypes.Array(SQLTypes.Time)),
      Column("geo", SQLTypes.GeoPoint)
    )
  )

  /** A field `name` that is an object with one field `c` of type `c`. */
  private def inner(name: String, c: SQLType): Column =
    Column(name, SQLTypes.Struct, multiFields = List(Column("c", c)))

  private def resolved(sql: String): (Seq[SingleSearch], Seq[SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) => (m.requests.map(_.update(Some(schema))), m.resolvedOperators)
      case other                 => fail(s"[$sql] expected a set operation, got $other")
    }

  private def tree(sql: String): SetOperationNode = {
    val (requests, operators) = resolved(sql)
    MultiSearch.columnTree(requests, operators) match {
      case Right(root)   => root
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  private def select(item: String): String = s"SELECT $item AS k FROM t"

  private def scalar(t: SQLType): SetOperationType = SetOperationType(t)

  private def node(
    operator: SetOperator,
    operands: Seq[Either[Int, SetOperationNode]],
    column: SetOperationType,
    untyped: Boolean
  ): SetOperationNode = SetOperationNode(operator, operands, Seq(column), Seq(untyped))

  "A set operation's tree" should "type each node as DuckDB does: the contract's six cases" in {
    // (n INTERSECT x) UNION s: INTERSECT is DOUBLE, then UNION VARCHAR -- typed: DuckDB answers
    // {'5.0', 'a'}; casting n and x to VARCHAR first would answer {'a'}
    tree(s"${select("n")} INTERSECT ${select("x")} UNION ${select("s")}") shouldBe node(
      UNION_DISTINCT,
      Seq(
        Right(node(INTERSECT, Seq(Left(0), Left(1)), scalar(SQLTypes.Double), untyped = false)),
        Left(2)
      ),
      scalar(SQLTypes.Varchar),
      untyped = false
    )
    // n UNION x UNION ALL s: a run of UNION, then UNION ALL over it -- DuckDB answers
    // {'5.0', 'a'}; casting first would answer {'5', '5.0', 'a'}
    tree(s"${select("n")} UNION ${select("x")} UNION ALL ${select("s")}") shouldBe node(
      UNION,
      Seq(
        Right(
          node(UNION_DISTINCT, Seq(Left(0), Left(1)), scalar(SQLTypes.Double), untyped = false)
        ),
        Left(2)
      ),
      scalar(SQLTypes.Varchar),
      untyped = false
    )
    // bin UNION d UNION n: one run DuckDB gives no type ("Unimplemented type for cast (DATE ->
    // BLOB)"): VARCHAR, untyped
    tree(s"${select("bin")} UNION ${select("d")} UNION ${select("n")}") shouldBe node(
      UNION_DISTINCT,
      Seq(Left(0), Left(1), Left(2)),
      scalar(SQLTypes.Varchar),
      untyped = true
    )
    // CAST(ts AS TIME) UNION d: a TIME beside a DATE, untyped
    tree(s"${select("CAST(ts AS TIME)")} UNION ${select("d")}") shouldBe node(
      UNION_DISTINCT,
      Seq(Left(0), Left(1)),
      scalar(SQLTypes.Varchar),
      untyped = true
    )
    // (t1 INTERSECT t2) UNION ALL bin: INTERSECT typed TIME, then UNION ALL untyped
    tree(s"${select("t1")} INTERSECT ${select("t2")} UNION ALL ${select("bin")}") shouldBe node(
      UNION,
      Seq(
        Right(node(INTERSECT, Seq(Left(0), Left(1)), scalar(SQLTypes.Time), untyped = false)),
        Left(2)
      ),
      scalar(SQLTypes.Varchar),
      untyped = true
    )
    // two structs whose field `a` is an INT and a TIME: `a` is VARCHAR, the column untyped, the
    // struct merged in DuckDB's order
    tree(s"${select("oi")} UNION ${select("ot")}") shouldBe node(
      UNION_DISTINCT,
      Seq(Left(0), Left(1)),
      SetOperationType(
        SQLTypes.Struct,
        Seq(StructField("a", SQLTypes.Varchar), StructField("c", SQLTypes.Varchar))
      ),
      untyped = true
    )
  }

  it should "say where DuckDB gives no type at any depth: a field inside a field, an element" in {
    // p.c an INT beside a TIME: untyped at the column
    tree(s"${select("si")} UNION ALL ${select("st")}") shouldBe node(
      UNION,
      Seq(Left(0), Left(1)),
      SetOperationType(
        SQLTypes.Struct,
        Seq(
          StructField(
            "p",
            SetOperationType(SQLTypes.Struct, Seq(StructField("c", SQLTypes.Varchar)))
          )
        )
      ),
      untyped = true
    )
    // p.c an INT beside a BIGINT: typed
    tree(s"${select("si")} UNION ALL ${select("sl")}") shouldBe node(
      UNION,
      Seq(Left(0), Left(1)),
      SetOperationType(
        SQLTypes.Struct,
        Seq(
          StructField(
            "p",
            SetOperationType(SQLTypes.Struct, Seq(StructField("c", SQLTypes.BigInt)))
          )
        )
      ),
      untyped = false
    )
    tree(s"${select("oi")} INTERSECT ALL ${select("ol")}").untyped shouldBe Seq(false)
    // lists: their elements
    val lists = tree(s"${select("ai")} EXCEPT ${select("atm")}")
    lists.operator shouldBe EXCEPT
    lists.columns shouldBe Seq(
      SetOperationType(SQLTypes.Array(SQLTypes.Varchar), element = Some(scalar(SQLTypes.Varchar)))
    )
    lists.untyped shouldBe Seq(true)
    // a text beside a VARBINARY and a number: DuckDB makes a BLOB it cannot cast the number to
    tree(s"${select("s")} UNION ${select("bin")} UNION ${select("n")}").untyped shouldBe Seq(true)
    // a text beside a VARBINARY only: a BLOB, typed
    val blob = tree(s"${select("s")} UNION ${select("bin")}")
    blob.columns shouldBe Seq(scalar(SQLTypes.VarBinary))
    blob.untyped shouldBe Seq(false)
    // points alone stay a point
    tree(s"${select("geo")} UNION ${select("geo")}").columns shouldBe Seq(SetOperationType.GeoPoint)
  }

  it should "type each column of a node, the root's being columnTypes' results" in {
    val sql =
      "SELECT n, bin, d AS e FROM t UNION SELECT x, d, ts FROM t INTERSECT SELECT bg, d, d FROM t"
    val (requests, operators) = resolved(sql)
    val root = tree(sql)
    root.operator shouldBe UNION_DISTINCT
    root.columns shouldBe Seq(
      scalar(SQLTypes.Double),
      scalar(SQLTypes.Varchar),
      scalar(SQLTypes.Timestamp)
    )
    root.untyped shouldBe Seq(false, true, false)
    MultiSearch.columnTypes(requests, operators).map(_.map(_.result)) shouldBe Right(root.columns)
    // the INTERSECT binds first: its own types
    root.operands.head shouldBe Left(0)
    root.operands(1) match {
      case Right(intersect) =>
        intersect.operator shouldBe INTERSECT
        intersect.operands shouldBe Seq(Left(1), Left(2))
        intersect.columns shouldBe Seq(
          scalar(SQLTypes.Double),
          scalar(SQLTypes.Date),
          scalar(SQLTypes.Timestamp)
        )
        intersect.untyped shouldBe Seq(false, false, false)
      case other => fail(s"not a node: $other")
    }
    // a date field's mapping format is the branch's, never a node's
    def formats(t: SetOperationType): Seq[String] =
      t.format.toSeq ++ t.fields.flatMap(f => formats(f.valueType)) ++
      t.element.toSeq.flatMap(formats)
    root.columns.flatMap(formats) shouldBe empty
  }

  it should "keep a SELECT * branch as an operand, and type the others" in {
    val root = tree(s"${select("n")} UNION ALL SELECT * FROM t UNION ALL ${select("x")}")
    root.operands shouldBe Seq(Left(0), Left(1), Left(2))
    root.columns shouldBe Seq(scalar(SQLTypes.Double))
    // a node under which no branch declares a column: its type is not known
    val star = tree(s"SELECT * FROM t INTERSECT SELECT * FROM t UNION ALL ${select("n")}")
    star.operands.head match {
      case Right(intersect) => intersect.columns shouldBe Seq(scalar(SQLTypes.Any))
      case other            => fail(s"not a node: $other")
    }
    star.columns shouldBe Seq(scalar(SQLTypes.Int))
  }

  it should "refuse what branchTypes refuses, in its words" in {
    Seq(
      s"${select("d")} UNION ALL ${select("n")}",
      s"${select("n")} EXCEPT ${select("d")} UNION ALL ${select("s")}",
      s"${select("oi")} UNION ${select("n")}"
    ).foreach { sql =>
      val (requests, operators) = resolved(sql)
      val refusal = MultiSearch.branchTypes(requests, operators)
      refusal.isLeft shouldBe true
      MultiSearch.columnTree(requests, operators) shouldBe refusal
    }
    val (requests, _) = resolved(s"${select("n")} UNION ALL ${select("x")}")
    MultiSearch.columnTree(requests, Seq(UNION, UNION)) shouldBe
    Left("A set operation over 2 branches needs 1 operators, got 2")
    MultiSearch.columnTree(requests.take(1), Nil) shouldBe
    Left("A set operation needs two branches at least, got 1")
    // no branch declares a column: branchTypes checks nothing, and neither does the tree
    val (stars, _) = resolved("SELECT * FROM t UNION ALL SELECT * FROM t UNION ALL SELECT * FROM t")
    Seq(Nil, Seq(UNION, UNION), Seq(INTERSECT), Seq(UNION, UNION, UNION)).foreach { ops =>
      MultiSearch.branchTypes(stars, ops) shouldBe Right(())
      MultiSearch.columnTree(stars, ops).map(_.columns) shouldBe Right(Nil)
    }
    // an operator list too short is read as UNION ALL past its end: INTERSECT binds first
    MultiSearch.columnTree(stars, Seq(INTERSECT)).map(_.operands) shouldBe
    Right(Seq(Right(SetOperationNode(INTERSECT, Seq(Left(0), Left(1)), Nil, Nil)), Left(2)))
    MultiSearch.columnTree(requests, Nil).map(_.operator) shouldBe Right(UNION)
    MultiSearch.columnTree(requests, Seq(INTERSECT_ALL)).map(_.operator) shouldBe
    Right(INTERSECT_ALL)
  }
}
