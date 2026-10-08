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
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A set operation's column types follow the lead's rules of 2026-10-07, once each branch's schema
  * is attached: the type DuckDB 1.5.5 gives the column where it gives one -- the relational engine
  * that runs `UNION`, `INTERSECT` and `EXCEPT` is DuckDB -- `VARCHAR` where it gives none, and a
  * refusal ONLY for a date beside a number, the one pair the relational engine fails on when the
  * statement runs.
  *
  * Every DuckDB type below was MEASURED on DuckDB 1.5.5 over the same columns (`id`, `s` and `txt`
  * text, `n` INTEGER, `x` DOUBLE, `f` FLOAT, `bg` BIGINT, `sm` SMALLINT, `d` / `d2` DATE, `ts`
  * TIMESTAMP, `b` BOOLEAN), statement by statement; every verdict on a literal-free statement was
  * MEASURED through arrow (its runtime matrix: 1,404 statements). PostgreSQL 16 refuses most of the
  * text pairs (`UNION types text and integer cannot be matched`); DuckDB comes first.
  */
class SetOperationTypesSpec extends AnyFlatSpec with Matchers {

  private val schema = Table(
    "t",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("s", SQLTypes.Keyword),
      Column("txt", SQLTypes.Text),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("f", SQLTypes.Real),
      Column("bg", SQLTypes.BigInt),
      Column("sm", SQLTypes.SmallInt),
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("b", SQLTypes.Boolean),
      Column("geo", SQLTypes.GeoPoint),
      Column("bin", SQLTypes.VarBinary),
      Column("tags", SQLTypes.Array(SQLTypes.Keyword)),
      Column("arr", SQLTypes.Array(SQLTypes.Int)),
      Column("arrb", SQLTypes.Array(SQLTypes.BigInt)),
      Column("arrd", SQLTypes.Array(SQLTypes.Date)),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(Column("a", SQLTypes.Int), Column("b", SQLTypes.Keyword))
      ),
      Column(
        "o2",
        SQLTypes.Struct,
        multiFields = List(Column("b", SQLTypes.Keyword), Column("c", SQLTypes.Boolean))
      ),
      Column("oa", SQLTypes.Struct, multiFields = List(Column("A", SQLTypes.Keyword))),
      Column("oad", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Date))),
      Column(
        "olat",
        SQLTypes.Struct,
        multiFields = List(Column("lat", SQLTypes.Keyword), Column("z", SQLTypes.Int))
      ),
      Column(
        "onest",
        SQLTypes.Struct,
        multiFields = List(Column("p", SQLTypes.GeoPoint), Column("q", SQLTypes.Int))
      ),
      Column("geo2", SQLTypes.GeoPoint),
      // one field `a` of several types, and lists of three of them
      Column("oi", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Int))),
      Column("od", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Date))),
      Column("ov", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Keyword))),
      Column("ob", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Boolean))),
      Column("obl", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.VarBinary))),
      Column("ai", SQLTypes.Array(SQLTypes.Int)),
      Column("ad", SQLTypes.Array(SQLTypes.Date)),
      Column("av", SQLTypes.Array(SQLTypes.Keyword)),
      // a field `p`: an object whose field `c` has one of three types, an INT, or a KEYWORD
      Column("si", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.Int))),
      Column("sd", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.Date))),
      Column("sv", SQLTypes.Struct, multiFields = List(inner("p", SQLTypes.Keyword))),
      Column("pn", SQLTypes.Struct, multiFields = List(Column("p", SQLTypes.Int))),
      Column("pv", SQLTypes.Struct, multiFields = List(Column("p", SQLTypes.Keyword)))
    )
  )

  /** A field `name` that is an object with one field `c` of type `c`. */
  private def inner(name: String, c: SQLType): Column =
    Column(name, SQLTypes.Struct, multiFields = List(Column("c", c)))

  private def branches(sql: String, withSchema: Boolean = true): Seq[SingleSearch] =
    Parser(sql) match {
      case Right(m: MultiSearch) =>
        if (withSchema) m.requests.map(_.update(Some(schema))) else m.requests
      case other => fail(s"[$sql] expected a set operation, got $other")
    }

  private def columns(sql: String): Either[String, Seq[MultiSearch.SetOperationColumn]] =
    MultiSearch.columnTypes(branches(sql))

  private def typeOf(sql: String): Either[String, SQLType] =
    columns(sql).map(_.head.resultType)

  /** `SELECT <a> AS k FROM t UNION ALL SELECT <b> AS k FROM t ...` */
  private def union(items: String*): String =
    items.map(i => s"SELECT $i AS k FROM t").mkString(" UNION ALL ")

  "A set operation's column" should "take the type DuckDB 1.5.5 gives it, every pair" in {
    val expected: Seq[(String, SQLType)] = Seq(
      // the brief's pairs
      union("d", "CURRENT_DATE")      -> SQLTypes.Date,
      union("n", "'1'")               -> SQLTypes.Varchar,
      union("d", "'2024-01-31'")      -> SQLTypes.Varchar,
      union("id", "n")                -> SQLTypes.Varchar,
      union("1", "'a'")               -> SQLTypes.Varchar,
      union("d", "ts")                -> SQLTypes.Timestamp,
      union("ts", "CAST(d AS DATE)")  -> SQLTypes.Timestamp,
      union("d", "d2")                -> SQLTypes.Date,
      union("n", "x")                 -> SQLTypes.Double,
      union("n", "CAST(n AS BIGINT)") -> SQLTypes.BigInt,
      union("n", "2")                 -> SQLTypes.Int,
      union("s", "id")                -> SQLTypes.Varchar,
      // the same pairs the other way round
      union("CURRENT_DATE", "d")      -> SQLTypes.Date,
      union("'1'", "n")               -> SQLTypes.Varchar,
      union("'2024-01-31'", "d")      -> SQLTypes.Varchar,
      union("n", "id")                -> SQLTypes.Varchar,
      union("'a'", "1")               -> SQLTypes.Varchar,
      union("ts", "d")                -> SQLTypes.Timestamp,
      union("x", "n")                 -> SQLTypes.Double,
      union("CAST(n AS BIGINT)", "n") -> SQLTypes.BigInt,
      union("2", "n")                 -> SQLTypes.Int,
      // a NULL literal takes the other branch's type
      union("NULL", "n")   -> SQLTypes.Int,
      union("n", "NULL")   -> SQLTypes.Int,
      union("NULL", "d")   -> SQLTypes.Date,
      union("NULL", "'a'") -> SQLTypes.Varchar,
      union("x", "NULL")   -> SQLTypes.Double,
      union("b", "NULL")   -> SQLTypes.Boolean,
      // three branches: the widest, and a text branch makes the column text whatever the others
      union("n", "x", "CAST(n AS BIGINT)") -> SQLTypes.Double,
      union("d", "ts", "d2")               -> SQLTypes.Timestamp,
      union("d", "n", "'1'")               -> SQLTypes.Varchar,
      union("n", "NULL", "x")              -> SQLTypes.Double,
      union("1", "2", "'a'")               -> SQLTypes.Varchar,
      union("s", "n", "d")                 -> SQLTypes.Varchar,
      union("CAST(n AS BIGINT)", "n", "n") -> SQLTypes.BigInt,
      union("d", "d2", "CURRENT_DATE")     -> SQLTypes.Date,
      // a KEYWORD and a TEXT column are both text
      union("id", "txt") -> SQLTypes.Varchar,
      union("txt", "n")  -> SQLTypes.Varchar,
      // a TIMESTAMP, a DOUBLE, a REAL or a TIME beside text is text too, spelled as DuckDB spells
      // it (the lead's ruling of 2026-10-06)
      union("ts", "s")               -> SQLTypes.Varchar,
      union("s", "ts")               -> SQLTypes.Varchar,
      union("ts", "'x'")             -> SQLTypes.Varchar,
      union("x", "s")                -> SQLTypes.Varchar,
      union("'a'", "x")              -> SQLTypes.Varchar,
      union("f", "txt")              -> SQLTypes.Varchar,
      union("2.5", "'a'")            -> SQLTypes.Varchar,
      union("CAST(ts AS TIME)", "s") -> SQLTypes.Varchar,
      union("d", "ts", "s")          -> SQLTypes.Varchar,
      union("x", "ts", "'1'")        -> SQLTypes.Varchar,
      // a TIME beside a TIMESTAMP, with text: VARCHAR (DuckDB)
      union("CAST(ts AS TIME)", "ts", "'x'") -> SQLTypes.Varchar,
      // a VARBINARY beside text is a VARBINARY, DuckDB's BLOB (the lead's ruling of 2026-10-07)
      union("bin", "s")         -> SQLTypes.VarBinary,
      union("s", "bin")         -> SQLTypes.VarBinary,
      union("bin", "'x'")       -> SQLTypes.VarBinary,
      union("bin", "txt", "s")  -> SQLTypes.VarBinary,
      union("bin", "NULL", "s") -> SQLTypes.VarBinary,
      union("bin", "NULL")      -> SQLTypes.VarBinary,
      union("bin", "bin")       -> SQLTypes.VarBinary,
      // a BOOLEAN reads as 1 / 0 beside a number
      union("b", "n")    -> SQLTypes.Int,
      union("n", "b")    -> SQLTypes.Int,
      union("b", "x")    -> SQLTypes.Double,
      union("b", "1")    -> SQLTypes.Int,
      union("TRUE", "n") -> SQLTypes.Int,
      // the numeric lattice
      union("n", "f")   -> SQLTypes.Real,
      union("bg", "f")  -> SQLTypes.Real,
      union("sm", "n")  -> SQLTypes.Int,
      union("bg", "x")  -> SQLTypes.Double,
      union("f", "x")   -> SQLTypes.Double,
      union("sm", "bg") -> SQLTypes.BigInt,
      union("n", "bg")  -> SQLTypes.BigInt,
      // the other set operators: the same rule (the relational engine runs them)
      "SELECT n AS k FROM t INTERSECT SELECT x AS k FROM t"  -> SQLTypes.Double,
      "SELECT id AS k FROM t EXCEPT SELECT n AS k FROM t"    -> SQLTypes.Varchar,
      "SELECT n AS k FROM t UNION SELECT '1' AS k FROM t"    -> SQLTypes.Varchar,
      "SELECT d AS k FROM t INTERSECT SELECT ts AS k FROM t" -> SQLTypes.Timestamp,
      "SELECT b AS k FROM t EXCEPT SELECT n AS k FROM t"     -> SQLTypes.Int
    )
    val wrong = expected.flatMap { case (sql, want) =>
      typeOf(sql) match {
        case Right(got) if got == want => None
        case other                     => Some(s"[$sql] answered $other instead of $want")
      }
    }
    wrong shouldBe empty
  }

  it should "be refused by name where a date meets a number, with no text and no VARBINARY" in {
    // a DATE or a TIMESTAMP beside a BOOLEAN or a number of any width, in one node with neither a
    // text nor a VARBINARY operand: the pair the relational engine fails on (MEASURED through
    // arrow: DuckDB's `Unimplemented type for cast (INTEGER -> DATE)` when a value flows)
    val refused: Seq[(String, String)] = Seq(
      union("d", "n")   -> "column 1: branch 1 'k' is DATE, branch 2 'k' is INT",
      union("n", "d")   -> "column 1: branch 1 'k' is INT, branch 2 'k' is DATE",
      union("b", "d")   -> "column 1: branch 1 'k' is BOOLEAN, branch 2 'k' is DATE",
      union("ts", "n")  -> "column 1: branch 1 'k' is TIMESTAMP, branch 2 'k' is INT",
      union("d", "f")   -> "column 1: branch 1 'k' is DATE, branch 2 'k' is REAL",
      union("sm", "ts") -> "column 1: branch 1 'k' is SMALLINT, branch 2 'k' is TIMESTAMP",
      union("x", "d")   -> "column 1: branch 1 'k' is DOUBLE, branch 2 'k' is DATE",
      union("ts", "bg") -> "column 1: branch 1 'k' is TIMESTAMP, branch 2 'k' is BIGINT",
      // a whole number LITERAL is an INTEGER, as DuckDB reads it
      union("CURRENT_DATE", "1") -> "column 1: branch 1 'k' is DATE, branch 2 'k' is INT",
      // each side is named by the branch that decides its type: the widest number, the TIMESTAMP
      union("b", "n", "d")    -> "column 1: branch 2 'k' is INT, branch 3 'k' is DATE",
      union("d", "ts", "n")   -> "column 1: branch 2 'k' is TIMESTAMP, branch 3 'k' is INT",
      union("NULL", "d", "n") -> "column 1: branch 2 'k' is DATE, branch 3 'k' is INT",
      // a TIME, which DuckDB has no common type for, does not hide the pair
      union("CAST(ts AS TIME)", "d", "n") -> "column 1: branch 2 'k' is DATE, branch 3 'k' is INT",
      "SELECT id, d FROM t UNION ALL SELECT g, n FROM t" ->
      "column 2: branch 1 'd' is DATE, branch 2 'n' is INT",
      "SELECT d AS k FROM t EXCEPT SELECT n AS k FROM t" ->
      "column 1: branch 1 'k' is DATE, branch 2 'k' is INT"
    )
    val wrong = refused.flatMap { case (sql, reason) =>
      MultiSearch.branchTypes(branches(sql)) match {
        case Left(msg)
            if msg == s"Set operation branches must project compatible types at $reason" =>
          None
        case other => Some(s"[$sql] answered $other instead of refusing at $reason")
      }
    }
    wrong shouldBe empty
  }

  it should "answer VARCHAR where DuckDB gives the column no type" in {
    // every value spelled as text; MEASURED through arrow for the VARBINARY nodes (arrow reads a
    // binary field as its base64 text, so a VARBINARY is text to it, the date beside a number
    // included), the lead's rule for the TIME pairs, whose computed branches arrow cannot read
    // yet (issue #418)
    val answered: Seq[(String, SQLType)] = Seq(
      // a VARBINARY beside a number, a temporal or a BOOLEAN: DuckDB has no cast to BLOB
      union("bin", "n")                -> SQLTypes.Varchar,
      union("x", "bin")                -> SQLTypes.Varchar,
      union("bin", "d")                -> SQLTypes.Varchar,
      union("ts", "bin")               -> SQLTypes.Varchar,
      union("b", "bin")                -> SQLTypes.Varchar,
      union("bin", "CAST(ts AS TIME)") -> SQLTypes.Varchar,
      // ... with text in the column too: not a BLOB, text
      union("bin", "s", "d") -> SQLTypes.Varchar,
      union("b", "s", "bin") -> SQLTypes.Varchar,
      // ... and beside a date AND a number, in any order (arrow's T, P2 and Q shapes)
      union("bin", "d", "n")      -> SQLTypes.Varchar,
      union("d", "bin", "n")      -> SQLTypes.Varchar,
      union("n", "d", "bin")      -> SQLTypes.Varchar,
      union("bin", "ts", "x")     -> SQLTypes.Varchar,
      union("x", "ts", "bin")     -> SQLTypes.Varchar,
      union("bin", "d", "b")      -> SQLTypes.Varchar,
      union("b", "d", "bin")      -> SQLTypes.Varchar,
      union("s", "n", "d", "bin") -> SQLTypes.Varchar,
      union("d", "bin", "n", "s") -> SQLTypes.Varchar,
      // a TIME beside a date, a number or a BOOLEAN
      union("CAST(ts AS TIME)", "ts") -> SQLTypes.Varchar,
      union("d", "CAST(ts AS TIME)")  -> SQLTypes.Varchar,
      union("CAST(ts AS TIME)", "n")  -> SQLTypes.Varchar,
      union("x", "CAST(ts AS TIME)")  -> SQLTypes.Varchar,
      union("CAST(ts AS TIME)", "b")  -> SQLTypes.Varchar,
      // a text branch makes the column text even beside a date and a number (DuckDB)
      union("d", "n", "s")    -> SQLTypes.Varchar,
      union("b", "ts", "txt") -> SQLTypes.Varchar
    )
    val wrong = answered.flatMap { case (sql, want) =>
      typeOf(sql) match {
        case Right(got) if got == want => None
        case other                     => Some(s"[$sql] answered $other instead of $want")
      }
    }
    wrong shouldBe empty
  }

  it should "refuse an ARRAY, a STRUCT or a GEO_POINT beside another type, text included" in {
    // MEASURED through arrow and on DuckDB 1.5.5: no date, number, BLOB or ordinary text is cast
    // to a list or a struct (`Unimplemented type for cast (INTEGER -> INTEGER[])`, `Type VARCHAR
    // with value 'a' can't be cast to the destination type STRUCT(lat DOUBLE, lon DOUBLE)`)
    val refused: Seq[(String, String)] = Seq(
      union("geo", "s")    -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is KEYWORD",
      union("geo", "n")    -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is INT",
      union("d", "geo")    -> "column 1: branch 1 'k' is DATE, branch 2 'k' is GEO_POINT",
      union("geo", "bin")  -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is VARBINARY",
      union("tags", "n")   -> "column 1: branch 1 'k' is ARRAY<KEYWORD>, branch 2 'k' is INT",
      union("s", "arr")    -> "column 1: branch 1 'k' is KEYWORD, branch 2 'k' is ARRAY<INT>",
      union("obj", "txt")  -> "column 1: branch 1 'k' is STRUCT, branch 2 'k' is TEXT",
      union("tags", "geo") -> "column 1: branch 1 'k' is ARRAY<KEYWORD>, branch 2 'k' is GEO_POINT",
      // two structs whose fields DuckDB cannot merge: an INTEGER and a DATE (MEASURED), named
      union("obj", "oad") ->
      "column 1: branch 1 'k' field 'a' is INT, branch 2 'k' field 'a' is DATE",
      union("geo", "tags") -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is ARRAY<KEYWORD>",
      // the nested type is refused before text can make the node text
      union("geo", "s", "n") -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is KEYWORD",
      // two lists whose elements DuckDB cannot combine (`Unimplemented type for cast (INTEGER ->
      // DATE)`, MEASURED)
      union("arr", "arrd") -> "column 1: branch 1 'k' is ARRAY<INT>, branch 2 'k' is ARRAY<DATE>"
    )
    val wrong = refused.flatMap { case (sql, reason) =>
      MultiSearch.branchTypes(branches(sql)) match {
        case Left(msg)
            if msg == s"Set operation branches must project compatible types at $reason" =>
          None
        case other => Some(s"[$sql] answered $other instead of refusing at $reason")
      }
    }
    wrong shouldBe empty
  }

  it should "answer a STRUCT beside a STRUCT or a GEO_POINT as DuckDB's merged STRUCT" in {
    // MEASURED on DuckDB 1.5.5, and through arrow for a point stored as an object: every field of
    // every branch, in the order it first appears; a field both have, matched whatever its case,
    // takes DuckDB's type for the two; a GEO_POINT is STRUCT(lat DOUBLE, lon DOUBLE)
    def fields(sql: String): Either[String, (SQLType, Seq[(String, String)])] =
      columns(sql).map { cs =>
        def render(f: Seq[MultiSearch.StructField]): Seq[(String, String)] =
          f.map { x =>
            val nested = render(x.fields).map(p => s"${p._1} ${p._2}")
            x.name -> (if (x.fields.nonEmpty) nested.mkString("(", ", ", ")") else x.sqlType.typeId)
          }
        cs.head.resultType -> render(cs.head.fields)
      }
    val point = Seq("lat" -> "DOUBLE", "lon" -> "DOUBLE")
    val expected: Seq[(String, (SQLType, Seq[(String, String)]))] = Seq(
      union("geo", "obj")  -> (SQLTypes.Struct, point ++ Seq("a" -> "INT", "b" -> "VARCHAR")),
      union("obj", "geo")  -> (SQLTypes.Struct, Seq("a" -> "INT", "b" -> "VARCHAR") ++ point),
      union("geo", "geo2") -> (SQLTypes.GeoPoint, point),
      union("geo", "NULL") -> (SQLTypes.GeoPoint, point),
      union("obj", "obj")  -> (SQLTypes.Struct, Seq("a" -> "INT", "b" -> "VARCHAR")),
      union("obj", "o2") -> (SQLTypes.Struct, Seq(
        "a" -> "INT",
        "b" -> "VARCHAR",
        "c" -> "BOOLEAN"
      )),
      union("o2", "obj") -> (SQLTypes.Struct, Seq(
        "b" -> "VARCHAR",
        "c" -> "BOOLEAN",
        "a" -> "INT"
      )),
      // a field both have: DuckDB's type for the two, the first name
      union("obj", "oa") -> (SQLTypes.Struct, Seq("a" -> "VARCHAR", "b" -> "VARCHAR")),
      union("geo", "olat") -> (SQLTypes.Struct, Seq(
        "lat" -> "VARCHAR",
        "lon" -> "DOUBLE",
        "z"   -> "INT"
      )),
      // a point inside an object, beside a point
      union("onest", "geo") ->
      (SQLTypes.Struct, Seq("p" -> "(lat DOUBLE, lon DOUBLE)", "q" -> "INT") ++ point),
      union("geo", "obj", "o2") ->
      (SQLTypes.Struct, point ++ Seq("a" -> "INT", "b" -> "VARCHAR", "c" -> "BOOLEAN"))
    )
    val wrong = expected.flatMap { case (sql, want) =>
      fields(sql) match {
        case Right(got) if got == want => None
        case other                     => Some(s"[$sql] answered $other instead of $want")
      }
    }
    wrong shouldBe empty
  }

  it should "type a struct's field and a list's element over every branch of the node at once" in {
    // MEASURED on DuckDB 1.5.5, every order, UNION ALL and UNION alike: a field `a` that is an
    // INTEGER, a DATE and a VARCHAR is a VARCHAR ({'1'}, {'2024-01-31'}, {'x'}); so are the
    // elements of INTEGER[], DATE[] and VARCHAR[], and the field `c` of a field `p` -- whatever
    // the order, although an INTEGER beside a DATE alone fails. Core typed them two by two, from
    // the left, and refused the orders that met the INTEGER and the DATE first.
    def typed(sql: String): Either[String, (SQLType, Seq[String])] =
      Parser(sql) match {
        case Right(m: MultiSearch) =>
          def render(f: Seq[MultiSearch.StructField]): Seq[String] =
            f.map { x =>
              if (x.fields.nonEmpty) s"${x.name} (${render(x.fields).mkString(", ")})"
              else s"${x.name} ${x.sqlType.typeId}"
            }
          MultiSearch
            .columnTypes(m.requests.map(_.update(Some(schema))), m.resolvedOperators)
            .map(cs => cs.head.resultType -> render(cs.head.fields))
        case other => fail(s"[$sql] expected a set operation, got $other")
      }
    val cases: Seq[(Seq[String], (SQLType, Seq[String]))] = Seq(
      Seq("oi", "od", "ov") -> (SQLTypes.Struct, Seq("a VARCHAR")),
      Seq("ai", "ad", "av") -> (SQLTypes.Array(SQLTypes.Varchar), Nil),
      Seq("si", "sd", "sv") -> (SQLTypes.Struct, Seq("p (c VARCHAR)")),
      // a BOOLEAN joins an INTEGER, and text makes the field text
      Seq("ob", "oi", "ov") -> (SQLTypes.Struct, Seq("a VARCHAR")),
      // a VARBINARY beside text and another type is text, as in a column: the relational engine
      // reads a binary as its base64 text (DuckDB alone, typing a BLOB, fails on it)
      Seq("obl", "ov", "oi") -> (SQLTypes.Struct, Seq("a VARCHAR"))
    )
    val statements = for {
      (items, want) <- cases
      order         <- items.permutations.toSeq
      operator      <- Seq("UNION ALL", "UNION")
    } yield order.map(i => s"SELECT $i AS k FROM t").mkString(s" $operator ") -> want
    statements should have size 60
    val wrong = statements.flatMap { case (sql, want) =>
      typed(sql) match {
        case Right(got) if got == want => None
        case other                     => Some(s"[$sql] answered $other instead of $want")
      }
    }
    wrong shouldBe empty
    // a VARBINARY beside text only stays a VARBINARY (MEASURED: STRUCT(a BLOB))
    typed(union("obl", "ov")) shouldBe Right((SQLTypes.Struct, Seq("a VARBINARY")))
  }

  it should "name the field two structs collide on, and its type in each" in {
    // MEASURED on DuckDB 1.5.5: an INTEGER beside a DATE, with no text, fails in every order
    // (`Unimplemented type for cast (INTEGER -> DATE)`), inside a field as inside a list; so does
    // an object beside any other type, text included
    val refused: Seq[(String, String)] = Seq(
      union("oi", "od") -> "branch 1 'k' field 'a' is INT, branch 2 'k' field 'a' is DATE",
      union("od", "oi") -> "branch 1 'k' field 'a' is DATE, branch 2 'k' field 'a' is INT",
      // the first two the field fails on: the BOOLEAN joins the INTEGER, not the DATE
      union("oi", "ob", "od") -> "branch 1 'k' field 'a' is INT, branch 3 'k' field 'a' is DATE",
      union("si", "sd") -> "branch 1 'k' field 'p.c' is INT, branch 2 'k' field 'p.c' is DATE",
      union("si", "pn") -> "branch 1 'k' field 'p' is STRUCT, branch 2 'k' field 'p' is INT",
      union("pv", "si", "sv") ->
      "branch 1 'k' field 'p' is VARCHAR, branch 2 'k' field 'p' is STRUCT",
      // a list keeps naming the two lists
      union("ai", "ad") -> "branch 1 'k' is ARRAY<INT>, branch 2 'k' is ARRAY<DATE>",
      // a node of the tree: the subtree is named by the branch that decides its type
      chain("oi UNION ALL oi EXCEPT od") ->
      "branch 1 'k' field 'a' is INT, branch 3 'k' field 'a' is DATE"
    )
    val wrong = refused.flatMap { case (sql, reason) =>
      Parser(sql) match {
        case Right(m: MultiSearch) =>
          MultiSearch.branchTypes(
            m.requests.map(_.update(Some(schema))),
            m.resolvedOperators
          ) match {
            case Left(msg)
                if msg ==
                  s"Set operation branches must project compatible types at column 1: $reason" =>
              None
            case other => Some(s"[$sql] answered $other instead of refusing with $reason")
          }
        case other => fail(s"[$sql] expected a set operation, got $other")
      }
    }
    wrong shouldBe empty
  }

  it should "answer an ARRAY, a STRUCT or a GEO_POINT beside its own type, NULL, or a list" in {
    // two lists of different element types take DuckDB 1.5.5's answer (MEASURED): INTEGER[] with
    // VARCHAR[] is VARCHAR[], with BIGINT[] BIGINT[]; VARCHAR[] with DATE[] is VARCHAR[]
    val answered: Seq[(String, SQLType)] = Seq(
      union("geo", "geo")   -> SQLTypes.GeoPoint,
      union("geo", "NULL")  -> SQLTypes.GeoPoint,
      union("obj", "obj")   -> SQLTypes.Struct,
      union("tags", "tags") -> SQLTypes.Array(SQLTypes.Varchar),
      union("NULL", "tags") -> SQLTypes.Array(SQLTypes.Varchar),
      union("arr", "tags")  -> SQLTypes.Array(SQLTypes.Varchar),
      union("tags", "arr")  -> SQLTypes.Array(SQLTypes.Varchar),
      union("arr", "arrb")  -> SQLTypes.Array(SQLTypes.BigInt),
      union("tags", "arrd") -> SQLTypes.Array(SQLTypes.Varchar)
    )
    val wrong = answered.flatMap { case (sql, want) =>
      typeOf(sql) match {
        case Right(got) if got == want => None
        case other                     => Some(s"[$sql] answered $other instead of $want")
      }
    }
    wrong shouldBe empty
  }

  it should "refuse nothing on types before the column types are known" in {
    // with no schema a bare column is ANY: Elasticsearch answers, as for a single statement
    MultiSearch
      .columnTypes(branches("SELECT d AS k FROM t UNION ALL SELECT n AS k FROM t", false))
      .map(_.map(_.resultType)) shouldBe Right(Seq(SQLTypes.Any))
    MultiSearch.branchTypes(branches("SELECT a FROM t UNION SELECT 'x' FROM u", false)) shouldBe
    Right(())
  }

  it should "carry each date field's mapping format to its position, at every depth" in {
    def format(f: String) =
      scala.collection.immutable.ListMap[String, app.softnetwork.elastic.sql.Value[_]](
        "format" -> app.softnetwork.elastic.sql.StringValue(f)
      )
    val formatted = Table(
      "t",
      columns = List(
        Column("ts", SQLTypes.Timestamp, options = format("epoch_second")),
        Column("d", SQLTypes.Date),
        Column("n", SQLTypes.Int),
        Column(
          "obj",
          SQLTypes.Struct,
          multiFields = List(
            Column("a", SQLTypes.Timestamp, options = format("yyyy/MM/dd")),
            Column(
              "in",
              SQLTypes.Struct,
              multiFields = List(Column("b", SQLTypes.Date, options = format("basic_date")))
            )
          )
        ),
        Column(
          "od",
          SQLTypes.Struct,
          multiFields = List(
            Column("a", SQLTypes.Date),
            Column("in", SQLTypes.Struct, multiFields = List(Column("b", SQLTypes.Timestamp)))
          )
        ),
        Column(
          "items",
          SQLTypes.Array(SQLTypes.Struct),
          multiFields = List(Column("a", SQLTypes.Date, options = format("dd/MM/yyyy")))
        ),
        Column(
          "items2",
          SQLTypes.Array(SQLTypes.Struct),
          multiFields = List(Column("a", SQLTypes.Timestamp))
        )
      )
    )
    def typed(sql: String): MultiSearch.SetOperationColumn =
      Parser(sql) match {
        case Right(m: MultiSearch) =>
          MultiSearch.columnTypes(m.requests.map(_.update(Some(formatted)))) match {
            case Right(columns) => columns.head
            case Left(refusal)  => fail(refusal)
          }
        case other => fail(s"[$sql] expected a set operation, got $other")
      }
    val default = Some(TemporalLiterals.DefaultDateFormat)
    // the column: its own format, else Elasticsearch's default
    typed(union("ts", "d")).branches.map(_.flatMap(_.format)) shouldBe
    Seq(Some("epoch_second"), default)
    // a computed value reads no field: no format
    typed(union("ts", "CURRENT_DATE")).branches.map(_.flatMap(_.format)) shouldBe
    Seq(Some("epoch_second"), None)
    // an object's field, and a field of an object inside it
    def field(t: Option[MultiSearch.SetOperationType], path: String*): Option[String] =
      path
        .foldLeft(t)((acc, name) => acc.flatMap(_.fields.find(_.name == name)).map(_.valueType))
        .flatMap(_.format)
    val objects = typed(union("obj", "od"))
    objects.branches.map(field(_, "a")) shouldBe Seq(Some("yyyy/MM/dd"), default)
    objects.branches.map(field(_, "in", "b")) shouldBe Seq(Some("basic_date"), default)
    // a list of objects' field
    typed(union("items", "items2")).branches.map(b => field(b.flatMap(_.element), "a")) shouldBe
    Seq(Some("dd/MM/yyyy"), default)
    // a column's result holds no format: no field decides it
    def formats(t: MultiSearch.SetOperationType): Seq[String] =
      t.format.toSeq ++ t.fields.flatMap(f => formats(f.valueType)) ++ t.element.toSeq.flatMap(
        formats
      )
    Seq(union("ts", "d"), union("obj", "od"), union("items", "items2")).foreach { sql =>
      withClue(sql)(formats(typed(sql).result) shouldBe empty)
    }
  }

  it should "say which columns convert their values: only those whose branches disagree" in {
    def converts(sql: String): Seq[Boolean] = columns(sql).map(_.map(_.converts)).getOrElse(Nil)
    converts(union("n", "x")) shouldBe Seq(true)
    converts(union("d", "ts")) shouldBe Seq(true)
    converts(union("id", "n")) shouldBe Seq(true)
    converts(union("ts", "s")) shouldBe Seq(true)
    converts(union("x", "'a'")) shouldBe Seq(true)
    converts(union("bin", "s")) shouldBe Seq(true)
    // a VARCHAR column DuckDB gives no type to: every value spelled as text
    converts(union("bin", "n")) shouldBe Seq(true)
    converts(union("bin", "d", "n")) shouldBe Seq(true)
    converts(union("CAST(ts AS TIME)", "d")) shouldBe Seq(true)
    // two lists of different element types: every element converted (the lead's ruling of
    // 2026-10-07: one converter at every depth); the same list, an object beside an object of the
    // same fields: nothing to convert
    converts(union("arr", "tags")) shouldBe Seq(true)
    converts(union("tags", "tags")) shouldBe Seq(false)
    converts(union("obj", "obj")) shouldBe Seq(false)
    // a field whose branches disagree converts its struct, a field that agrees does not
    converts(union("obj", "oa")) shouldBe Seq(true)
    converts(union("onest", "onest")) shouldBe Seq(false)
    // every point becomes {lat, lon}, every object the column's fields
    converts(union("geo", "geo")) shouldBe Seq(true)
    converts(union("geo", "obj")) shouldBe Seq(true)
    converts(union("obj", "o2")) shouldBe Seq(true)
    // a VARBINARY beside VARBINARY or a NULL literal only: nothing to convert
    converts(union("bin", "bin")) shouldBe Seq(false)
    converts(union("bin", "NULL")) shouldBe Seq(false)
    // branches that agree pay nothing
    converts(union("n", "2")) shouldBe Seq(false)
    converts(union("id", "txt")) shouldBe Seq(false)
    converts(union("d", "CURRENT_DATE")) shouldBe Seq(false)
    // a NULL literal has nothing to convert
    converts(union("n", "NULL")) shouldBe Seq(false)
    // a SELECT * branch is not typed here, so nothing is converted beside it
    converts("SELECT n AS k FROM t UNION ALL SELECT * FROM t") shouldBe Seq(false)
    // position by position
    converts("SELECT id, n FROM t UNION ALL SELECT g, x FROM t") shouldBe Seq(false, true)
  }

  // ── the operator tree (a chain of set operators is typed as a TREE, as DuckDB 1.5.5 types it) ──

  /** `SELECT <a> AS k FROM t <op> SELECT <b> AS k FROM t ...` from `a op b op c`. */
  private def chain(spec: String): String = {
    val operators = "(UNION ALL|UNION DISTINCT|UNION|INTERSECT ALL|INTERSECT|EXCEPT ALL|EXCEPT)"
    val parts = spec.split(s"\\s+$operators\\s+")
    val ops = s"\\s+$operators\\s+".r.findAllMatchIn(spec).map(_.group(1)).toList
    (s"SELECT ${parts.head} AS k FROM t" :: ops.zip(parts.tail).map { case (op, item) =>
      s"$op SELECT $item AS k FROM t"
    }).mkString(" ")
  }

  private def treeTypes(sql: String): Either[String, Seq[SQLType]] =
    Parser(sql) match {
      case Right(m: MultiSearch) =>
        MultiSearch
          .columnTypes(m.requests.map(_.update(Some(schema))), m.resolvedOperators)
          .map(_.map(_.resultType))
      case other => fail(s"[$sql] expected a set operation, got $other")
    }

  "A chain of set operators" should "be typed over its operator tree, each node by the rules" in {
    // the tree MEASURED on DuckDB 1.5.5 (over 18,000 random chains, and again parenthesised): a
    // run of ONE union kind is one node, whatever its order or parentheses; INTERSECT binds first,
    // INTERSECT and EXCEPT are binary, left to right; two union kinds are two nodes. Each node by
    // the lead's rules of 2026-10-07. The literal-free chains are MEASURED through arrow (N.., U..,
    // F.., X.., and the T / Q shapes of its second matrix)
    val answered: Seq[((String, String), SQLType)] = Seq(
      "X05" -> chain("n UNION d UNION 'a'")                   -> SQLTypes.Varchar,
      "X09" -> chain("n UNION ALL d INTERSECT 'a'")           -> SQLTypes.Varchar,
      "X11" -> chain("n UNION DISTINCT d UNION DISTINCT 'a'") -> SQLTypes.Varchar,
      "X12" -> chain("'a' EXCEPT n UNION ALL d")              -> SQLTypes.Varchar,
      "Y01" -> chain("'a' INTERSECT n INTERSECT d")           -> SQLTypes.Varchar,
      "Y02" -> chain("n INTERSECT 'a' INTERSECT d")           -> SQLTypes.Varchar,
      "Z01" -> chain("n INTERSECT x INTERSECT 'a'")           -> SQLTypes.Varchar,
      "Z02" -> chain("d INTERSECT ts UNION ALL 'a'")          -> SQLTypes.Varchar,
      "Z03" -> chain("n EXCEPT x UNION ALL 'a'")              -> SQLTypes.Varchar,
      "Z04" -> chain("n UNION ALL x INTERSECT bg")            -> SQLTypes.Double,
      "Z05" -> chain("d EXCEPT ts")                           -> SQLTypes.Timestamp,
      "Z06" -> chain("n INTERSECT ALL x")                     -> SQLTypes.Double,
      "Z07" -> chain("s EXCEPT ALL bin")                      -> SQLTypes.VarBinary,
      "Z10" -> chain("n UNION ALL d UNION ALL 'a' EXCEPT x")  -> SQLTypes.Varchar,
      "Z12" -> chain("'a' UNION ALL n UNION d")               -> SQLTypes.Varchar,
      "Z13" -> chain("b INTERSECT n EXCEPT ALL x")            -> SQLTypes.Double,
      "Z14" -> chain("ts INTERSECT ALL d INTERSECT ALL 'x'")  -> SQLTypes.Varchar,
      "Z16" -> chain("n UNION ALL 'a' INTERSECT d")           -> SQLTypes.Varchar,
      "Z18" -> chain("n UNION ALL d UNION ALL 'a'")           -> SQLTypes.Varchar,
      "Z20" -> chain("s UNION ALL n EXCEPT ALL d")            -> SQLTypes.Varchar,
      // a node DuckDB gives no type to is VARCHAR (rule 2), the BLOB beside text and a number too
      "X14"  -> chain("n UNION ALL bin UNION ALL s") -> SQLTypes.Varchar,
      "Z08"  -> chain("bin UNION s UNION n")         -> SQLTypes.Varchar,
      "X14i" -> chain("n INTERSECT bin INTERSECT s") -> SQLTypes.Varchar,
      // ... and a VARCHAR subtree is text to its parent
      "W01" -> chain("bin INTERSECT n UNION ALL d") -> SQLTypes.Varchar,
      // a text operand makes the node text before a date meets a number
      "N07u" -> chain("n UNION d UNION s")         -> SQLTypes.Varchar,
      "N07a" -> chain("n UNION ALL d UNION ALL s") -> SQLTypes.Varchar,
      "U55i" -> chain("s INTERSECT n INTERSECT d") -> SQLTypes.Varchar,
      "U55e" -> chain("s EXCEPT n EXCEPT d")       -> SQLTypes.Varchar,
      "F12i" -> chain("b INTERSECT x INTERSECT s") -> SQLTypes.Varchar,
      "N03"  -> chain("n UNION ALL x INTERSECT s") -> SQLTypes.Varchar,
      "N05"  -> chain("n UNION x UNION ALL bg")    -> SQLTypes.Double,
      // a VARBINARY is text to the date beside the number (arrow's T and Q shapes): every union
      // run holding it, every INTERSECT / EXCEPT whose first node holds it
      "Tu" -> chain("d UNION n UNION bin")                      -> SQLTypes.Varchar,
      "Ta" -> chain("bin UNION ALL d UNION ALL n")              -> SQLTypes.Varchar,
      "Ti" -> chain("bin INTERSECT d INTERSECT n")              -> SQLTypes.Varchar,
      "Te" -> chain("d EXCEPT bin EXCEPT n")                    -> SQLTypes.Varchar,
      "Qi" -> chain("ts INTERSECT bin INTERSECT x INTERSECT s") -> SQLTypes.Varchar,
      "Qe" -> chain("bin EXCEPT d EXCEPT n EXCEPT s")           -> SQLTypes.Varchar,
      // a nested type beside its own type, a subtree included
      "G1" -> chain("geo INTERSECT geo UNION ALL geo") -> SQLTypes.GeoPoint,
      "G2" -> chain("arr UNION ALL tags EXCEPT arrb")  -> SQLTypes.Array(SQLTypes.Varchar),
      // a point beside an object, in any node: DuckDB's merged struct
      "G6" -> chain("obj UNION ALL geo INTERSECT obj") -> SQLTypes.Struct
    )
    val refused: Seq[(String, String)] = Seq(
      "X01" -> chain("n EXCEPT d UNION ALL 'a'"),
      "X02" -> chain("d INTERSECT n UNION ALL 'a'"),
      "X03" -> chain("n UNION d UNION ALL 'a'"),
      "X04" -> chain("n UNION ALL d UNION 'a'"),
      "X06" -> chain("n EXCEPT d EXCEPT 'a'"),
      "X07" -> chain("n INTERSECT d INTERSECT 'a'"),
      "X08" -> chain("'a' UNION ALL n INTERSECT d"),
      "X10" -> chain("n EXCEPT x UNION ALL d"),
      "X13" -> chain("n UNION ALL d EXCEPT 'a'"),
      "X15" -> chain("s UNION ALL b INTERSECT d"),
      "Y03" -> chain("n INTERSECT d"),
      "Y04" -> chain("n EXCEPT d"),
      "Y05" -> chain("n INTERSECT d INTERSECT 'a'"),
      "Z15" -> chain("x UNION ALL n UNION ALL d"),
      "Z17" -> chain("d UNION d2 EXCEPT n"),
      "Z19" -> chain("n INTERSECT ALL d2 UNION ALL 'a'"),
      // MEASURED through arrow: the date meets the number in a node with no text
      "N07i" -> chain("n INTERSECT d INTERSECT s"),
      "N07e" -> chain("n EXCEPT d EXCEPT s"),
      // MEASURED through arrow (T and Q shapes): the first node meets the date and the number
      // before the VARBINARY can make it text
      "Ti2" -> chain("d INTERSECT n INTERSECT bin"),
      "Te2" -> chain("x EXCEPT ts EXCEPT bin"),
      "Qi2" -> chain("b INTERSECT d INTERSECT s INTERSECT bin"),
      // a TIME does not make the node text
      "W02" -> chain("CAST(ts AS TIME) UNION ALL d UNION ALL n"),
      // a nested type beside another type, in any node
      "G3" -> chain("geo INTERSECT geo UNION ALL s"),
      "G4" -> chain("s UNION bin UNION geo"),
      "G5" -> chain("arr EXCEPT arrd")
    )
    val wrong =
      answered.flatMap { case ((id, sql), want) =>
        treeTypes(sql) match {
          case Right(Seq(got)) if got == want => None
          case other => Some(s"$id [$sql] answered $other instead of $want")
        }
      } ++ refused.flatMap { case (id, sql) =>
        treeTypes(sql) match {
          case Left(_) => None
          case other   => Some(s"$id [$sql] answered $other instead of refusing")
        }
      }
    wrong shouldBe empty
  }

  it should "name, in a refusal, the branch that decides each side's type" in {
    def refusal(spec: String): Either[String, Unit] =
      Parser(chain(spec)) match {
        case Right(m: MultiSearch) =>
          MultiSearch.branchTypes(m.requests.map(_.update(Some(schema))), m.resolvedOperators)
        case other => fail(s"[$spec] expected a set operation, got $other")
      }
    val prefix = "Set operation branches must project compatible types at column 1: "
    // the INTERSECT / EXCEPT side is one node: `n` meets `d` there
    refusal("n EXCEPT d UNION ALL 'a'") shouldBe
    Left(prefix + "branch 1 'k' is INT, branch 2 'k' is DATE")
    // a subtree is named by the branch that decides its type: `n EXCEPT x` is DOUBLE, x's
    refusal("n EXCEPT x UNION ALL d") shouldBe
    Left(prefix + "branch 2 'k' is DOUBLE, branch 3 'k' is DATE")
    refusal("s UNION ALL b INTERSECT d") shouldBe
    Left(prefix + "branch 2 'k' is BOOLEAN, branch 3 'k' is DATE")
    refusal("n INTERSECT d INTERSECT s") shouldBe
    Left(prefix + "branch 1 'k' is INT, branch 2 'k' is DATE")
    refusal("d INTERSECT n INTERSECT bin") shouldBe
    Left(prefix + "branch 1 'k' is DATE, branch 2 'k' is INT")
    // a nested subtree is named by its first branch, beside the first operand of another type
    refusal("geo INTERSECT geo UNION ALL s") shouldBe
    Left(prefix + "branch 1 'k' is GEO_POINT, branch 3 'k' is KEYWORD")
  }

  it should "answer core's UNION ALL exactly as the single run it always was" in {
    // every UNION ALL above, through the operator tree and through the flat fold: one answer
    val unionAll = Seq(
      union("d", "CURRENT_DATE"),
      union("n", "'1'"),
      union("d", "ts", "d2"),
      union("d", "n", "'1'"),
      union("n", "NULL", "x"),
      union("b", "n"),
      union("bin", "s"),
      union("bin", "NULL", "s"),
      union("d", "n"),
      union("b", "n", "d"),
      union("bin", "s", "d"),
      union("bin", "n"),
      union("geo", "s"),
      union("bin", "d", "n"),
      union("d", "n", "s"),
      "SELECT id, d FROM t UNION ALL SELECT g, n FROM t"
    )
    val wrong = unionAll.flatMap { sql =>
      val flat = MultiSearch.columnTypes(branches(sql)).map(_.map(_.resultType))
      val tree = treeTypes(sql)
      if (flat == tree) None else Some(s"[$sql] tree $tree, flat $flat")
    }
    wrong shouldBe empty
  }
}
