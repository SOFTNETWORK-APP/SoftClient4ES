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

package app.softnetwork.elastic.client.metadata

import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Types

/** Story BIDC-10a — the ONE type catalogue, in `core`, that the JDBC driver's `getTypeInfo` and the
  * Flight producer's `CommandGetXdbcTypeInfo` both derive from instead of hand-transcribing.
  *
  * Two kinds of assertion, deliberately, because each is blind to what the other catches (BIDC-6's
  * lesson, and D.1b's): the POSITIONAL fixture below is satisfied by any order the fixture and the
  * source agree on - a coordinated edit keeps it green - while the ORDERING PROPERTIES are computed
  * from the contract and redden on a reorder with no fixture edit at all.
  */
class JdbcTypeCatalogSpec extends AnyFlatSpec with Matchers {

  /** The catalogue in contract order: (typeId, DATA_TYPE, PRECISION, prefix, suffix).
    *
    * The eleven types that shipped before this story keep their exact values; the nine new ones
    * follow the size rule documented on `JdbcTypeCatalog.declared` (types sharing a DATA_TYPE share
    * its precision).
    */
  private val expected: Seq[(String, Int, Int, Option[String], Option[String])] = Seq(
    ("TINYINT", Types.TINYINT, 3, None, None),
    ("BIGINT", Types.BIGINT, 19, None, None),
    ("VARBINARY", Types.VARBINARY, 65535, Some("'"), Some("'")),
    ("CHAR", Types.CHAR, 65535, Some("'"), Some("'")),
    ("INT", Types.INTEGER, 10, None, None),
    ("SMALLINT", Types.SMALLINT, 5, None, None),
    ("REAL", Types.REAL, 7, None, None),
    ("DOUBLE", Types.DOUBLE, 15, None, None),
    ("NUMERIC", Types.DOUBLE, 15, None, None),
    ("VARCHAR", Types.VARCHAR, 65535, Some("'"), Some("'")),
    ("GEO_POINT", Types.VARCHAR, 65535, Some("'"), Some("'")),
    ("KEYWORD", Types.VARCHAR, 65535, Some("'"), Some("'")),
    ("TEXT", Types.VARCHAR, 65535, Some("'"), Some("'")),
    ("BOOLEAN", Types.BOOLEAN, 1, None, None),
    ("DATE", Types.DATE, 10, Some("'"), Some("'")),
    ("TIME", Types.TIME, 8, Some("'"), Some("'")),
    ("TIMESTAMP", Types.TIMESTAMP, 23, Some("'"), Some("'")),
    ("DATETIME", Types.TIMESTAMP, 23, Some("'"), Some("'")),
    ("STRUCT", Types.STRUCT, 0, None, None),
    ("ARRAY<STRUCT>", Types.ARRAY, 0, None, None)
  )

  private def actual =
    JdbcTypeCatalog.entries.map(e =>
      (e.typeId, e.dataType, e.columnSize, e.literalPrefix, e.literalSuffix)
    )

  "The JDBC type catalogue" should "stream exactly the reconciled rows, in order" in {
    actual shouldBe expected
  }

  it should "cover every user-facing column type the engine can report" in {
    JdbcTypeCatalog.entries.map(_.typeId).toSet shouldBe Set(
      "TINYINT",
      "SMALLINT",
      "INT",
      "BIGINT",
      "REAL",
      "DOUBLE",
      "NUMERIC",
      "BOOLEAN",
      "DATE",
      "TIME",
      "DATETIME",
      "TIMESTAMP",
      "KEYWORD",
      "TEXT",
      "VARCHAR",
      "CHAR",
      "STRUCT",
      "ARRAY<STRUCT>",
      "GEO_POINT",
      "VARBINARY"
    )
  }

  /** 🔴 The derivation pin. A row does not STATE a type code - it computes one - so this asserts
    * that the catalogue and the mapping cannot disagree. If `dataType` ever became a constructor
    * field, this is the test that should have to change.
    */
  it should "derive every DATA_TYPE from the one mapping" in {
    JdbcTypeCatalog.entries.foreach { e =>
      withClue(s"${e.typeId}: ") {
        e.dataType shouldBe JdbcTypeCatalog.jdbcType(e.sqlType)
        e.typeId shouldBe e.sqlType.typeId
      }
    }
  }

  it should "never map a catalogued type to OTHER" in {
    JdbcTypeCatalog.entries.foreach { e =>
      withClue(s"${e.typeId} fell through the mapping: ") {
        e.dataType should not be Types.OTHER
      }
    }
  }

  // -- The ordering contract -------------------------------------------------
  //
  // `getTypeInfo`'s javadoc: rows "are ordered by DATA_TYPE and then by how closely the data type
  // maps to the corresponding JDBC SQL type". These assertions are computed from that sentence, so
  // a reorder reddens them WITHOUT any fixture edit - which is the whole point, since the
  // positional pin above survives a coordinated edit (measured on the jdbc leg, D.1b).

  it should "order rows by DATA_TYPE ascending" in {
    val codes = JdbcTypeCatalog.entries.map(_.dataType)
    codes shouldBe codes.sorted
  }

  /** 🔴 Distinctness is GONE and this test is the record of it. Four types share `Types.VARCHAR`,
    * two share `Types.DOUBLE`, two share `Types.TIMESTAMP`. An assertion still pinning distinct
    * codes - as both repos' fixtures did while the catalogue had eleven rows - is asserting the old
    * catalogue, so the tie-break below is what has to hold instead.
    */
  it should "have genuine DATA_TYPE ties, so the tie-break is load-bearing" in {
    val grouped = JdbcTypeCatalog.entries.groupBy(_.dataType).filter(_._2.size > 1)
    grouped.keySet shouldBe Set(Types.VARCHAR, Types.DOUBLE, Types.TIMESTAMP)
    grouped(Types.VARCHAR).map(_.typeId) should contain theSameElementsAs
    Seq("VARCHAR", "TEXT", "KEYWORD", "GEO_POINT")
  }

  it should "put the canonically named type first within each DATA_TYPE group" in {
    val groups = JdbcTypeCatalog.entries.groupBy(_.dataType)
    val adjudicated = groups.filter { case (code, group) =>
      group.size > 1 && group.exists(e =>
        JdbcTypeCatalog.canonicalJdbcName(code).contains(e.typeId)
      )
    }
    // The rule is only meaningful where a group actually contains its canonical type.
    adjudicated.keySet shouldBe Set(Types.VARCHAR, Types.DOUBLE, Types.TIMESTAMP)
    adjudicated.foreach { case (code, _) =>
      val first = JdbcTypeCatalog.entries.filter(_.dataType == code).head.typeId
      withClue(s"first row of DATA_TYPE $code: ") {
        JdbcTypeCatalog.canonicalJdbcName(code) shouldBe Some(first)
      }
    }
  }

  it should "break remaining ties by typeId, so the order is total and stable" in {
    JdbcTypeCatalog.entries.groupBy(_.dataType).foreach { case (code, _) =>
      val canonical = JdbcTypeCatalog.canonicalJdbcName(code)
      val nonCanonical =
        JdbcTypeCatalog.entries.filter(e => e.dataType == code && !canonical.contains(e.typeId))
      withClue(s"non-canonical members of DATA_TYPE $code: ") {
        nonCanonical.map(_.typeId) shouldBe nonCanonical.map(_.typeId).sorted
      }
    }
  }

  /** The whole catalogue re-sorted by its own Ordering must be the catalogue. A no-op on a sorted
    * sequence, and a red the moment `entries` stops being produced by `catalogOrdering`.
    */
  it should "be produced by its own documented ordering" in {
    JdbcTypeCatalog.entries.sorted(JdbcTypeCatalog.catalogOrdering) shouldBe JdbcTypeCatalog.entries
  }

  /** ⚠️ `NULL` describes the absence of a value, not a type a column can have. Its presence in the
    * runtime mapping is legitimate and different; its presence HERE would be a defect.
    */
  it should "exclude NULL, which is not a storable column type" in {
    JdbcTypeCatalog.entries.map(_.typeId) should not contain "NULL"
    JdbcTypeCatalog.entries.map(_.dataType) should not contain Types.NULL
    // ...while the mapping still answers for it, because that is a different question.
    JdbcTypeCatalog.jdbcType(SQLTypes.Null) shouldBe Types.NULL
  }

  it should "resolve a getColumns TYPE_NAME back to its catalogue row" in {
    JdbcTypeCatalog.find("KEYWORD").map(_.dataType) shouldBe Some(Types.VARCHAR)
    JdbcTypeCatalog.find("keyword").map(_.typeId) shouldBe Some("KEYWORD")
    JdbcTypeCatalog.find("ARRAY<STRUCT>").map(_.dataType) shouldBe Some(Types.ARRAY)
    JdbcTypeCatalog.find("NULL") shouldBe None
  }
}
