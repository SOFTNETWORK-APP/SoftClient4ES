package app.softnetwork.elastic.sql.schema

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story BIDC-10a Part D (AC 10) — `SHOW TABLES` publishes the CLIENT vocabulary, and it does so
  * through one authority so the display value cannot drift from the stored one.
  *
  * The projection used to be `TableType.name.toUpperCase`, so a plain index was advertised as
  * `REGULAR` — a value no client consumes. The ADBC leg of the sidecar's integration suite failed
  * with `Expected 'TABLE' in ['REGULAR']`, and an ODBC object browser filtering on the standard
  * vocabulary shows no tables at all. `TableType.sqlName` is now the single source of that value
  * and `TableType.name` remains the `_meta.type` storage key.
  *
  * 🔴 The enumeration is derived from the COMPILED package, never from a hand-written list.
  * `TableType` is `sealed`, so the compiler guarantees every subtype is declared in the same file
  * and compiled into the same package directory; listing that directory is therefore complete and
  * needs no allow-list. A seventh table type lands here and reddens `expected`, which is the point:
  * this is where the vocabulary is re-decided. (Same mechanism, and the same lesson, as
  * `HelpCorpusSpec`'s AST walk — an enumeration by anything narrower silently misses a leaf.)
  */
class TableTypeVocabularySpec extends AnyFlatSpec with Matchers {

  /** The reconciled table (story spec, section Reconciled values D.3): TableType -> (stored
    * `_meta.type` name, `SHOW TABLES` value). The stored column is what must NOT move.
    */
  private val expected: Map[TableType, (String, String)] = Map(
    TableType.Regular          -> (("regular", "TABLE")),
    TableType.View             -> (("view", "VIEW")),
    TableType.MaterializedView -> (("materialized_view", "MATERIALIZED VIEW")),
    TableType.External         -> (("external", "EXTERNAL")),
    TableType.Changelog        -> (("changelog", "CHANGELOG")),
    TableType.Enrichment       -> (("enrichment", "ENRICHMENT"))
  )

  /** The enumeration is derived from the COMPILED package, never from a hand-written list, and it
    * is shared with `ShowTablesTableTypeSpec` so the two cannot disagree about how many types
    * exist. Every drop path in that walk FAILS rather than skipping - see [[TableTypeEnumeration]].
    */
  private def declaredTableTypes: Seq[TableType] = TableTypeEnumeration.declaredTableTypes

  "The TableType hierarchy" should "be enumerated exactly by the reconciled vocabulary table" in {
    val declared = declaredTableTypes
    declared should not be empty
    val missing = declared.toSet.diff(expected.keySet)
    val stale = expected.keySet.diff(declared.toSet)
    withClue(
      s"undocumented table types: ${missing.map(_.name).mkString(", ")}; " +
      s"table types in the expectation that no longer exist: ${stale.map(_.name).mkString(", ")}. " +
      "A new TableType must decide what clients call it -- see TableType.sqlName. "
    ) {
      missing shouldBe empty
      stale shouldBe empty
    }
    declared.size shouldBe expected.size
  }

  it should "keep the STORED name unchanged for every type" in {
    declaredTableTypes.foreach { t =>
      withClue(s"stored `_meta.type` name of $t: ") {
        t.name shouldBe expected(t)._1
      }
    }
  }

  it should "publish the client vocabulary as sqlName for every type" in {
    declaredTableTypes.foreach { t =>
      withClue(s"`SHOW TABLES` value of $t: ") {
        t.sqlName shouldBe expected(t)._2
      }
    }
  }

  /** The literal pin. Property assertions survive a vocabulary change; a literal one catches it —
    * that asymmetry is precisely what let `REGULAR` ship (BIDC-6's `TableTypeDomainSpec` asserted
    * the defect as intended behaviour and stayed green).
    */
  it should "never advertise the internal name of a plain index" in {
    TableType.Regular.sqlName shouldBe "TABLE"
    TableType.Regular.sqlName should not be "REGULAR"
    declaredTableTypes.map(_.sqlName) should not contain "REGULAR"
  }

  /** AD-A-6-SUPERSEDED: the ENGINE surface keeps the two apart. The JDBC and Flight `TABLE_TYPE`
    * contracts deliberately collapse a materialized view onto `VIEW` - that mapping lives in the
    * drivers and is pinned there, not here.
    */
  it should "keep MATERIALIZED VIEW distinct from VIEW" in {
    TableType.MaterializedView.sqlName should not be TableType.View.sqlName
  }

  /** 🔴 The spelling is SPACED because every statement naming this object is spaced (`CREATE
    * MATERIALIZED VIEW`, `SHOW MATERIALIZED VIEW STATUS`, ...). An underscore would make the `SHOW
    * TABLES` `type` column the only place the product spells its own object differently from the
    * statement that creates it. Pinned literally: a property assertion cannot see this.
    */
  it should "spell a materialized view the way our own SQL spells it" in {
    TableType.MaterializedView.sqlName shouldBe "MATERIALIZED VIEW"
    TableType.MaterializedView.sqlName should not include "_"
  }

  /** Well-formed = upper-case words separated by single spaces. A SPACE is legal (AD-A-6-
    * SUPERSEDED, `MATERIALIZED VIEW`); an underscore is not, so this assertion is also what stops
    * the spaced spelling silently regressing. Distinctness matters because two types sharing a
    * client name would make `SHOW TABLES` ambiguous.
    */
  it should "give every type a distinct, well-formed client name" in {
    val names = declaredTableTypes.map(_.sqlName)
    names.distinct.size shouldBe names.size
    names.foreach { n =>
      withClue(s"client name `$n`: ") {
        n should fullyMatch regex "[A-Z]+( [A-Z]+)*"
      }
    }
  }

  /** 🔴 The migration guard. Adding a display name must NOT make an existing index's stored
    * `_meta.type` unreadable: `TableType.apply` still parses the stored name, and only the stored
    * name.
    */
  it should "still parse every stored name back to its own type" in {
    declaredTableTypes.foreach { t =>
      withClue(s"round-trip of $t through _meta.type: ") {
        TableType(t.name) shouldBe t
        TableType(t.name.toUpperCase) shouldBe t
      }
    }
  }

  it should "keep parsing the legacy stored value of a plain index" in {
    TableType("regular") shouldBe TableType.Regular
  }
}
