package app.softnetwork.elastic.sql.schema

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.reflect.Modifier
import java.net.URL
import scala.util.Try

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
    TableType.MaterializedView -> (("materialized_view", "MATERIALIZED_VIEW")),
    TableType.External         -> (("external", "EXTERNAL")),
    TableType.Changelog        -> (("changelog", "CHANGELOG")),
    TableType.Enrichment       -> (("enrichment", "ENRICHMENT"))
  )

  /** Every `TableType` the compiler knows about, read off the compiled package directory.
    *
    * 🔴 `getResources`, not `getResource`. This spec lives in the SAME package as the type it
    * enumerates, so `sql/target/.../test-classes/app/.../schema` shadows the main classes directory
    * and the singular lookup returns the test tree — which holds no `TableType` at all. The first
    * version of this walk therefore enumerated NOTHING, and every per-type assertion below passed
    * VACUOUSLY over an empty sequence. Union every classpath root that carries the package, and
    * make emptiness a failure HERE so no caller can be vacuous.
    *
    * A non-`file:` classpath entry FAILS rather than skips, for the same reason. A concrete subtype
    * that is not a Scala `object` likewise fails rather than being dropped.
    */
  private def declaredTableTypes: Seq[TableType] = {
    val pkgPath = classOf[TableType].getName.split('.').init.mkString("/")
    val loader = classOf[TableType].getClassLoader
    val roots: Seq[URL] = {
      val e = loader.getResources(pkgPath)
      val b = Seq.newBuilder[URL]
      while (e.hasMoreElements) b += e.nextElement()
      b.result()
    }
    if (roots.isEmpty) fail(s"the schema package `$pkgPath` is not on the test classpath")
    roots.foreach { url =>
      if (url.getProtocol != "file")
        fail(
          s"the schema package `$pkgPath` resolves to a ${url.getProtocol} URL ($url). This walk " +
          "lists the compiled classes of a SEALED hierarchy and needs a directory; teach it to " +
          "read the archive rather than letting the vocabulary guard go vacuous."
        )
    }
    val subtypes: Seq[Class[_]] = roots
      .flatMap(url => Option(new File(url.toURI).listFiles).getOrElse(Array.empty[File]).toSeq)
      .filter(f => f.isFile && f.getName.endsWith(".class"))
      .map(f => pkgPath.replace('/', '.') + "." + f.getName.stripSuffix(".class"))
      .distinct
      .sorted
      // The `Option[Class[_]]` needs its type written out: on the 2.12 leg the existential defeats
      // `flatMap`'s inference.
      .flatMap { n =>
        val loaded: Option[Class[_]] = Try(Class.forName(n, false, loader)).toOption
        loaded.toSeq
      }
      .filter(c => classOf[TableType].isAssignableFrom(c))
      .filterNot(c => c.isInterface || Modifier.isAbstract(c.getModifiers))
      .distinct

    if (subtypes.isEmpty)
      fail(
        s"no concrete TableType was found under $roots. Every assertion in this spec iterates " +
        "this sequence, so an empty result would make them all pass vacuously."
      )

    subtypes.map { c =>
      val instance: Option[TableType] = Try(
        Class.forName(c.getName, true, loader).getField("MODULE$").get(null).asInstanceOf[TableType]
      ).toOption
      instance.getOrElse(
        fail(
          s"`${c.getName}` is a concrete TableType that is not a Scala `object`; this walk can " +
          "only instantiate case objects. Teach it, or the vocabulary guard silently loses a type."
        )
      )
    }
  }

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

  it should "keep MATERIALIZED_VIEW distinct from VIEW" in {
    TableType.MaterializedView.sqlName should not be TableType.View.sqlName
  }

  it should "give every type a distinct, well-formed client name" in {
    val names = declaredTableTypes.map(_.sqlName)
    names.distinct.size shouldBe names.size
    names.foreach(n => n should fullyMatch regex "[A-Z][A-Z_]*")
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
