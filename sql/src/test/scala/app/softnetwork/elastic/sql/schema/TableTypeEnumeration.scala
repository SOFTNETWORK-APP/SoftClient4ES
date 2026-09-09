package app.softnetwork.elastic.sql.schema

import org.scalatest.Assertions

import java.io.File
import java.lang.reflect.Modifier
import java.net.URL

/** The ONE enumeration of `TableType`, shared by every spec that must not miss a type.
  *
  * Story BIDC-10a Part D. `TableType` is `sealed`, so the compiler guarantees every subtype is
  * declared in the same file and compiled into the same package directory; listing that directory
  * is therefore COMPLETE and needs no allow-list. A spec that hand-lists the types instead stays
  * green when a seventh is added, silently under-covering the very thing it guards - which is why
  * this lives here rather than being written twice.
  *
  * It is reachable from `core` because `core -> macros -> sql` carries `test->test`, so
  * `sql/target/scala-2.13/test-classes` is on core's test classpath.
  *
  * 🔴 Every way out of this walk FAILS; none skips.
  *
  *   1. `getResources`, not `getResource`. A spec living in the SAME package as `TableType` puts a
  *      `test-classes/app/.../schema` directory on the classpath ahead of the main one, and the
  *      singular lookup then returns the test tree, which holds no `TableType` at all. The walk
  *      returns EMPTY and every per-type assertion downstream passes VACUOUSLY. Measured, on the
  *      first version of `TableTypeVocabularySpec`. Union every root. 2. A non-`file:` root fails
  *      rather than being skipped. It is not hypothetical: running these assertions against a
  *      PUBLISHED `softclient4es-sql` jar (an overlay run, or a spec moved to a module that
  *      consumes the artifact rather than the project) makes the package resolve to a `jar:` URL.
  *      Better a loud "teach me to read the archive" than a guard that quietly stops guarding. 3. A
  *      `.class` in the package that cannot be loaded fails. A swallowed `ClassNotFoundException`
  *      here would mean a `TableType` whose client vocabulary was never decided, which is the whole
  *      defect this story closes. 4. A concrete subtype that is not a Scala `object` fails. 5. An
  *      empty result fails HERE, inside the enumerator, so that no caller can be vacuous even if it
  *      forgets to check.
  */
object TableTypeEnumeration extends Assertions {

  def declaredTableTypes: Seq[TableType] = {
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
      .map { n =>
        try Class.forName(n, false, loader)
        catch {
          case t: Throwable =>
            fail(
              s"`$n` is a compiled class in the schema package and could not be loaded " +
              s"(${t.getClass.getSimpleName}: ${t.getMessage}). This walk must not skip a class " +
              "silently - a skipped TableType is a client vocabulary nobody decided.",
              t
            )
        }
      }
      .filter(c => classOf[TableType].isAssignableFrom(c))
      .filterNot(c => c.isInterface || Modifier.isAbstract(c.getModifiers))
      .distinct

    if (subtypes.isEmpty)
      fail(
        s"no concrete TableType was found under $roots. Every assertion built on this walk " +
        "iterates its result, so an empty one would make them all pass vacuously."
      )

    subtypes.map { c =>
      val module =
        try Class.forName(c.getName, true, loader).getField("MODULE$").get(null)
        catch {
          case t: Throwable =>
            fail(
              s"`${c.getName}` is a concrete TableType that is not a Scala `object`; this walk " +
              "can only instantiate case objects. Teach it, or the vocabulary guard silently " +
              "loses a type.",
              t
            )
        }
      module.asInstanceOf[TableType]
    }
  }
}
