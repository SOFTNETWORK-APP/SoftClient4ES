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

package app.softnetwork.elastic.licensing

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{URL, URLClassLoader}
import java.util.Enumeration

/** Test-only classloader tooling for the #258 isolation specs.
  *
  * The three SPI lookups fixed by #258 are once-per-JVM latches: an object's static initialiser
  * (`ElasticClientFactory`), a `lazy val` (`ExtensionRegistry`) and a CAS-cached strategy
  * (`LicenseRefreshStrategyFactory`). In sbt's shared test JVM dozens of earlier suites fire them
  * under a healthy classloader, so a spec that merely installs an isolating context classloader
  * passes with or without the fix. [[RedefiningClassLoader]] restores FIRST TOUCH by construction:
  * it defines the latch-holding classes afresh from their own bytecode, so the object under test
  * runs its initialiser inside the spec's classloader window, whatever ran before.
  *
  * Lives in the `licensing` test tree because that is the lowest module with a latch; `core`'s
  * specs see it through `core.dependsOn(licensing % "test->test")`.
  */
object ClassLoaderIsolation {

  /** A NON-null loader that sees only the bootstrap classes: no application class and no
    * `META-INF/services` resource. This is the shape of a host-owned context classloader that
    * cannot see our jars (Tableau, plugin containers, app servers). A `null` context classloader is
    * deliberately never used: `ServiceLoader` and Akka both fall back to a healthy loader on
    * `null`, so it does not reproduce the defect.
    */
  def blindLoader(): ClassLoader = new URLClassLoader(Array.empty[URL], null)

  /** Run `body` with `loader` installed as the current thread's context classloader; the previous
    * loader is restored on every exit path.
    */
  def withContextClassLoader[A](loader: ClassLoader)(body: => A): A = {
    val thread = Thread.currentThread()
    val saved = thread.getContextClassLoader
    thread.setContextClassLoader(loader)
    try body
    finally thread.setContextClassLoader(saved)
  }

  /** The Scala `object` named `objectName`, as defined by `loader`, forcing its static initialiser
    * NOW - which is where an object-level `ServiceLoader.load` runs.
    */
  def moduleOf(loader: ClassLoader, objectName: String): AnyRef =
    Class.forName(objectName + "$", true, loader).getField("MODULE$").get(null)

  /** The bytecode of `className` as the loader `from` sees it (its own or its parents'). */
  def classBytes(from: ClassLoader, className: String): Array[Byte] = {
    val path = className.replace('.', '/') + ".class"
    val in = from.getResourceAsStream(path)
    if (in == null) {
      throw new ClassNotFoundException(s"$className: no resource $path is visible from $from")
    }
    try readFully(in)
    finally in.close()
  }

  // InputStream.readAllBytes is JDK 9+; these modules emit JDK 8 bytecode (-target:jvm-1.8).
  private def readFully(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    var read = in.read(buffer)
    while (read >= 0) {
      out.write(buffer, 0, read)
      read = in.read(buffer)
    }
    out.toByteArray
  }

  /** Parent-first loader that DEFINES AFRESH a chosen set of classes from the parent's bytecode and
    * delegates every other class to the parent. A class defined here is a distinct `Class` object
    * with its own static state, so an `object` in the set runs its initialiser again on first use.
    *
    * @param parent
    *   the loader whose classpath supplies every class, including the bytes of the redefined ones
    * @param redefine
    *   fully-qualified class names to define afresh; a name `X` also covers `X$` (the module class
    *   of an `object X`) and every `X$...` inner or synthetic class
    * @param hiddenResources
    *   resource names this loader answers with its OWN roots only, never the parent's - how a spec
    *   makes an SPI interface's loader see no provider registration at all
    * @param extraUrls
    *   additional roots searched by this loader - how a spec serves a synthetic `META-INF/services`
    *   registration that must never reach the real test classpath
    */
  final class RedefiningClassLoader(
    parent: ClassLoader,
    redefine: Seq[String],
    hiddenResources: Set[String],
    extraUrls: Array[URL]
  ) extends URLClassLoader(extraUrls, parent) {

    def redefines(name: String): Boolean =
      redefine.exists(c => name == c || name.startsWith(c + "$"))

    override protected def loadClass(name: String, resolve: Boolean): Class[_] =
      getClassLoadingLock(name).synchronized {
        val loaded = findLoadedClass(name)
        if (loaded != null) loaded
        else if (redefines(name)) {
          val bytes = classBytes(parent, name)
          val defined = defineClass(name, bytes, 0, bytes.length)
          if (resolve) resolveClass(defined)
          defined
        } else super.loadClass(name, resolve)
      }

    override def getResources(name: String): Enumeration[URL] =
      if (hiddenResources.contains(name)) findResources(name) else super.getResources(name)

    override def getResource(name: String): URL =
      if (hiddenResources.contains(name)) findResource(name) else super.getResource(name)
  }
}
