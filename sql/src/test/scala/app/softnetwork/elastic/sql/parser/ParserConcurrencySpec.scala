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

package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{URL, URLClassLoader}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable

/** The grammar's productions are `lazy val`s on one singleton (`object Parser`), which is the
  * declaration form `PackratParsers` requires for its memo cache to key on a STABLE parser
  * instance. That swaps per-call construction for shared, lazily-initialised state, and this suite
  * is the guard on the one hazard that swap introduces: several threads reaching an
  * as-yet-uninitialised production at the same time.
  *
  * Two legs, because they fail in different ways:
  *
  *   1. '''COLD''' - `object Parser` is loaded afresh in a child-first classloader, so EVERY
  *      production is uninitialised when N threads hit it simultaneously. This is the leg that
  *      would catch an initialisation deadlock or a half-built parser. It cannot be done against
  *      the ambient `Parser`: sbt runs `sql` tests unforked in one JVM, so by the time any suite
  *      runs the singleton may already be warm. 2. '''WARM''' - the ordinary `Parser`, N threads,
  *      results compared against the single-threaded answers.
  *
  * Both legs bound their wait: a deadlock must fail the suite, not hang the build forever.
  *
  * The structural argument this checks, for the record: a `lazy val` on an object initialises under
  * that object's own monitor, so every production shares ONE lock and no lock-ordering inversion is
  * possible between them; and because `PackratParsers.parser2packrat` takes its argument BY NAME
  * (`p: => Parser[T]`, held in a local `lazy val`), a production's body is not evaluated when the
  * production initialises - only when it is first APPLIED. Initialisation therefore never re-enters
  * the grammar, which is what keeps the mutually recursive productions (`criteria` -> `predicate`
  * -> `criteria`) safe as `lazy val`s.
  */
class ParserConcurrencySpec extends AnyFlatSpec with Matchers {

  /** Deliberately spread across the grammar - DQL, aggregation, window, JOIN, derived table,
    * subquery, DDL, DML, geo, watcher, and a rejection - so that as many distinct productions as
    * possible are initialised concurrently rather than one hot path being warmed by thread 1.
    */
  private val statements: List[String] = List(
    "SELECT a FROM t",
    "SELECT * FROM orders WHERE status = 'OPEN' AND amount BETWEEN 1 AND 10",
    "SELECT country, COUNT(customer_id) AS ct FROM orders GROUP BY country HAVING COUNT(customer_id) > 1",
    "SELECT name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn FROM emp",
    "SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id",
    "SELECT d.cid FROM (SELECT cid FROM customers WHERE region = 'EU') d",
    "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')",
    "CREATE TABLE IF NOT EXISTS users (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))",
    "UPDATE orders SET status = 'CLOSED' WHERE id = 1",
    "DELETE FROM orders WHERE id = 1",
    "SELECT * FROM places WHERE distance(location, POINT(48.8, 2.3)) <= 10 km",
    "SELECT department, STDDEV(salary) AS sd FROM emp GROUP BY department",
    "SELECT MAX(DATE_PARSE(createdAt, '%Y-%m-%d')) AS m FROM t GROUP BY k",
    "SHOW TABLES",
    "SELECT a, FROM WHERE t GROUP 'x' HAVING"
  )

  private val Threads = 8
  private val Rounds = 40
  private val TimeoutSeconds = 120L

  /** Runs `body` on `Threads` threads released together by a latch, bounded by [[TimeoutSeconds]].
    * Returns each thread's per-statement answers. A thread that throws records the throwable's
    * class and message in place of its answer, so an exception is a visible diff rather than a
    * swallowed one.
    */
  private def raced(body: String => String): (List[List[String]], List[Throwable]) = {
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(Threads)
    val answers = Array.fill(Threads)(List.empty[String])
    val errors = mutable.ListBuffer.empty[Throwable]
    val threads = (0 until Threads).map { t =>
      val th = new Thread(
        () => {
          try {
            start.await()
            val acc = mutable.ListBuffer.empty[String]
            for (round <- 0 until Rounds) {
              // Each thread starts at a different offset so the threads are not walking the
              // statement list in lockstep - that is what makes them race on DIFFERENT
              // productions rather than queue on the same one.
              val sql = statements((t + round) % statements.size)
              acc += body(sql)
            }
            answers(t) = acc.toList
          } catch {
            case e: Throwable => errors.synchronized(errors += e)
          } finally done.countDown()
        },
        s"parser-race-$t"
      )
      th.setDaemon(true)
      th.start()
      th
    }
    start.countDown()
    val finished = done.await(TimeoutSeconds, TimeUnit.SECONDS)
    threads.foreach(_.interrupt())
    if (!finished) {
      fail(
        s"$Threads threads did not finish within $TimeoutSeconds s - the grammar's lazy vals " +
        "deadlocked or livelocked under concurrent initialisation"
      )
    }
    (answers.toList, errors.toList)
  }

  private def expectedPerThread(body: String => String): List[List[String]] =
    (0 until Threads).toList.map { t =>
      (0 until Rounds).toList.map(round => body(statements((t + round) % statements.size)))
    }

  "the grammar" should "parse identically on 8 threads racing a COLD object Parser" in {
    val loader = ColdParserLoader.build()
    try {
      val cold = ColdParserLoader.parseFunction(loader)
      val (answers, errors) = raced(cold)
      errors shouldBe empty
      // The single-threaded expectation is computed on the SAME cold loader, after the race, so
      // any answer the race corrupted shows up as a diff rather than being re-derived from it.
      val expected = expectedPerThread(cold)
      answers.zipWithIndex.foreach { case (got, t) =>
        withClue(s"thread $t: ") { got shouldBe expected(t) }
      }
      answers.flatten.count(_.startsWith("Left(")) should be > 0
      answers.flatten.count(_.startsWith("Right(")) should be > 0
    } finally loader.close()
  }

  it should "parse identically on 8 threads racing the ambient object Parser" in {
    val warm: String => String = sql => Parser(sql).toString
    val expected = expectedPerThread(warm)
    val (answers, errors) = raced(warm)
    errors shouldBe empty
    answers.zipWithIndex.foreach { case (got, t) =>
      withClue(s"thread $t: ") { got shouldBe expected(t) }
    }
  }

  it should "agree between the cold and the warm parser" in {
    val loader = ColdParserLoader.build()
    try {
      val cold = ColdParserLoader.parseFunction(loader)
      statements.foreach { sql =>
        withClue(s"[$sql] ") { cold(sql) shouldBe Parser(sql).toString }
      }
    } finally loader.close()
  }
}

/** Loads `app.softnetwork.elastic.sql.**` from a classloader with NO parent, so `object Parser` and
  * every production it holds are initialised from scratch inside this suite.
  *
  * The URL set is derived from the code-source location of a handful of marker classes rather than
  * from `java.class.path`: sbt runs `sql` tests unforked through its own layered classloader, so
  * `java.class.path` is the sbt LAUNCHER's classpath and does not contain the project at all. A
  * marker whose code source cannot be located fails the build loudly, naming the class - this
  * harness must never degrade into "loaded the ambient classes after all", which would make the
  * cold leg pass vacuously.
  */
private[parser] object ColdParserLoader {

  /** Markers whose code source pins one classpath entry each. They are only a FLOOR: the URL set is
    * the union of these and every `URLClassLoader` in the ambient loader chain, because sbt's
    * layered test loaders hold the bulk of the dependency classpath.
    */
  private val markerNames: List[String] = List(
    "app.softnetwork.elastic.sql.parser.Parser",
    "scala.collection.immutable.List",
    "scala.util.parsing.combinator.Parsers",
    "com.typesafe.config.ConfigFactory",
    "org.slf4j.Logger",
    "com.fasterxml.jackson.databind.ObjectMapper",
    "com.fasterxml.jackson.core.JsonFactory",
    "com.fasterxml.jackson.annotation.JsonInclude",
    "com.fasterxml.jackson.module.scala.DefaultScalaModule$"
  )

  def build(): URLClassLoader = {
    val here = getClass.getClassLoader
    val fromChain = Iterator
      .iterate(here)(l => if (l == null) null else l.getParent)
      .takeWhile(_ != null)
      .collect { case u: URLClassLoader => u.getURLs.toList }
      .flatten
      .toList
    val fromMarkers = markerNames.map { n =>
      val c = Class.forName(n, false, here)
      Option(c.getProtectionDomain)
        .flatMap(pd => Option(pd.getCodeSource))
        .flatMap(cs => Option(cs.getLocation))
        .getOrElse(
          throw new IllegalStateException(
            s"ColdParserLoader: cannot locate the code source of $n - the cold leg would " +
            "silently fall back to the ambient classes"
          )
        )
    }
    val urls = (fromMarkers ++ fromChain).distinct
    // parent = null => bootstrap only. Nothing resolves through the ambient app classloader, so
    // `object Parser` here is a genuinely separate, uninitialised singleton.
    new URLClassLoader(urls.toArray, null)
  }

  /** `Parser.apply(String): Either[ParserError, Statement]`, reached reflectively because the cold
    * `Either` is a different `Class` than ours. `toString` is the comparison surface: it renders
    * the whole AST for a success and the parser reason for a rejection, so a verdict change and a
    * render change are both visible.
    */
  def parseFunction(loader: URLClassLoader): String => String = {
    val cls = Class.forName("app.softnetwork.elastic.sql.parser.Parser$", false, loader)
    // Gate integrity: if the class came back from the ambient loader (a parent that was not
    // `null`, a marker that resolved through delegation) the "cold" leg would be re-running the
    // WARM one and could never fail. Assert the separation before anything is parsed.
    if (cls.getClassLoader ne loader) {
      throw new IllegalStateException(
        s"ColdParserLoader: object Parser was loaded by ${cls.getClassLoader} , not by the " +
        "child-first loader - the cold leg would pass vacuously"
      )
    }
    val module = cls.getField("MODULE$").get(null)
    val apply = cls.getMethod("apply", classOf[String])
    sql => String.valueOf(apply.invoke(module, sql))
  }
}
