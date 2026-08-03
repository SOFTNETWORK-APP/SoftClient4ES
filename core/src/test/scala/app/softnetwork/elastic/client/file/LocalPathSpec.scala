package app.softnetwork.elastic.client.file

import app.softnetwork.elastic.sql.query.{Delta, Json, JsonArray, Unknown}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

// `OptionValues` is what supplies `.value` on an Option — without it this file does not compile.
class LocalPathSpec extends AnyWordSpec with Matchers with OptionValues {

  private def str(p: java.nio.file.Path): String = p.toString.replace('\\', '/')

  "LocalPath.resolve" should {

    "reject blank input" in {
      LocalPath.resolve(null) shouldBe None
      LocalPath.resolve("") shouldBe None
      LocalPath.resolve("   ") shouldBe None
    }

    "accept a schemeless absolute path" in {
      str(LocalPath.resolve("/Users/me/data/customers.jsonl").value) shouldBe
      "/Users/me/data/customers.jsonl"
    }

    "accept schemeless relative paths" in {
      str(LocalPath.resolve("data/customers.jsonl").value) shouldBe "data/customers.jsonl"
      str(LocalPath.resolve("./data/customers.jsonl").value) shouldBe "./data/customers.jsonl"
    }

    "accept a schemeless path containing spaces" in {
      str(LocalPath.resolve("/Users/me/my data/customers.jsonl").value) shouldBe
      "/Users/me/my data/customers.jsonl"
    }

    "accept the three-slash file URI form" in {
      str(LocalPath.resolve("file:///Users/me/data/customers.jsonl").value) shouldBe
      "/Users/me/data/customers.jsonl"
    }

    "accept the single-slash file URI form" in {
      str(LocalPath.resolve("file:/Users/me/data/customers.jsonl").value) shouldBe
      "/Users/me/data/customers.jsonl"
    }

    "accept a localhost authority" in {
      str(LocalPath.resolve("file://localhost/Users/me/data/customers.jsonl").value) shouldBe
      "/Users/me/data/customers.jsonl"
    }

    "defer a non-local authority to Hadoop" in {
      // `file://Users/...` parses `Users` as the authority — NOT this machine.
      LocalPath.resolve("file://Users/me/data/customers.jsonl") shouldBe None
    }

    "accept a file URI containing an unencoded space" in {
      // `new URI` throws here; the literal fallback must keep the space verbatim.
      str(LocalPath.resolve("file:///Users/me/my data/customers.jsonl").value) shouldBe
      "/Users/me/my data/customers.jsonl"
    }

    "percent-decode a file URI" in {
      str(LocalPath.resolve("file:///Users/me/my%20data/customers.jsonl").value) shouldBe
      "/Users/me/my data/customers.jsonl"
    }

    "percent-decode non-ASCII characters as UTF-8" in {
      // Unicode ESCAPES, not literal accents: scalacCompilerOptions (build.sbt:13-17) is
      // Seq("-deprecation", "-feature", "-target:jvm-1.8") — there is NO `-encoding UTF-8`, so
      // source decoding follows the platform default charset. A literal "été" here would make the
      // assertion machine-dependent.
      str(LocalPath.resolve("file:///Users/me/data/%C3%A9t%C3%A9.jsonl").value) shouldBe
      "/Users/me/data/\u00e9t\u00e9.jsonl"
    }

    "not turn '+' into a space (URLDecoder trap)" in {
      str(LocalPath.resolve("file:///Users/me/data/my+file.jsonl").value) shouldBe
      "/Users/me/data/my+file.jsonl"
    }

    "treat a Windows drive letter as a path, not a scheme" in {
      str(LocalPath.resolve("""C:\data\customers.jsonl""").value) should startWith("C:")
      str(LocalPath.resolve("C:/data/customers.jsonl").value) should startWith("C:")
    }

    "resolve a Windows file URI according to the host platform" in {
      // The drive-slash strip is platform-gated (see stripDriveSlash), so assert what THIS OS does.
      // The platform-independent assertion lives in the "strip a Windows drive slash" test below.
      val resolved = str(LocalPath.resolve("file:///C:/data/customers.jsonl").value)
      if (LocalPath.onWindows) resolved should startWith("C:")
      else resolved shouldBe "/C:/data/customers.jsonl"
    }

    "not expand a tilde" in {
      str(LocalPath.resolve("~/data/customers.jsonl").value) shouldBe "~/data/customers.jsonl"
    }

    "accept an uppercase FILE scheme" in {
      str(LocalPath.resolve("FILE:///Users/me/data/customers.jsonl").value) shouldBe
      "/Users/me/data/customers.jsonl"
    }

    "reject a file URI with no path" in {
      LocalPath.resolve("file:") shouldBe None
      LocalPath.resolve("file://") shouldBe None
    }

    "handle an opaque file URI via the literal fallback" in {
      // `new URI` SUCCEEDS here but getPath is null — the guard must route to splitLiteral.
      str(LocalPath.resolve("file:relative/x.jsonl").value) shouldBe "relative/x.jsonl"
    }

    "keep '?' and '#' as file name characters, exactly like Hadoop" in {
      // Verified against hadoop-common 3.4.2: new Path("file:///a/report#1.jsonl").toUri.getPath
      // is "/a/report#1.jsonl". Taking URI.getPath here would read "/a/report" — the WRONG FILE.
      str(LocalPath.resolve("file:///a/report#1.jsonl").value) shouldBe "/a/report#1.jsonl"
      str(LocalPath.resolve("file:///a/b.jsonl?x=1").value) shouldBe "/a/b.jsonl?x=1"
      str(LocalPath.resolve("/Users/me/data/report#1.jsonl").value) shouldBe
      "/Users/me/data/report#1.jsonl"
    }

    "NOT trim surrounding whitespace" in {
      // Hadoop preserves it (new Path("/tmp/trailing ").toUri.getPath == "/tmp/trailing ").
      // Trimming would silently address a different file.
      str(LocalPath.resolve("/tmp/trailing ").value) shouldBe "/tmp/trailing "
      str(LocalPath.resolve("  /Users/me/x.jsonl").value) shouldBe "  /Users/me/x.jsonl"
    }

    "defer every remote scheme to Hadoop" in {
      LocalPath.resolve("s3a://bucket/customers.jsonl") shouldBe None
      LocalPath.resolve("s3://bucket/customers.jsonl") shouldBe None
      LocalPath.resolve("gs://bucket/customers.jsonl") shouldBe None
      LocalPath.resolve("hdfs://namenode:8020/customers.jsonl") shouldBe None
      LocalPath.resolve("abfss://c@a.dfs.core.windows.net/customers.jsonl") shouldBe None
      LocalPath.resolve("wasbs://c@a.blob.core.windows.net/customers.jsonl") shouldBe None
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Issue #183 regression guard.
  //
  // A Configuration whose `fs.file.impl` points at a class that does not exist makes any Hadoop
  // FileSystem resolution for the `file` scheme blow up with
  //   RuntimeException: java.lang.ClassNotFoundException: Class does.not.Exist not found
  // So: if these tests pass, the local fast path was taken and Hadoop was never consulted — proven
  // on every JDK, including the JDK 11 that CI runs today. This is what stops the bug coming back.
  //
  // BOTH properties are load-bearing — `fs.file.impl` alone is NOT enough.
  // `FileSystem$Cache$Key` is (scheme, authority, ugi, unique); the Configuration is NOT part of
  // the key (verified: `javap -c` on hadoop-common 3.4.2, `FileSystem$Cache$Key.<init>`). Once ANY
  // code in this JVM has resolved a `file`-scheme FileSystem with a working Configuration, the
  // cache hands the same LocalFileSystem back and `fs.file.impl` is never read again — the guard
  // silently becomes vacuous. `core` tests run UNFORKED in one JVM by default, and
  // `FileSourceSpec`'s Parquet + Delta tests populate that cache.
  // `fs.file.impl.disable.cache = true` makes `FileSystem.get(uri, conf)` call `createFileSystem`
  // BEFORE consulting the cache (verified in the same disassembly: the `fs.%s.impl.disable.cache`
  // branch precedes `CACHE.get`), so the poisoned class name is always resolved and always throws.
  // ---------------------------------------------------------------------------------------------
  "the local fast path" should {

    def poisoned: Configuration = {
      val conf = new Configuration()
      conf.set("fs.file.impl", "does.not.Exist")
      conf.setBoolean("fs.file.impl.disable.cache", true) // see the note above — do not remove
      conf
    }

    def writeTemp(suffix: String, content: String): File = {
      val f = File.createTempFile("localpath", suffix)
      f.deleteOnExit()
      Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
      f
    }

    // Meta-test: proves the guard itself is ARMED. Without it, every assertion below could pass
    // while Hadoop was happily consulted, and nobody would notice. This test must be RED if the
    // poisoning ever stops working (wrong property name, Hadoop behaviour change, cache hit).
    "actually be able to detect a Hadoop call (guard self-check)" in {
      val f = writeTemp(".jsonl", """{"id":0}""" + "\n")
      val ex = intercept[Exception] {
        HadoopInputFile.fromPath(new Path(f.getAbsolutePath), poisoned).newStream()
      }
      // RuntimeException: java.lang.ClassNotFoundException: Class does.not.Exist not found
      ex.toString should include("does.not.Exist")
    }

    "open a local stream without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".jsonl", """{"id":1}""" + "\n")

      val is = openStream(f.getAbsolutePath)
      try new String(is.readAllBytes(), StandardCharsets.UTF_8) should include("\"id\":1")
      finally is.close()
    }

    "open a file:// URI without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".jsonl", """{"id":2}""" + "\n")

      val is = openStream(f.toPath.toUri.toString)
      try new String(is.readAllBytes(), StandardCharsets.UTF_8) should include("\"id\":2")
      finally is.close()
    }

    "sniff a JSON array without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".json", """[{"id":1},{"id":2}]""")

      FileFormatDetector.detect(f.getAbsolutePath) shouldBe JsonArray
    }

    "sniff JSON lines without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".json", """{"id":1}""" + "\n")

      FileFormatDetector.detect(f.getAbsolutePath) shouldBe Json
    }

    "classify an unknown local extension without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".txt", "not json")

      // Exercises isDeltaTable (site 7): no _delta_log ⇒ Unknown, and NO Hadoop call.
      FileFormatDetector.detect(f.getAbsolutePath) shouldBe Unknown
    }

    "detect a local Delta table directory without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val dir = Files.createTempDirectory("localpath-delta")
      val log = Files.createDirectory(dir.resolve("_delta_log"))
      // Register the PARENT first. java.io.DeleteOnExitHook deletes in REVERSE registration order
      // ("last in, first deleted"), and File.delete() silently fails on a non-empty directory — so
      // the child must be registered LAST to be deleted FIRST, or the temp dir leaks every run.
      dir.toFile.deleteOnExit()
      log.toFile.deleteOnExit()

      FileFormatDetector.detect(dir.toAbsolutePath.toString) shouldBe Delta
    }

    "report a local directory with the historical message" in {
      implicit val conf: Configuration = poisoned
      val dir = Files.createTempDirectory("localpath-dir")
      dir.toFile.deleteOnExit()

      // Covers validateLocalPath's `Path is not a file` branch, which nothing else exercises.
      val ex = intercept[IllegalArgumentException] {
        JsonArrayFileSource.getMetadata(dir.toAbsolutePath.toString)
      }
      ex.getMessage should include("Path is not a file")
    }

    "route a file:// URI with a non-local authority through Hadoop" in {
      // Covers validateHadoopPath, which is otherwise DEAD in the test suite after this change:
      // every other path in core's tests is local. Uses a clean Configuration on purpose.
      //
      // MEASURED on both JDKs, not assumed. The invariant under test is "this input still reaches
      // Hadoop"; WHICH way Hadoop then fails is JDK-dependent, and both ways prove the routing:
      //
      //   JDK <= 22 : IllegalArgumentException
      //               "Wrong FS: file://someotherhost/no-such-183.json, expected: file:///"
      //               — LocalFileSystem.checkPath rejects the foreign authority before stat'ing.
      //               (NOT "does not exist": checkPath runs first.)
      //   JDK >= 23 : UnsupportedOperationException "getSubject is not supported"
      //               — FileSystem.get -> CACHE.get -> new Key -> UGI.getCurrentUser. This suite
      //               is run forked onto JDK 25 by AC 1b, so that branch is really exercised.
      //
      // The local fast path can produce NEITHER string, so either one proves validateHadoopPath
      // ran. Asserting only the JDK-11 message would make this test fail on the very JDK the
      // story exists to support.
      implicit val conf: Configuration = hadoopConfiguration
      val ex = intercept[Exception] {
        JsonArrayFileSource.getMetadata("file://someotherhost/no-such-183.json")
      }
      withClue(s"unexpected failure mode: $ex") {
        ex.toString should (include("Wrong FS") or include("getSubject"))
      }
    }

    "validate a local JSON array file without consulting Hadoop" in {
      implicit val conf: Configuration = poisoned
      val f = writeTemp(".json", """[{"id":1},{"id":2},{"id":3}]""")

      // getMetadata calls validateFile (site 1) then opens the stream (site 5).
      JsonArrayFileSource.getMetadata(f.getAbsolutePath).elementCount shouldBe 3
    }

    "report a missing local file with the historical message" in {
      implicit val conf: Configuration = poisoned
      val missing = Paths.get(System.getProperty("java.io.tmpdir"), "no-such-183.json").toString

      val ex = intercept[IllegalArgumentException] {
        JsonArrayFileSource.getMetadata(missing)
      }
      ex.getMessage should include("does not exist")
      ex.getMessage should include(missing)
    }

    "still route a remote scheme through Hadoop" in {
      // Do NOT call openStream("s3a://…") here. `hadoop-aws` is `% Provided`
      // (core/build.sbt:46) and sbt puts Provided on the TEST classpath, so S3AFileSystem really
      // instantiates, runs the default AWS credential chain (including an IMDS probe at
      // 169.254.169.254) and issues a live request for a bucket named `bucket`. That is a
      // network-dependent unit test that hangs offline and can throw an Error (not an Exception)
      // on SDK skew. Classification is what this test is about, and it is pure:
      LocalPath.resolve("s3a://bucket/customers.jsonl") shouldBe None
    }
  }

  // ---------------------------------------------------------------------------------------------
  // AD-10 — `HadoopConfigurationFactory.forPath` is now rewired onto `LocalPath.scheme`, so there
  // is ONE scheme parser in this package instead of two that disagreed. Pure `Configuration`
  // assertions only: never open a stream or call `FileSystem.get` for a remote scheme here (see
  // the note on "still route a remote scheme through Hadoop" above).
  // ---------------------------------------------------------------------------------------------
  "HadoopConfigurationFactory.forPath" should {
    "route an UPPERCASE remote scheme to its cloud configuration (was: localConf, no credentials)" in {
      HadoopConfigurationFactory.forPath("S3A://bucket/x.jsonl").get("fs.s3a.impl") shouldBe
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    }

    "route a remote scheme whose path contains an unencoded space (was: localConf)" in {
      // `new java.net.URI(...)` throws on this input, which is exactly how the credentials were lost.
      HadoopConfigurationFactory.forPath("s3a://bucket/my file.jsonl").get("fs.s3a.impl") shouldBe
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    }

    "still treat file: and schemeless paths as local" in {
      HadoopConfigurationFactory.forPath("/tmp/x.jsonl").get("fs.file.impl") shouldBe
      "org.apache.hadoop.fs.LocalFileSystem"
      HadoopConfigurationFactory.forPath("file:///tmp/x.jsonl").get("fs.file.impl") shouldBe
      "org.apache.hadoop.fs.LocalFileSystem"
    }

    "still treat a Windows drive letter as local, not as scheme 'C'" in {
      HadoopConfigurationFactory.forPath("C:/data/x.jsonl").get("fs.file.impl") shouldBe
      "org.apache.hadoop.fs.LocalFileSystem"
    }
  }

  "LocalPath.scheme" should {
    "lowercase, and refuse a one-character prefix" in {
      LocalPath.scheme("S3A://b/x") shouldBe Some("s3a")
      LocalPath.scheme("FILE:///tmp/x") shouldBe Some("file")
      LocalPath.scheme("s3a://b/my file.jsonl") shouldBe Some("s3a") // `new URI` would throw here
      LocalPath.scheme("C:/data/x.jsonl") shouldBe None
      LocalPath.scheme("/tmp/x.jsonl") shouldBe None
      LocalPath.scheme(null) shouldBe None
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Differential parity — the ONLY assertion that pins "bit-identical to Hadoop on JDK ≤ 22",
  // which AD-6, AD-8 and the regression-risk table all claim. Runs on the default JDK 11, where
  // Hadoop still works, so it is a real comparison and not a restatement of the new code.
  // Deliberately NOT run under -Dtest.jdk.home=<24|25>: there Hadoop cannot answer at all.
  // ---------------------------------------------------------------------------------------------
  "LocalPath.resolve" should {
    "agree with org.apache.hadoop.fs.Path on every local form" in {
      val cases = Seq(
        "/Users/me/data/customers.jsonl",
        "/Users/me/my data/customers.jsonl",
        "/Users/me/data/report#1.jsonl",
        "/Users/me/data/a?b.jsonl",
        "/tmp/trailing ",
        "data/customers.jsonl",
        "./data/customers.jsonl",
        "~/data/customers.jsonl",
        "file:///Users/me/data/customers.jsonl",
        "file:/Users/me/data/customers.jsonl",
        "file://localhost/Users/me/data/customers.jsonl",
        "file:///Users/me/my data/customers.jsonl",
        "file:///a/report#1.jsonl",
        "file:///a/b.jsonl?x=1",
        "file:///Users/me/data/my+file.jsonl"
      )
      cases.foreach { s =>
        withClue(s"input [$s]: ") {
          // `.normalize()` on BOTH sides: Hadoop's Path collapses a leading "./" while
          // java.nio.file.Paths keeps it. `./data/x.jsonl` and `data/x.jsonl` denote the same file
          // against the same working directory, so that difference is cosmetic — this comparison
          // asserts "same file", which is the property AD-6/AD-8 actually claim.
          // Measured: 15 inputs, 14 byte-identical, 1 ("./data/customers.jsonl") equal only after
          // normalize. If a SECOND input ever needs normalize to agree, investigate — do not widen.
          val mine = LocalPath.resolve(s).map(p => Paths.get(p.toString).normalize.toString)
          val hadoop = Paths.get(new Path(s).toUri.getPath).normalize.toString
          mine shouldBe Some(hadoop)
        }
      }
    }

    "diverge from Hadoop ONLY on percent-decoding, and only in the user's favour" in {
      // java.io.File.toURI emits this shape; Hadoop re-encodes the '%' and looks for a directory
      // literally named "my%20data", which is why that form has ALWAYS failed. We decode it.
      val encoded = "file:///Users/me/my%20data/customers.jsonl"
      new Path(encoded).toUri.getPath shouldBe "/Users/me/my%20data/customers.jsonl"
      str(LocalPath.resolve(encoded).value) shouldBe "/Users/me/my data/customers.jsonl"
    }

    "strip a Windows drive slash only on Windows" in {
      LocalPath.stripDriveSlash("/C:/data/x.jsonl", windows = true) shouldBe "C:/data/x.jsonl"
      LocalPath.stripDriveSlash("/C:/data/x.jsonl", windows = false) shouldBe "/C:/data/x.jsonl"
      LocalPath.stripDriveSlash("/Users/me/x.jsonl", windows = true) shouldBe "/Users/me/x.jsonl"
    }
  }
}
