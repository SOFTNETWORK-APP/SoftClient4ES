package app.softnetwork.elastic.client.repl

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.Properties

/** Unit tests for BundleInfo (REPL.4 / #163 fix 4).
  *
  * The resource presence/absence is injected through a test ClassLoader — no test-scoped resource
  * copy (and especially no `core/src/test/resources/application.conf` — REPL.2 finding).
  */
class BundleInfoSpec extends AnyWordSpec with Matchers {

  private def classLoaderWith(content: Option[String]): ClassLoader =
    new ClassLoader(null) {
      override def getResourceAsStream(name: String): InputStream =
        content match {
          case Some(text) if name == BundleInfo.ResourceName =>
            new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8))
          case _ => null
        }
    }

  private val fullProperties: String =
    """bundle.version=0.20.2
      |engine.version=0.20.1
      |community.extensions.version=0.2.1
      |arrow.extensions.version=0.2.2
      |bundle.git.sha=abcdef1234567890
      |java.floor=11
      |""".stripMargin

  "BundleInfo.load" should {

    "return None when the bundle-info resource is absent (plain installs, sbt runs)" in {
      BundleInfo.load(classLoaderWith(None)) shouldBe None
    }

    "parse a complete bundle-info resource" in {
      val bundle = BundleInfo.load(classLoaderWith(Some(fullProperties)))
      bundle shouldBe defined
      bundle.get.bundleVersion shouldBe "0.20.2"
      bundle.get.engineVersion shouldBe "0.20.1"
      bundle.get.communityExtensionsVersion shouldBe "0.2.1"
      bundle.get.arrowExtensionsVersion shouldBe "0.2.2"
      bundle.get.gitSha shouldBe Some("abcdef1234567890")
      bundle.get.javaFloor shouldBe Some("11")
    }

    "tolerate missing optional keys (git SHA, java floor)" in {
      val minimal =
        """bundle.version=0.20.2
          |engine.version=0.20.1
          |community.extensions.version=0.2.1
          |arrow.extensions.version=0.2.2
          |""".stripMargin
      val bundle = BundleInfo.load(classLoaderWith(Some(minimal)))
      bundle shouldBe defined
      bundle.get.gitSha shouldBe None
      bundle.get.javaFloor shouldBe None
    }

    "return None when a mandatory key is missing" in {
      val missingEngine =
        """bundle.version=0.20.2
          |community.extensions.version=0.2.1
          |arrow.extensions.version=0.2.2
          |""".stripMargin
      BundleInfo.load(classLoaderWith(Some(missingEngine))) shouldBe None
    }

    "return None when a mandatory key is blank" in {
      val blankEngine =
        """bundle.version=0.20.2
          |engine.version=
          |community.extensions.version=0.2.1
          |arrow.extensions.version=0.2.2
          |""".stripMargin
      BundleInfo.load(classLoaderWith(Some(blankEngine))) shouldBe None
    }
  }

  "BundleInfo.parse" should {

    "trim values" in {
      val props = new Properties()
      props.setProperty("bundle.version", " 0.20.2 ")
      props.setProperty("engine.version", "0.20.1")
      props.setProperty("community.extensions.version", "0.2.1")
      props.setProperty("arrow.extensions.version", "0.2.2")
      val bundle = BundleInfo.parse(props)
      bundle shouldBe defined
      bundle.get.bundleVersion shouldBe "0.20.2"
    }
  }

  "BundleInfo.Bundle rendering" should {

    val bundle = BundleInfo.Bundle(
      bundleVersion = "0.20.2",
      engineVersion = "0.20.1",
      communityExtensionsVersion = "0.2.1",
      arrowExtensionsVersion = "0.2.2",
      gitSha = Some("abcdef1"),
      javaFloor = Some("11")
    )

    "produce the spec disclosure line" in {
      bundle.summary shouldBe "Bundle 0.20.2 (engine 0.20.1, community 0.2.1, arrow-ext 0.2.2)"
    }

    "keep the banner line within the 56-char fixed-width box" in {
      bundle.bannerLine.length should be <= 56
      bundle.bannerLine shouldBe "Bundle 0.20.2 (engine 0.20.1, ext 0.2.1 + 0.2.2)"
    }
  }
}
