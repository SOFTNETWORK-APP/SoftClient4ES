package app.softnetwork.elastic.client

import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

trait LogVerificationHelper { _: Matchers with MockitoSugar =>
  def captureAndVerifyLog(
    logger: Logger,
    level: String,
    expectedMessages: String*
  )(implicit pos: org.scalactic.source.Position): Unit = {
    import scala.jdk.CollectionConverters._

    val captor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    level match {
      case "info"  => verify(logger, atLeastOnce).info(captor.capture())
      case "error" => verify(logger, atLeastOnce).error(captor.capture())
      case "warn"  => verify(logger, atLeastOnce).warn(captor.capture())
      case "debug" => verify(logger, atLeastOnce).debug(captor.capture())
    }

    val allMessages = captor.getAllValues.asScala.mkString("\n")

    expectedMessages.foreach { expected =>
      withClue(s"Expected message not found: '$expected'\nAll messages:\n$allMessages\n") {
        allMessages should include(expected)
      }
    }
  }
}
