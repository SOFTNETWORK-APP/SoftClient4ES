package app.softnetwork.elastic.client.file

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext

class FileSourceSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("FileSourceTest")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  "FileFormatDetector" should {
    "detect Parquet files" in {
      FileFormatDetector.detect("data/file.parquet") shouldBe Parquet
      FileFormatDetector.detect("data/file.parq") shouldBe Parquet
    }

    "detect JSON files" in {
      FileFormatDetector.detect("data/file.jsonl") shouldBe Json
      FileFormatDetector.detect("data/file.ndjson") shouldBe Json
    }

    "detect unknown files" in {
      FileFormatDetector.detect("data/file.txt") shouldBe Unknown
    }

    "throw on unknown files with detectOrThrow" in {
      assertThrows[IllegalArgumentException] {
        FileFormatDetector.detectOrThrow("data/file.txt")
      }
    }
  }

  "JsonFileSource" should {
    "read JSON lines file" in {
      // Create a temporary file for testing
      val tempFile = java.io.File.createTempFile("test", ".jsonl")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""{"id":1,"name":"Alice"}""")
      writer.println("""{"id":2,"name":"Bob"}""")
      writer.close()

      val result = JsonFileSource
        .fromFile(tempFile.getAbsolutePath)
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should include("Alice")
      result.last should include("Bob")
    }
  }

  "JsonFileFactory" should {
    "read JSON lines file" in {
      // Create a temporary file for testing
      val tempFile = java.io.File.createTempFile("test", ".jsonl")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""{"id":1,"name":"Alice","nested":{"age":30}}""")
      writer.println("""{"id":2,"name":"Bob","nested":{"age":25}}""")
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath)
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should include("Alice")
      result.last should include("Bob")
    }

    "read JSON Array file (single line)" in {
      val tempFile = java.io.File.createTempFile("test", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println(
        """[{"id":1,"name":"Alice","nested": {"age":30}},{"id":2,"name":"Bob","nested":{"age":25}}]"""
      )
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = JsonArray)
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should include("Alice")
      result.last should include("Bob")
    }

    "read JSON Array file (multi-line)" in {
      val tempFile = java.io.File.createTempFile("test", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("[")
      writer.println("""  {
          "id":1,
          "name":"Alice",
          "nested":{
            "age":30
           }
         },""")
      writer.println("""  {"id":2,"name":"Bob","nested":{"age":25}}""")
      writer.println("]")
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath)
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should include("Alice")
      result.last should include("Bob")
    }
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }
}
