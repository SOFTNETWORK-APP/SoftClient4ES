package app.softnetwork.elastic.client.file

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.time.{Seconds, Span}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{AddFile, Metadata}
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}

import java.io.File
import java.nio.file.Files
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class FileSourceSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("FileSourceTest")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val conf: Configuration = hadoopConfiguration
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

    "read JSON Array file with nested arrays and objects" in {
      val tempFile = File.createTempFile("test-nested", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""[
                       |  { "uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0 },
                       |  { "uuid": "A14", "name": "Moe Szyslak", "birthDate": "1967-11-21", "childrenCount": 0 },
                       |  { "uuid": "A16", "name": "Barney Gumble2", "birthDate": "1969-05-09",
                       |    "children": [
                       |      { "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09" },
                       |      { "parentId": "A16", "name": "Josh Gumble", "birthDate": "2002-05-09" }
                       |    ],
                       |    "childrenCount": 2
                       |  }
                       |]""".stripMargin)
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = JsonArray)
        .runWith(Sink.seq)
        .futureValue

      result should have size 3

      // Verify first element (no children)
      result.head should include("Homer Simpson")
      result.head should include("A12")
      result.head should include("childrenCount")

      // Verify second element (no children)
      result(1) should include("Moe Szyslak")
      result(1) should include("A14")

      // Verify third element (with nested children array)
      val thirdElement = result(2)
      thirdElement should include("Barney Gumble2")
      thirdElement should include("A16")
      thirdElement should include("children")
      thirdElement should include("Steve Gumble")
      thirdElement should include("Josh Gumble")
      thirdElement should include("parentId")

      // Parse and validate nested structure

      val mapper = JacksonConfig.objectMapper

      val jsonNode = mapper.readTree(thirdElement)
      jsonNode.get("uuid").asText() shouldBe "A16"
      jsonNode.get("childrenCount").asInt() shouldBe 2

      val children = jsonNode.get("children")
      children.isArray shouldBe true
      children.size() shouldBe 2

      children.get(0).get("name").asText() shouldBe "Steve Gumble"
      children.get(0).get("parentId").asText() shouldBe "A16"

      children.get(1).get("name").asText() shouldBe "Josh Gumble"
      children.get(1).get("parentId").asText() shouldBe "A16"
    }

    "handle JSON Array with empty nested arrays" in {
      val tempFile = File.createTempFile("test-empty-nested", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""[
                       |  { "uuid": "A12", "name": "Test", "children": [] }
                       |]""".stripMargin)
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = JsonArray)
        .runWith(Sink.seq)
        .futureValue

      result should have size 1
      result.head should include("children")
      result.head should include("[]")
    }

    "handle deeply nested JSON structures" in {
      val tempFile = File.createTempFile("test-deep-nested", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""[
                       |  {
                       |    "level1": {
                       |      "level2": {
                       |        "level3": {
                       |          "data": "deep value",
                       |          "array": [1, 2, 3]
                       |        }
                       |      }
                       |    }
                       |  }
                       |]""".stripMargin)
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = JsonArray)
        .runWith(Sink.seq)
        .futureValue

      result should have size 1
      result.head should include("deep value")
      result.head should include("level1")
      result.head should include("level2")
      result.head should include("level3")
    }

    "read Parquet file" in {
      // Create temporary Parquet file
      val tempDir = Files.createTempDirectory("parquet-test").toFile
      tempDir.deleteOnExit()
      val parquetFile = new File(tempDir, "test.parquet")
      parquetFile.deleteOnExit()

      // Define schema
      val schemaString =
        """message TestRecord {
          |  required int32 id;
          |  required binary name (UTF8);
          |  required int32 age;
          |}""".stripMargin

      val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
      val groupFactory = new SimpleGroupFactory(schema)

      // Write Parquet file
      val path = new Path(parquetFile.getAbsolutePath)
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(path)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(conf)
        .build()

      try {
        // Write records
        val record1 = groupFactory.newGroup()
        record1.add("id", 1)
        record1.add("name", "Alice")
        record1.add("age", 30)
        writer.write(record1)

        val record2 = groupFactory.newGroup()
        record2.add("id", 2)
        record2.add("name", "Bob")
        record2.add("age", 25)
        writer.write(record2)

        val record3 = groupFactory.newGroup()
        record3.add("id", 3)
        record3.add("name", "Charlie")
        record3.add("age", 35)
        writer.write(record3)
      } finally {
        writer.close()
      }

      // Read Parquet file
      val result = FileSourceFactory
        .fromFile(parquetFile.getAbsolutePath, format = Parquet)
        .runWith(Sink.seq)
        .futureValue

      result should have size 3
      result.head should include("Alice")
      result(1) should include("Bob")
      result.last should include("Charlie")

      // Verify JSON structure
      result.foreach { json =>
        json should include("id")
        json should include("name")
        json should include("age")
      }
    }

    "read Parquet file with nested structures" in {
      val tempDir = Files.createTempDirectory("parquet-nested-test").toFile
      tempDir.deleteOnExit()
      val parquetFile = new File(tempDir, "nested.parquet")
      parquetFile.deleteOnExit()

      val schemaString =
        """message NestedRecord {
          |  required int32 id;
          |  required group person {
          |    required binary name (UTF8);
          |    required int32 age;
          |  }
          |  required group address {
          |    required binary city (UTF8);
          |    required binary country (UTF8);
          |  }
          |}""".stripMargin

      val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
      val groupFactory = new SimpleGroupFactory(schema)

      val path = new Path(parquetFile.getAbsolutePath)
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(path)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(conf)
        .build()

      try {
        val record = groupFactory.newGroup()
        record.add("id", 1)

        val person = record.addGroup("person")
        person.add("name", "Alice")
        person.add("age", 30)

        val address = record.addGroup("address")
        address.add("city", "Paris")
        address.add("country", "France")

        writer.write(record)
      } finally {
        writer.close()
      }

      val result = FileSourceFactory
        .fromFile(parquetFile.getAbsolutePath, format = Parquet)
        .runWith(Sink.seq)
        .futureValue

      result should have size 1
      val json = result.head
      json should include("Alice")
      json should include("Paris")
      json should include("France")
    }

    "get Parquet file metadata" in {
      val tempDir = Files.createTempDirectory("parquet-meta-test").toFile
      tempDir.deleteOnExit()
      val parquetFile = new File(tempDir, "meta.parquet")
      parquetFile.deleteOnExit()

      val schemaString =
        """message TestRecord {
          |  required int32 id;
          |  required binary name (UTF8);
          |}""".stripMargin

      val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
      val groupFactory = new SimpleGroupFactory(schema)

      val path = new Path(parquetFile.getAbsolutePath)
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(path)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(conf)
        .build()

      try {
        (1 to 100).foreach { i =>
          val record = groupFactory.newGroup()
          record.add("id", i)
          record.add("name", s"Person$i")
          writer.write(record)
        }
      } finally {
        writer.close()
      }

      val metadata = ParquetFileSource.getFileMetadata(parquetFile.getAbsolutePath)

      metadata.numRowGroups should be > 0
      metadata.numRows shouldBe 100
      metadata.schema should include("id")
      metadata.schema should include("name")
    }

    "read Delta Lake table" in {
      val tempDir = Files.createTempDirectory("delta-test").toFile
      tempDir.deleteOnExit()

      val deltaLog = DeltaLog.forTable(conf, tempDir.getAbsolutePath)

      // Define Delta Standalone StructType schema
      val schema = new StructType(
        Array(
          new StructField("id", new IntegerType(), false),
          new StructField("name", new StringType(), false),
          new StructField("age", new IntegerType(), false)
        )
      )

      // Create metadata with Delta Standalone StructType
      val metadata = Metadata
        .builder()
        .schema(schema)
        .build()

      // Write Parquet data file
      val dataFile = new File(tempDir, "part-00000.parquet")

      val parquetSchema = MessageTypeParser.parseMessageType(
        """message TestRecord {
          |  required int32 id;
          |  required binary name (UTF8);
          |  required int32 age;
          |}""".stripMargin
      )

      val groupFactory = new SimpleGroupFactory(parquetSchema)
      val path = new Path(dataFile.getAbsolutePath)
      val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
        .builder(path)
        .withType(parquetSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withConf(conf)
        .build()

      try {
        val record1 = groupFactory.newGroup()
        record1.add("id", 1)
        record1.add("name", "Alice")
        record1.add("age", 30)
        writer.write(record1)

        val record2 = groupFactory.newGroup()
        record2.add("id", 2)
        record2.add("name", "Bob")
        record2.add("age", 25)
        writer.write(record2)
      } finally {
        writer.close()
      }

      // Create AddFile action - relative path from the table root
      val addFile = AddFile
        .builder(
          dataFile.getName,
          Map.empty[String, String].asJava,
          dataFile.length(),
          System.currentTimeMillis(),
          true
        )
        .build()

      // Create Operation
      val operation = new Operation(Operation.Name.WRITE)

      // Commit to Delta log
      val txn = deltaLog.startTransaction()
      txn.commit(List(metadata, addFile).asJava, operation, "0")

      // Read Delta table
      val result = FileSourceFactory
        .fromFile(tempDir.getAbsolutePath, format = Delta)
        .runWith(Sink.seq)
        .futureValue

      result should have size 2
      result.head should include("Alice")
      result.last should include("Bob")

      result.foreach { json =>
        json should include("id")
        json should include("name")
        json should include("age")
      }
    }

    "read Delta table with time travel" in {
      val tempDir = Files.createTempDirectory("delta-timetravel-test").toFile
      tempDir.deleteOnExit()

      val deltaLog = DeltaLog.forTable(conf, tempDir.getAbsolutePath)

      // Define Delta Standalone StructType schema
      val schema = new StructType(
        Array(
          new StructField("id", new IntegerType(), false),
          new StructField("value", new StringType(), false)
        )
      )

      val metadata = Metadata
        .builder()
        .schema(schema)
        .build()

      // Version 0: Initial data
      val dataFile1 = createDeltaDataFile(tempDir, "v0.parquet", List((1, "v0_data")))
      val addFile1 = createAddFile(dataFile1)

      val operation1 = new Operation(Operation.Name.WRITE)
      val txn1 = deltaLog.startTransaction()
      txn1.commit(List(metadata, addFile1).asJava, operation1, "0")

      // Version 1: Add more data
      val dataFile2 = createDeltaDataFile(tempDir, "v1.parquet", List((2, "v1_data")))
      val addFile2 = createAddFile(dataFile2)

      val operation2 = new Operation(Operation.Name.WRITE)
      val txn2 = deltaLog.startTransaction()
      txn2.commit(List(addFile2).asJava, operation2, "1")

      // Read version 0
      val resultV0 = DeltaFileSource
        .fromFileAtVersion(tempDir.getAbsolutePath, version = 0)
        .runWith(Sink.seq)
        .futureValue

      resultV0 should have size 1
      resultV0.head should include("v0_data")

      // Read version 1 (latest)
      val resultV1 = FileSourceFactory
        .fromFile(tempDir.getAbsolutePath, format = Delta)
        .runWith(Sink.seq)
        .futureValue

      resultV1 should have size 2
    }

    "get Delta table info" in {
      val tempDir = Files.createTempDirectory("delta-info-test").toFile
      tempDir.deleteOnExit()

      val deltaLog = DeltaLog.forTable(conf, tempDir.getAbsolutePath)

      // Define Delta Standalone StructType schema
      val schema = new StructType(
        Array(
          new StructField("id", new IntegerType(), false)
        )
      )

      val metadata = Metadata
        .builder()
        .schema(schema)
        .description("Test table")
        .build()

      val operation = new Operation(Operation.Name.CREATE_TABLE)
      val txn = deltaLog.startTransaction()
      txn.commit(List(metadata).asJava, operation, "0")

      val info = DeltaFileSource.getTableInfo(tempDir.getAbsolutePath)

      info.version shouldBe 0
      info.description shouldBe Some("Test table")
      info.schema should include("id")
    }

    "read Delta table with partitions" in {
      val tempDir = Files.createTempDirectory("delta-partition-test").toFile
      tempDir.deleteOnExit()

      val deltaLog = DeltaLog.forTable(conf, tempDir.getAbsolutePath)

      // Define partitioned schema with Delta Standalone types
      val schema = new StructType(
        Array(
          new StructField("id", new IntegerType(), false),
          new StructField("category", new StringType(), false),
          new StructField("value", new StringType(), false)
        )
      )

      val metadata = Metadata
        .builder()
        .schema(schema)
        .partitionColumns(List("category").asJava)
        .build()

      // Create data files for different partitions
      val dataFileA = createDeltaDataFilePartitioned(
        tempDir,
        "category=A/part-00000.parquet",
        List((1, "A", "data_A1"), (2, "A", "data_A2"))
      )

      val dataFileB = createDeltaDataFilePartitioned(
        tempDir,
        "category=B/part-00000.parquet",
        List((3, "B", "data_B1"), (4, "B", "data_B2"))
      )

      val addFileA = AddFile
        .builder(
          "category=A/part-00000.parquet",
          Map("category" -> "A").asJava,
          dataFileA.length(),
          System.currentTimeMillis(),
          true
        )
        .build()

      val addFileB = AddFile
        .builder(
          "category=B/part-00000.parquet",
          Map("category" -> "B").asJava,
          dataFileB.length(),
          System.currentTimeMillis(),
          true
        )
        .build()

      val operation = new Operation(Operation.Name.WRITE)
      val txn = deltaLog.startTransaction()
      txn.commit(List(metadata, addFileA, addFileB).asJava, operation, "0")

      // Read all data
      val resultAll = FileSourceFactory
        .fromFile(tempDir.getAbsolutePath, format = Delta)
        .runWith(Sink.seq)
        .futureValue

      resultAll should have size 4
    }

    "handle non-existent file" in {
      val result = FileSourceFactory
        .fromFile("/non/existent/file.json", format = Json)
        .runWith(Sink.seq)

      whenReady(result.failed) { ex =>
        ex shouldBe a[IllegalArgumentException]
        ex.getMessage should include("does not exist")
      }
    }

    "handle empty file" in {
      val tempFile = java.io.File.createTempFile("test", ".json")
      tempFile.deleteOnExit()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = Json)
        .runWith(Sink.seq)
        .futureValue

      result should have size 0
    }

    "handle invalid JSON Array" in {
      val tempFile = File.createTempFile("test", ".json")
      tempFile.deleteOnExit()

      val writer = new java.io.PrintWriter(tempFile)
      writer.println("""{"not":"an array"}""")
      writer.close()

      val result = FileSourceFactory
        .fromFile(tempFile.getAbsolutePath, format = JsonArray)
        .runWith(Sink.seq)

      whenReady(result.failed) { ex =>
        ex shouldBe a[IllegalArgumentException]
        ex.getMessage should include("JSON array")
      }
    }
  }

  // Helper methods
  private def createDeltaDataFile(
    tempDir: File,
    fileName: String,
    data: List[(Int, String)]
  ): File = {
    val dataDir = new File(tempDir, "data")
    dataDir.mkdirs()
    val dataFile = new File(dataDir, fileName)

    val schema = MessageTypeParser.parseMessageType(
      """message Record {
        |  required int32 id;
        |  required binary value (UTF8);
        |}""".stripMargin
    )

    val groupFactory = new SimpleGroupFactory(schema)
    val path = new Path(dataFile.getAbsolutePath)
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(path)
      .withType(schema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withConf(conf)
      .build()

    try {
      data.foreach { case (id, value) =>
        val record = groupFactory.newGroup()
        record.add("id", id)
        record.add("value", value)
        writer.write(record)
      }
    } finally {
      writer.close()
    }

    dataFile
  }

  private def createDeltaDataFilePartitioned(
    tempDir: File,
    relativePath: String,
    data: List[(Int, String, String)]
  ): File = {
    val dataFile = new File(tempDir, relativePath)
    dataFile.getParentFile.mkdirs()

    val schema = MessageTypeParser.parseMessageType(
      """message Record {
        |  required int32 id;
        |  required binary category (UTF8);
        |  required binary value (UTF8);
        |}""".stripMargin
    )

    val groupFactory = new SimpleGroupFactory(schema)
    val path = new Path(dataFile.getAbsolutePath)
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter
      .builder(path)
      .withType(schema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withConf(conf)
      .build()

    try {
      data.foreach { case (id, category, value) =>
        val record = groupFactory.newGroup()
        record.add("id", id)
        record.add("category", category)
        record.add("value", value)
        writer.write(record)
      }
    } finally {
      writer.close()
    }

    dataFile
  }

  private def createAddFile(dataFile: File): AddFile = {
    AddFile
      .builder(
        s"data/${dataFile.getName}",
        Map.empty[String, String].asJava,
        dataFile.length(),
        System.currentTimeMillis(),
        true
      )
      .build()
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }
}
