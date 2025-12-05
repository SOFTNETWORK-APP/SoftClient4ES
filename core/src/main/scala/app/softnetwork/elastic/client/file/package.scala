package app.softnetwork.elastic.client

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  JsonNode,
  ObjectMapper,
  SerializationFeature
}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.concurrent.{blocking, ExecutionContext}
import scala.io.{Source => IoSource}
import scala.util.{Failure, Success, Try}

package object file {

  private val logger: Logger = LoggerFactory.getLogger("FileSource")

  sealed trait FileFormat {
    def name: String
  }

  case object Parquet extends FileFormat {
    override def name: String = "Parquet"
  }

  case object Json extends FileFormat {
    override def name: String = "JSON"
  }

  case object Unknown extends FileFormat {
    override def name: String = "Unknown"
  }

  /** Hadoop configuration with optimizations for local file system */
  def hadoopConfiguration: Configuration = {
    val conf = new Configuration()
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.setBoolean("parquet.avro.readInt96AsFixed", true)
    // Optimizations
    conf.setInt("io.file.buffer.size", 65536) // 64KB buffer
    conf.setBoolean("fs.automatic.close", true)
    conf
  }

  /** Jackson ObjectMapper configuration */
  object JacksonConfig {
    lazy val objectMapper: ObjectMapper = {
      val mapper = new ObjectMapper()

      // Scala module for native support of Scala types
      mapper.registerModule(DefaultScalaModule)

      // Java Time module for java.time.Instant, LocalDateTime, etc.
      mapper.registerModule(new JavaTimeModule())

      // Setup for performance
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

      // Ignores null values in serialization
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

      // Optimizations
      mapper.configure(SerializationFeature.INDENT_OUTPUT, false) // No pretty print
      mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false)

      mapper
    }
  }

  /** Base trait for file sources */
  sealed trait FileSource {

    protected def logger: Logger = LoggerFactory.getLogger(getClass)

    def format: FileFormat

    /** Reads a file and returns a Source of JSON strings
      *
      * @param filePath
      *   path to the file
      * @param bufferSize
      *   buffer size for backpressure
      * @param ec
      *   execution context
      * @param conf
      *   Hadoop configuration (optional)
      * @return
      *   Source of JSON strings
      */
    def fromFile(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed]

    lazy val mapper: ObjectMapper = JacksonConfig.objectMapper

    /** Version with Jackson JsonNode (more efficient)
      */
    def fromFileAsJsonNode(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[JsonNode, NotUsed] = {

      fromFile(filePath, bufferSize).map { jsonString =>
        mapper.readTree(jsonString)
      }
    }

    /** Version with JSON validation and error logging
      */
    def fromFileValidated(
      filePath: String,
      bufferSize: Int = 500,
      skipInvalid: Boolean = true
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      var invalidCount = 0L
      var validCount = 0L

      fromFile(filePath, bufferSize).mapConcat { line =>
        Try(mapper.readTree(line)) match {
          case Success(_) =>
            validCount += 1
            if (validCount % 10000 == 0) {
              logger.info(s"✅ Validated $validCount documents (${invalidCount} invalid)")
            }
            List(line)

          case Failure(ex) =>
            invalidCount += 1
            logger.warn(
              s"⚠️  Invalid JSON line skipped (error: ${ex.getMessage}): ${line.take(100)}..."
            )
            if (skipInvalid) List.empty else throw ex
        }
      }
    }

    /** Validates that the file exists and is readable
      */
    protected def validateFile(filePath: String)(implicit conf: Configuration): Unit = {
      val path = new Path(filePath)
      val fs = FileSystem.get(conf)

      if (!fs.exists(path)) {
        throw new IllegalArgumentException(s"File does not exist: $filePath")
      }

      if (!fs.isFile(path)) {
        throw new IllegalArgumentException(s"Path is not a file: $filePath")
      }

      val status = fs.getFileStatus(path)
      if (status.getLen == 0) {
        logger.warn(s"⚠️  File is empty: $filePath")
      }

      logger.info(s"📁 Loading file: $filePath (${status.getLen} bytes)")
    }
  }

  /** Source for Parquet files */
  object ParquetFileSource extends FileSource {

    override def format: FileFormat = Parquet

    /** Creates an Akka Streams source from a Parquet file. Converts each Avro record to JSON
      * String.
      */
    override def fromFile(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {
      fromFileWithCustomMapping(filePath, bufferSize, record => record.toString)
    }

    /** Alternative: Read Parquet with custom Avro to JSON conversion
      */
    def fromFileWithCustomMapping(
      filePath: String,
      bufferSize: Int = 500,
      avroToJson: GenericRecord => String
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {
      // Validate file before processing
      validateFile(filePath)

      var recordCount = 0L
      val startTime = System.currentTimeMillis()

      Source
        .unfoldResource[String, ParquetReader[GenericRecord]](
          // Create: Open the Parquet reader
          create = () => {
            logger.info(s"📂 Opening Parquet file: $filePath")
            Try {
              AvroParquetReader
                .builder[GenericRecord](HadoopInputFile.fromPath(new Path(filePath), conf))
                .withConf(conf)
                .build()
            } match {
              case Success(reader) => reader
              case Failure(ex) =>
                logger.error(s"❌ Failed to open Parquet file: $filePath", ex)
                throw ex
            }
          },

          // Read: Reads the next record and converts it to JSON
          read = reader =>
            blocking {
              Try(Option(reader.read())) match {
                case Success(Some(record)) =>
                  recordCount += 1
                  if (recordCount % 10000 == 0) {
                    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                    val throughput = recordCount / elapsed
                    logger.info(
                      f"📊 Read $recordCount records from Parquet ($throughput%.2f records/sec)"
                    )
                  }
                  Some(avroToJson(record))

                case Success(None) =>
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  logger.info(
                    s"✅ Finished reading Parquet file: $recordCount records in ${elapsed}s"
                  )
                  None

                case Failure(ex) =>
                  logger.error(s"❌ Error reading Parquet record at position $recordCount", ex)
                  None
              }
            },

          // Close: Close the reader properly
          close = reader => {
            Try(reader.close()) match {
              case Success(_) => logger.debug(s"🔒 Closed Parquet reader for: $filePath")
              case Failure(ex) =>
                logger.warn(s"⚠️  Failed to close Parquet reader: ${ex.getMessage}")
            }
          }
        )
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }
  }

  /** Source for JSON files (NDJSON or JSON Lines) */
  object JsonFileSource extends FileSource {

    override def format: FileFormat = Json

    /** Reads a JSON Lines file (one line = one JSON document). Format compatible with Elasticsearch
      * Bulk.
      */
    override def fromFile(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      // Validate file before processing
      validateFile(filePath)

      var lineCount = 0L
      val startTime = System.currentTimeMillis()

      Source
        .unfoldResource[String, BufferedReader](
          // Create: Open the file
          create = () => {
            logger.info(s"📂 Opening JSON file: $filePath")
            Try {
              val is: InputStream = HadoopInputFile.fromPath(new Path(filePath), conf).newStream()
              new BufferedReader(new InputStreamReader(is, "UTF-8"))
            } match {
              case Success(reader) => reader
              case Failure(ex) =>
                logger.error(s"❌ Failed to open JSON file: $filePath", ex)
                throw ex
            }
          },

          // Read: Read the next line
          read = reader => {
            blocking {
              Try(Option(reader.readLine())) match {
                case Success(Some(line)) =>
                  lineCount += 1
                  if (lineCount % 10000 == 0) {
                    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                    val throughput = lineCount / elapsed
                    logger.info(f"📊 Read $lineCount lines from JSON ($throughput%.2f lines/sec)")
                  }
                  val trimmed = line.trim
                  if (trimmed.nonEmpty) Some(trimmed) else None // Skip empty lines

                case Success(None) =>
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  logger.info(s"✅ Finished reading JSON file: $lineCount lines in ${elapsed}s")
                  None

                case Failure(ex) =>
                  logger.error(s"❌ Error reading JSON line at position $lineCount", ex)
                  None
              }
            }
          },

          // Close: Close the reader properly
          close = reader => {
            Try(reader.close()) match {
              case Success(_) => logger.debug(s"🔒 Closed JSON reader for: $filePath")
              case Failure(ex) =>
                logger.warn(s"⚠️  Failed to close JSON reader: ${ex.getMessage}")
            }
          }
        )
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }

    /** Alternative version with scala.io.Source (less control but simpler)
      */
    def fromFileManaged(
      filePath: String,
      bufferSize: Int = 500
    ): Source[String, NotUsed] = {

      logger.info(s"📂 Opening JSON file (managed): $filePath")

      Source
        .fromIterator(() => {
          val source = IoSource.fromFile(filePath, "UTF-8")
          source.getLines().filter(_.trim.nonEmpty)
        })
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }
  }

  /** Automatic file format detection */
  object FileFormatDetector {

    def detect(filePath: String): FileFormat = {
      val lowerPath = filePath.toLowerCase

      if (lowerPath.endsWith(".parquet") || lowerPath.endsWith(".parq")) {
        Parquet
      } else if (
        lowerPath.endsWith(".json") || lowerPath
          .endsWith(".jsonl") || lowerPath.endsWith(".ndjson")
      ) {
        Json
      } else {
        Unknown
      }
    }

    /** Detect with validation
      */
    def detectOrThrow(filePath: String): FileFormat = {
      detect(filePath) match {
        case Unknown =>
          throw new IllegalArgumentException(
            s"Unsupported file format: $filePath. Supported: .parquet, .parq, .json, .jsonl, .ndjson"
          )
        case format => format
      }
    }
  }

  /** Factory to get the appropriate FileSource based on file format
    */
  object FileSourceFactory {

    private def apply(filePath: String): FileSource = {
      FileFormatDetector.detect(filePath) match {
        case Parquet => ParquetFileSource
        case Json    => JsonFileSource
        case Unknown =>
          throw new IllegalArgumentException(
            s"Cannot determine file format for: $filePath. Supported: .parquet, .parq, .json, .jsonl, .ndjson"
          )
      }
    }

    def apply(filePath: String, format: FileFormat): FileSource = {
      format match {
        case Parquet => ParquetFileSource
        case Json    => JsonFileSource
        case Unknown => apply(filePath)
      }
    }

    /** Load file with specific format
      */
    def fromFile(
      filePath: String,
      bufferSize: Int = 500,
      format: FileFormat = Unknown
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {
      val source = apply(filePath, format)
      logger.info(s"📁 Detected format: ${source.format.name}")
      source.fromFile(filePath, bufferSize)
    }

    /** Load file with validation
      */
    def fromFileValidated(
      filePath: String,
      bufferSize: Int = 500,
      skipInvalid: Boolean = true,
      format: FileFormat = Unknown
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {
      val source = apply(filePath, format)
      logger.info(s"📁 Detected format: ${source.format.name}")
      source.fromFileValidated(filePath, bufferSize, skipInvalid)
    }
  }
}
