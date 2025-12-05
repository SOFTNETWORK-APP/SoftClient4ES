package app.softnetwork.elastic.client

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.node.ObjectNode
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
import io.delta.standalone.DeltaLog
import io.delta.standalone.data.{CloseableIterator, RowRecord}
import io.delta.standalone.types._
import org.apache.parquet.io.SeekableInputStream
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.io.{Source => IoSource}
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

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

  case object JsonArray extends FileFormat {
    override def name: String = "JSON Array"
  }

  case object Delta extends FileFormat {
    override def name: String = "Delta Lake"
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
      *
      * @param filePath
      *   Path to validate
      * @param checkIsFile
      *   If true, validates it's a file; if false, validates it's a directory
      * @param conf
      *   Hadoop configuration
      */
    protected def validateFile(
      filePath: String,
      checkIsFile: Boolean = true
    )(implicit conf: Configuration): Unit = {
      val path = new Path(filePath)
      val fs = FileSystem.get(conf)

      if (!fs.exists(path)) {
        throw new IllegalArgumentException(s"File does not exist: $filePath")
      }

      val status = fs.getFileStatus(path)

      if (checkIsFile && !status.isFile) {
        throw new IllegalArgumentException(s"Path is not a file: $filePath")
      }

      if (!checkIsFile && !status.isDirectory) {
        throw new IllegalArgumentException(s"Path is not a directory: $filePath")
      }

      if (checkIsFile && status.getLen == 0) {
        logger.warn(s"⚠️  File is empty: $filePath")
      }

      val pathType = if (checkIsFile) "file" else "directory"
      val sizeInfo = if (checkIsFile) s"(${status.getLen} bytes)" else ""
      logger.info(s"📁 Loading $pathType: $filePath $sizeInfo")
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
      Try(validateFile(filePath)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for Parquet file: $filePath", ex)
          return Source.failed(ex)
      }

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
      Try(validateFile(filePath)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for JSON file: $filePath", ex)
          return Source.failed(ex)
      }

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

  /** Source for JSON Array files (single array containing all documents) Format:
    * [{"id":1},{"id":2},{"id":3}]
    */
  object JsonArrayFileSource extends FileSource {

    override def format: FileFormat = JsonArray

    private val jsonFactory = new JsonFactory()

    override def fromFile(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      Try(validateFile(filePath)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for JSON Array file: $filePath", ex)
          return Source.failed(ex)
      }

      var elementCount = 0L
      val startTime = System.currentTimeMillis()

      Source
        .unfoldResource[String, (InputStream, JsonParser)](
          // Create: Open file via Hadoop and create JSON parser
          create = () => {
            logger.info(s"📂 Opening JSON Array file via Hadoop: $filePath")
            Try {
              val is: SeekableInputStream =
                HadoopInputFile.fromPath(new Path(filePath), conf).newStream()

              // Create Jackson parser on top of Hadoop SeekableInputStream
              val parser = jsonFactory.createParser(is)

              // Expect array start
              val token = parser.nextToken()
              if (token != JsonToken.START_ARRAY) {
                is.close()
                throw new IllegalArgumentException(
                  s"Expected JSON array, but found: ${token}. File: $filePath"
                )
              }

              logger.info(s"📊 Started parsing JSON Array via Hadoop FS")
              (is, parser)
            } match {
              case Success(result) => result
              case Failure(ex) =>
                logger.error(s"❌ Failed to open JSON Array file: $filePath", ex)
                throw ex
            }
          },

          // Read: Parse next element from array
          read = { case (_, parser) =>
            blocking {
              Try {
                val token = parser.nextToken()

                if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                  elementCount += 1

                  if (elementCount % 10000 == 0) {
                    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                    val throughput = elementCount / elapsed
                    logger.info(f"📊 Parsed $elementCount elements ($throughput%.2f elements/sec)")
                  }

                  // Parse current element as JsonNode
                  val node: JsonNode = mapper.readTree(parser)
                  Some(mapper.writeValueAsString(node))

                } else if (token == JsonToken.END_ARRAY || token == null) {
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  logger.info(
                    s"✅ Finished reading JSON Array: $elementCount elements in ${elapsed}s"
                  )
                  None

                } else {
                  // Skip unexpected tokens
                  logger.warn(s"⚠️  Unexpected token in JSON Array: $token")
                  None
                }
              } match {
                case Success(result) => result
                case Failure(ex) =>
                  logger.error(s"❌ Error parsing JSON Array element at position $elementCount", ex)
                  None
              }
            }
          },

          // Close: Close parser and Hadoop input stream
          close = { case (inputStream, parser) =>
            Try {
              parser.close() // This also closes the underlying stream
            } match {
              case Success(_) =>
                logger.debug(s"🔒 Closed JSON Array parser for: $filePath")
              case Failure(ex) =>
                logger.warn(s"⚠️  Failed to close JSON Array parser: ${ex.getMessage}")
            }

            // Ensure Hadoop stream is closed
            Try(inputStream.close()) match {
              case Success(_) =>
                logger.debug(s"🔒 Closed Hadoop input stream for: $filePath")
              case Failure(ex) =>
                logger.warn(s"⚠️  Failed to close Hadoop input stream: ${ex.getMessage}")
            }
          }
        )
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }

    /** Alternative: Load entire array in memory (use for small files only!)
      */
    def fromFileInMemory(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      validateFile(filePath)

      logger.info(s"📂 Loading JSON Array file in memory: $filePath")

      Source
        .future(Future {
          blocking {
            val is: InputStream = HadoopInputFile.fromPath(new Path(filePath), conf).newStream()
            try {
              val arrayNode = mapper.readTree(is)
              if (!arrayNode.isArray) {
                throw new IllegalArgumentException(s"File is not a JSON array: $filePath")
              }

              arrayNode.elements().asScala.map(node => mapper.writeValueAsString(node)).toList
            } finally {
              is.close()
            }
          }
        })
        .mapConcat(identity)
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }
  }

  /** Source for Delta Lake files using Delta Standalone
    */
  object DeltaFileSource extends FileSource {

    override def format: FileFormat = Delta

    override def fromFile(
      filePath: String,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      Try(validateFile(filePath, checkIsFile = false)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for Delta Lake table: $filePath", ex)
          return Source.failed(ex)
      }

      var rowCount = 0L
      val startTime = System.currentTimeMillis()

      Source
        .unfoldResource[String, CloseableIterator[RowRecord]](
          create = () => {
            logger.info(s"📂 Opening Delta Lake table: $filePath")
            Try {
              val deltaLog = DeltaLog.forTable(conf, filePath)
              val snapshot = deltaLog.snapshot()

              logger.info(
                s"📊 Delta table version: ${snapshot.getVersion}, " +
                s"files: ${snapshot.getAllFiles.size()}"
              )

              snapshot.open()
            } match {
              case Success(result) => result
              case Failure(ex) =>
                logger.error(s"❌ Failed to open Delta Lake table: $filePath", ex)
                throw ex
            }
          },
          read = iterator =>
            blocking {
              Try {
                if (iterator.hasNext) {
                  rowCount += 1
                  if (rowCount % 10000 == 0) {
                    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                    val throughput = rowCount / elapsed
                    logger.info(f"📊 Read $rowCount rows from Delta ($throughput%.2f rows/sec)")
                  }

                  val row = iterator.next()
                  Some(rowRecordToJson(row))
                } else {
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  logger.info(s"✅ Finished reading Delta table: $rowCount rows in ${elapsed}s")
                  None
                }
              } match {
                case Success(result) => result
                case Failure(ex) =>
                  logger.error(s"❌ Error reading Delta row at position $rowCount", ex)
                  None
              }
            },
          close = iterator =>
            Try(iterator.close()) match {
              case Success(_) => logger.debug(s"🔒 Closed Delta reader for: $filePath")
              case Failure(ex) =>
                logger.warn(s"⚠️  Failed to close Delta reader: ${ex.getMessage}")
            }
        )
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }

    /** Convert Delta Standalone RowRecord to JSON string Utilise l'API réelle: getList/getMap
      * retournent des java.util collections
      */
    private def rowRecordToJson(row: RowRecord): String = {
      import com.fasterxml.jackson.databind.node.ObjectNode

      val objectNode = mapper.createObjectNode()
      val schema = row.getSchema
      val fields = schema.getFields

      fields.foreach { field =>
        val fieldName = field.getName
        val fieldType = field.getDataType

        if (row.isNullAt(fieldName)) {
          objectNode.putNull(fieldName)
        } else {
          addValueToNode(objectNode, fieldName, row, fieldType)
        }
      }

      mapper.writeValueAsString(objectNode)
    }

    /** Add typed value to Jackson ObjectNode
      */
    private def addValueToNode(
      node: ObjectNode,
      fieldName: String,
      row: RowRecord,
      dataType: DataType
    ): Unit = {

      Try {
        dataType match {
          case _: StringType =>
            node.put(fieldName, row.getString(fieldName))

          case _: IntegerType =>
            node.put(fieldName, row.getInt(fieldName))

          case _: ByteType =>
            node.put(fieldName, row.getByte(fieldName).toInt)

          case _: ShortType =>
            node.put(fieldName, row.getShort(fieldName).toInt)

          case _: LongType =>
            node.put(fieldName, row.getLong(fieldName))

          case _: FloatType =>
            node.put(fieldName, row.getFloat(fieldName))

          case _: DoubleType =>
            node.put(fieldName, row.getDouble(fieldName))

          case _: BooleanType =>
            node.put(fieldName, row.getBoolean(fieldName))

          case _: DecimalType =>
            node.put(fieldName, row.getBigDecimal(fieldName))

          case _: DateType =>
            // Date stocké comme Int (jours depuis epoch)
            val dateValue = row.getInt(fieldName)
            val date = java.time.LocalDate.ofEpochDay(dateValue.toLong)
            node.put(fieldName, date.toString)

          case _: TimestampType =>
            // Timestamp stocké comme Long (microsecondes depuis epoch)
            val timestampMicros = row.getLong(fieldName)
            val instant = java.time.Instant.ofEpochMilli(timestampMicros / 1000)
            node.put(fieldName, instant.toString)

          case arrayType: ArrayType =>
            val arrayNode = node.putArray(fieldName)
            // getList retourne java.util.List[Nothing] - on le traite comme Object
            val list = row.getList(fieldName).asInstanceOf[java.util.List[Any]]

            list.asScala.foreach { element =>
              if (element == null) {
                arrayNode.addNull()
              } else {
                addElementToArray(arrayNode, element, arrayType.getElementType)
              }
            }

          case structType: StructType =>
            val nestedNode = node.putObject(fieldName)
            val nestedRow = row.getRecord(fieldName)
            val nestedFields = structType.getFields

            nestedFields.foreach { nestedField =>
              val nestedFieldName = nestedField.getName
              val nestedFieldType = nestedField.getDataType

              if (nestedRow.isNullAt(nestedFieldName)) {
                nestedNode.putNull(nestedFieldName)
              } else {
                addValueToNode(nestedNode, nestedFieldName, nestedRow, nestedFieldType)
              }
            }

          case mapType: MapType =>
            val mapNode = node.putObject(fieldName)
            // getMap retourne java.util.Map[Nothing, Nothing] - on le traite comme Object
            val map = row.getMap(fieldName).asInstanceOf[java.util.Map[Any, Any]]

            map.asScala.foreach { case (key, value) =>
              val keyStr = key.toString
              if (value == null) {
                mapNode.putNull(keyStr)
              } else {
                addElementToObject(mapNode, keyStr, value, mapType.getValueType)
              }
            }

          case _: BinaryType =>
            val bytes = row.getBinary(fieldName)
            node.put(fieldName, java.util.Base64.getEncoder.encodeToString(bytes))

          case _ =>
            // Fallback: convertir en string
            logger.warn(s"Unsupported data type for field $fieldName: ${dataType.getTypeName}")
            node.put(fieldName, "")
        }
      } match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"Error processing field $fieldName: ${ex.getMessage}", ex)
          node.put(fieldName, "")
      }
    }

    /** Add element to JSON array node
      */
    private def addElementToArray(
      arrayNode: com.fasterxml.jackson.databind.node.ArrayNode,
      element: Any,
      elementType: DataType
    ): Unit = {

      elementType match {
        case _: StringType =>
          arrayNode.add(element.toString)

        case _: IntegerType | _: ByteType | _: ShortType =>
          arrayNode.add(element.asInstanceOf[Number].intValue())

        case _: LongType =>
          arrayNode.add(element.asInstanceOf[Number].longValue())

        case _: FloatType =>
          arrayNode.add(element.asInstanceOf[Number].floatValue())

        case _: DoubleType =>
          arrayNode.add(element.asInstanceOf[Number].doubleValue())

        case _: BooleanType =>
          arrayNode.add(element.asInstanceOf[Boolean])

        case _: DecimalType =>
          arrayNode.add(element.asInstanceOf[java.math.BigDecimal])

        case _: DateType =>
          val days = element.asInstanceOf[Number].intValue()
          val date = java.time.LocalDate.ofEpochDay(days.toLong)
          arrayNode.add(date.toString)

        case _: TimestampType =>
          val micros = element.asInstanceOf[Number].longValue()
          val instant = java.time.Instant.ofEpochMilli(micros / 1000)
          arrayNode.add(instant.toString)

        case structType: StructType =>
          val nestedNode = arrayNode.addObject()
          val nestedRow = element.asInstanceOf[RowRecord]
          val nestedFields = structType.getFields

          nestedFields.foreach { field =>
            val nestedFieldName = field.getName
            if (!nestedRow.isNullAt(nestedFieldName)) {
              addValueToNode(nestedNode, nestedFieldName, nestedRow, field.getDataType)
            } else {
              nestedNode.putNull(nestedFieldName)
            }
          }

        case arrayType: ArrayType =>
          // Array imbriqué
          val nestedArrayNode = arrayNode.addArray()
          val nestedList = element.asInstanceOf[java.util.List[Any]]
          nestedList.asScala.foreach { nestedElement =>
            if (nestedElement == null) {
              nestedArrayNode.addNull()
            } else {
              addElementToArray(nestedArrayNode, nestedElement, arrayType.getElementType)
            }
          }

        case _ =>
          arrayNode.add(element.toString)
      }
    }

    /** Add element to JSON object node
      */
    private def addElementToObject(
      objectNode: com.fasterxml.jackson.databind.node.ObjectNode,
      key: String,
      value: Any,
      valueType: DataType
    ): Unit = {

      valueType match {
        case _: StringType =>
          objectNode.put(key, value.toString)

        case _: IntegerType | _: ByteType | _: ShortType =>
          objectNode.put(key, value.asInstanceOf[Number].intValue())

        case _: LongType =>
          objectNode.put(key, value.asInstanceOf[Number].longValue())

        case _: FloatType =>
          objectNode.put(key, value.asInstanceOf[Number].floatValue())

        case _: DoubleType =>
          objectNode.put(key, value.asInstanceOf[Number].doubleValue())

        case _: BooleanType =>
          objectNode.put(key, value.asInstanceOf[Boolean])

        case _: DecimalType =>
          objectNode.put(key, value.asInstanceOf[java.math.BigDecimal])

        case _ =>
          objectNode.put(key, value.toString)
      }
    }

    /** Read Delta with version (time travel)
      */
    def fromFileAtVersion(
      filePath: String,
      version: Long,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      Try(validateFile(filePath, checkIsFile = false)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for Delta Lake table: $filePath", ex)
          return Source.failed(ex)
      }

      var rowCount = 0L
      val startTime = System.currentTimeMillis()

      Source
        .unfoldResource[String, CloseableIterator[RowRecord]](
          create = () => {
            logger.info(s"📂 Opening Delta Lake table at version $version: $filePath")
            val deltaLog = DeltaLog.forTable(conf, filePath)
            val snapshot = deltaLog.getSnapshotForVersionAsOf(version)

            logger.info(s"📊 Delta table version $version, files: ${snapshot.getAllFiles.size()}")

            snapshot.open()
          },
          read = iterator =>
            blocking {
              Try {
                if (iterator.hasNext) {
                  rowCount += 1
                  if (rowCount % 10000 == 0) {
                    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                    val throughput = rowCount / elapsed
                    logger.info(
                      f"📊 Read $rowCount rows from Delta v$version ($throughput%.2f rows/sec)"
                    )
                  }
                  Some(rowRecordToJson(iterator.next()))
                } else {
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  logger.info(
                    s"✅ Finished reading Delta table v$version: $rowCount rows in ${elapsed}s"
                  )
                  None
                }
              } match {
                case Success(result) => result
                case Failure(ex) =>
                  logger.error(s"❌ Error reading Delta row at position $rowCount", ex)
                  None
              }
            },
          close = iterator => Try(iterator.close())
        )
        .buffer(bufferSize, akka.stream.OverflowStrategy.backpressure)
    }

    /** Read Delta with timestamp (time travel)
      */
    def fromFileAtTimestamp(
      filePath: String,
      timestampMillis: Long,
      bufferSize: Int = 500
    )(implicit
      ec: ExecutionContext,
      conf: Configuration = hadoopConfiguration
    ): Source[String, NotUsed] = {

      Try(validateFile(filePath, checkIsFile = false)) match {
        case Success(_) => // OK
        case Failure(ex) =>
          logger.error(s"❌ Validation failed for Delta Lake table: $filePath", ex)
          return Source.failed(ex)
      }

      logger.info(s"📂 Opening Delta Lake table at timestamp $timestampMillis: $filePath")

      val deltaLog = DeltaLog.forTable(conf, filePath)
      val snapshot = deltaLog.getSnapshotForTimestampAsOf(timestampMillis)

      logger.info(s"📊 Resolved to version: ${snapshot.getVersion}")

      fromFileAtVersion(filePath, snapshot.getVersion, bufferSize)
    }

    /** Get Delta table metadata
      */
    def getTableInfo(
      filePath: String
    )(implicit conf: Configuration = hadoopConfiguration): DeltaTableInfo = {
      val deltaLog = DeltaLog.forTable(conf, filePath)
      val snapshot = deltaLog.snapshot()
      val metadata = snapshot.getMetadata

      DeltaTableInfo(
        version = snapshot.getVersion,
        numFiles = snapshot.getAllFiles.size(),
        schema = metadata.getSchema.getTreeString,
        partitionColumns = metadata.getPartitionColumns.asScala.toList,
        createdTime = metadata.getCreatedTime.orElse(0L),
        description = Option(metadata.getDescription)
      )
    }
  }

  /** Case class for Delta table information
    */
  case class DeltaTableInfo(
    version: Long,
    numFiles: Int,
    schema: String,
    partitionColumns: List[String],
    createdTime: Long,
    description: Option[String]
  )

  /** Automatic file format detection */
  object FileFormatDetector {

    def detect(filePath: String)(implicit conf: Configuration = hadoopConfiguration): FileFormat = {
      val lowerPath = filePath.toLowerCase

      if (lowerPath.endsWith(".parquet") || lowerPath.endsWith(".parq")) {
        Parquet
      } else if (lowerPath.endsWith(".jsonl") || lowerPath.endsWith(".ndjson")) {
        Json
      } else if (lowerPath.endsWith(".json")) {
        // Distinguishing JSON Lines vs JSON Array by reading the first character
        detectJsonType(filePath)
      } else if (isDeltaTable(filePath)) {
        Delta
      } else {
        Unknown
      }
    }

    /** Detect if it's a Delta Lake table (check for _delta_log directory)
      */
    private def isDeltaTable(
      filePath: String
    )(implicit conf: Configuration = hadoopConfiguration): Boolean = {
      Try {
        val fs = FileSystem.get(conf)
        val deltaLogPath = new Path(filePath, "_delta_log")
        fs.exists(deltaLogPath) && fs.getFileStatus(deltaLogPath).isDirectory
      }.getOrElse(false)
    }

    /** Distinguish between JSON Lines and JSON Array
      */
    private def detectJsonType(
      filePath: String
    )(implicit conf: Configuration = hadoopConfiguration): FileFormat = {
      Try {
        val is = HadoopInputFile.fromPath(new Path(filePath), conf).newStream()
        try {
          val reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))
          val firstChar = reader.read().toChar
          reader.close()

          if (firstChar == '[') JsonArray else Json
        } finally {
          is.close()
        }
      }.getOrElse(Unknown)
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

    private def apply(
      filePath: String
    )(implicit conf: Configuration): FileSource = {
      FileFormatDetector.detect(filePath) match {
        case Parquet   => ParquetFileSource
        case Json      => JsonFileSource
        case JsonArray => JsonArrayFileSource
        case Delta     => DeltaFileSource
        case Unknown =>
          throw new IllegalArgumentException(
            s"Cannot determine file format for: $filePath. Supported: .parquet, .parq, .json, .jsonl, .ndjson"
          )
      }
    }

    def apply(filePath: String, format: FileFormat)(implicit
      conf: Configuration = hadoopConfiguration
    ): FileSource = {
      format match {
        case Parquet   => ParquetFileSource
        case Json      => JsonFileSource
        case JsonArray => JsonArrayFileSource
        case Delta     => DeltaFileSource
        case Unknown   => apply(filePath)
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
