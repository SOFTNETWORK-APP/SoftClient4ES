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

package app.softnetwork.elastic.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.file._
import app.softnetwork.elastic.client.result.{ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.sql.schema.sqlConfig
import org.apache.hadoop.conf.Configuration

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.{ExecutionContext, Future}

/** Bulk API for Elasticsearch clients.
  */
trait BulkApi extends BulkTypes with ElasticClientHelpers {
  self: RefreshApi with SettingsApi with IndexApi =>

  type A = BulkActionType
  type R = BulkResultType

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Bulk from a Parquet or JSON file with automatic detection
    *
    * @param filePath
    *   path to the file (.parquet, .json, .jsonl)
    * @param indexKey
    *   JSON key to extract the index
    * @param idKey
    *   JSON key to extract the ID
    * @param suffixDateKey
    *   JSON key to append a date to the index
    * @param suffixDatePattern
    *   date formatting pattern
    * @param update
    *   true for upsert, false for index
    * @param delete
    *   true for delete
    * @param parentIdKey
    *   JSON key for the parent
    * @param callbacks
    *   callbacks for events
    * @param bufferSize
    *   read buffer size
    * @param validateJson
    *   validate each JSON line
    * @param skipInvalid
    *   ignore invalid JSON lines
    * @param format
    *   file format (auto-detection if Unknown)
    * @param hadoopConf
    *   custom Hadoop configuration
    * @param bulkOptions
    *   configuration options
    * @return
    *   Future with the detailed result
    */
  def bulkFromFile(
    filePath: String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None,
    callbacks: BulkCallbacks = BulkCallbacks.default,
    bufferSize: Int = 500,
    validateJson: Boolean = true,
    skipInvalid: Boolean = true,
    format: FileFormat = Unknown,
    hadoopConf: Option[Configuration] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {

    implicit val ec: ExecutionContext = system.dispatcher
    implicit val conf: Configuration = hadoopConf.getOrElse(hadoopConfiguration)

    logger.info(s"📁 Starting bulk from file: $filePath")

    val source: Source[String, NotUsed] = if (validateJson) {
      FileSourceFactory.fromFileValidated(filePath, bufferSize, skipInvalid, format)
    } else {
      FileSourceFactory.fromFile(filePath, bufferSize, format)
    }

    // Use the existing API with the file source
    bulkWithResult[String](
      items = source,
      toDocument = identity, // The document is already in JSON format.
      indexKey = indexKey,
      idKey = idKey,
      suffixDateKey = suffixDateKey,
      suffixDatePattern = suffixDatePattern,
      update = update,
      delete = delete,
      parentIdKey = parentIdKey,
      callbacks = callbacks
    )
  }

  /** Bulk from a Parquet file specifically
    */
  def bulkFromParquet(
    filePath: String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    callbacks: BulkCallbacks = BulkCallbacks.default,
    bufferSize: Int = 500,
    hadoopConf: Option[Configuration] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {

    implicit val ec: ExecutionContext = system.dispatcher
    implicit val conf: Configuration = hadoopConf.getOrElse(hadoopConfiguration)

    logger.info(s"📁 Starting bulk from Parquet file: $filePath")

    bulkWithResult[String](
      items = ParquetFileSource.fromFile(filePath, bufferSize),
      toDocument = identity,
      indexKey = indexKey,
      idKey = idKey,
      callbacks = callbacks
    )
  }

  /** Bulk from a specific JSON file
    */
  def bulkFromJson(
    filePath: String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    callbacks: BulkCallbacks = BulkCallbacks.default,
    bufferSize: Int = 500,
    validateJson: Boolean = true,
    skipInvalid: Boolean = true,
    hadoopConf: Option[Configuration] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {

    implicit val ec: ExecutionContext = system.dispatcher
    implicit val conf: Configuration = hadoopConf.getOrElse(hadoopConfiguration)

    logger.info(s"📁 Starting bulk from JSON file: $filePath")

    val source = if (validateJson) {
      JsonFileSource.fromFileValidated(filePath, bufferSize, skipInvalid)
    } else {
      JsonFileSource.fromFile(filePath, bufferSize)
    }

    bulkWithResult[String](
      items = source,
      toDocument = identity,
      indexKey = indexKey,
      idKey = idKey,
      callbacks = callbacks
    )
  }

  /** Bulk with detailed results (successes + failures).
    *
    * This method provides:
    *
    *   - List of successfully indexed documents
    *   - List of failed documents with error details
    *   - Performance metrics
    *   - Configurable automatic retry
    *
    * @param items
    *   : documents to index
    * @param toDocument
    *   : JSON transformation function
    * @param indexKey
    *   : key for the index field
    * @param idKey
    *   : key for the id field
    * @param suffixDateKey
    *   : key for the date field to suffix the index
    * @param suffixDatePattern
    *   : date pattern for the suffix
    * @param update
    *   : true for upsert, false for index
    * @param delete
    *   : true for delete
    * @param parentIdKey
    *   : key for the parent field
    * @param callbacks
    *   : callbacks for events
    * @param bulkOptions
    *   : configuration options
    * @return
    *   Future with detailed results
    */
  def bulkWithResult[D](
    items: Source[D, NotUsed],
    toDocument: D => String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None,
    callbacks: BulkCallbacks = BulkCallbacks.default
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {

    implicit val materializer: Materializer = Materializer(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val startTime = System.currentTimeMillis()
    var metrics = BulkMetrics(startTime = startTime)

    bulkSource(
      items,
      toDocument,
      indexKey,
      idKey,
      suffixDateKey,
      suffixDatePattern,
      update,
      delete,
      parentIdKey
    )
      .runWith(Sink.fold((Set.empty[String], Seq.empty[FailedDocument], Set.empty[String])) {
        case ((successIds, failedDocs, indices), Right(successfulDoc)) =>
          callbacks.onSuccess(successfulDoc.id, successfulDoc.index)
          (successIds + successfulDoc.id, failedDocs, indices + successfulDoc.index)

        case ((successIds, failedDocs, indices), Left(failed)) =>
          callbacks.onFailure(failed)
          metrics = metrics.addFailure(failed.error)
          (successIds, failedDocs :+ failed, indices + failed.index)
      })
      .map { case (successIds, failedDocs, indices) =>
        metrics = metrics.copy(
          endTime = Some(System.currentTimeMillis()),
          totalDocuments = successIds.size + failedDocs.size
        )

        val result = BulkResult(
          successCount = successIds.size,
          successIds = successIds,
          failedCount = failedDocs.size,
          failedDocuments = failedDocs,
          indices = indices,
          metrics = metrics.complete
        )

        callbacks.onComplete(result)

        // Refresh indexes if necessary
        if (!bulkOptions.disableRefresh) {
          indices.foreach(refresh)
        }

        result
      }
  }

  //format:off
  /** Source: Akka Streams, which provides real-time results.
    *
    * Each emitted item is either:
    *   - Right(id) for success
    *   - Left(failed) for failure
    *
    * @example
    * {{{
    *   bulkSource(items, toDocument)
    *     .runWith(Sink.foreach {
    *       case Right(id)    => println(s"✅ Success: $id")
    *       case Left(failed) => println(s"❌ Failed: ${failed.id}")
    *     })
    * }}}
    * @param items
    *   the documents to index
    * @param toDocument
    *   JSON transformation function
    * @param indexKey
    *   key for the index field
    * @param idKey
    *   key for the id field
    * @param suffixDateKey
    *   date field key to suffix the index
    * @param suffixDatePattern
    *   date pattern for the suffix
    * @param update
    *   true for upsert, false for index
    * @param delete
    *   true to delete
    * @param parentIdKey
    *   parent field key
    * @param bulkOptions
    *   configuration options
    * @return
    *   Source outputting Right(id) or Left(failed)
    */
  //format:on
  def bulkSource[D](
    items: Source[D, NotUsed],
    toDocument: D => String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Source[Either[FailedDocument, SuccessfulDocument], NotUsed] = {

    implicit val ec: ExecutionContext = system.dispatcher

    var metrics = BulkMetrics()

    items
      // ✅ Transformation en BulkItem
      .map { item =>
        toBulkItem(
          toDocument,
          indexKey,
          idKey,
          suffixDateKey,
          suffixDatePattern,
          update,
          delete,
          parentIdKey,
          item
        )
      }
      // ✅ Settings management (refresh, replicas)
      .via(
        BulkSettings[BulkItem](bulkOptions.disableRefresh)(
          self,
          toBulkAction
        )
      )
      // ✅ Batch grouping
      .grouped(bulkOptions.maxBulkSize)
      .map { batch =>
        metrics = metrics.copy(totalBatches = metrics.totalBatches + 1)
        if (metrics.totalBatches % bulkOptions.logEvery == 0) {
          logger.info(
            s"Processing batch ${metrics.totalBatches} " +
            s"(${metrics.totalDocuments} docs, ${metrics.throughput} docs/sec)"
          )
        }
        batch
      }
      // ✅ Conversion to BulkActionType
      .map(_.map(toBulkAction))
      // ✅ Execution of bulk with parallelism
      .via(balancedBulkFlow(bulkOptions.balance))
      // ✅ Extracting results with original batch
      .mapConcat { case (result, originalBatch) =>
        extractBulkResults(result, originalBatch)
      }
      // ✅ Automatic retry of failures
      .mapAsync(1) {
        case success @ Right(_) =>
          Future.successful(success)

        case failure @ Left(failed) if bulkOptions.retryOnFailure && failed.retryable =>
          retryWithBackoff(failed, bulkOptions.maxRetries)
            .map {
              case true  => Right(SuccessfulDocument(id = failed.id, index = failed.index))
              case false => failure
            }

        case failure =>
          Future.successful(failure)
      }
  }

  /** Backward compatible API (old signature).
    *
    * @deprecated
    *   Use `bulkWithResult` to get failure details
    */
  def bulk[D](
    items: Source[D, NotUsed],
    toDocument: D => String,
    indexKey: Option[String] = None,
    idKey: Option[Set[String]] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): ElasticResult[BulkResult] = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val result = Await.result(
      bulkWithResult(
        items,
        toDocument,
        indexKey,
        idKey,
        suffixDateKey,
        suffixDatePattern,
        update,
        delete,
        parentIdKey
      ),
      Duration.Inf
    )

    // Refresh indexes if necessary
    if (!bulkOptions.disableRefresh) {
      result.indices.foreach(refresh)
    }

    ElasticResult.success(result)
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  implicit private[client] def toBulkElasticAction(a: BulkActionType): BulkElasticAction

  /** Basic flow for executing a bulk action. This method must be implemented by concrete classes
    * depending on the Elasticsearch version and client used.
    *
    * @param bulkOptions
    *   configuration options
    * @return
    *   Flow transforming bulk actions into results
    */
  private[client] def bulkFlow(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed]

  /** Convert a BulkResultType into individual results. This method must extract the successes and
    * failures from the ES response.
    *
    * @param result
    *   raw result from the bulk
    * @return
    *   sequence of Right(id) for success or Left(failed) for failure
    */
  private[client] def extractBulkResults(
    result: BulkResultType,
    originalBatch: Seq[BulkItem]
  ): Seq[Either[FailedDocument, SuccessfulDocument]]

  /** Conversion BulkItem -> BulkActionType */
  private[client] def toBulkAction(bulkItem: BulkItem): BulkActionType

  /** Conversion BulkActionType -> BulkItem */
  private[client] def actionToBulkItem(action: BulkActionType): BulkItem

  // ========================================================================
  // PRIVATE HELPERS
  // ========================================================================

  /** Flow with balance for parallelism */
  private def balancedBulkFlow(
    parallelism: Int
  )(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], (BulkResultType, Seq[BulkItem]), NotUsed] = {

    implicit val ec: ExecutionContext = system.dispatcher

    if (parallelism > 1) {
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        val balancer = b.add(Balance[Seq[BulkActionType]](parallelism))
        val merge = b.add(Merge[(BulkResultType, Seq[BulkItem])](parallelism))

        // ✅ Keep the original batch for extracting the results
        val bulkFlowWithOriginal = Flow[Seq[BulkActionType]]
          .map(batch => (batch, batch.map(actionToBulkItem)))
          .mapAsync(1) { case (actions, originalBatch) =>
            // Run the bulk via bulkFlow
            Source
              .single(actions)
              .via(bulkFlow)
              .runWith(Sink.head)
              .map(result => (result, originalBatch))
          }

        balancer ~> bulkFlowWithOriginal ~> merge

        1 until parallelism foreach { _ =>
          balancer ~> bulkFlowWithOriginal ~> merge
        }

        FlowShape(balancer.in, merge.out)
      })
    } else {
      Flow[Seq[BulkActionType]]
        .map(batch => (batch, batch.map(actionToBulkItem)))
        .mapAsync(1) { case (actions, originalBatch) =>
          Source
            .single(actions)
            .via(bulkFlow)
            .runWith(Sink.head)
            .map(result => (result, originalBatch))
        }
    }
  }

  /** Retry with exponential backoff */
  private def retryWithBackoff(
    failed: FailedDocument,
    maxRetries: Int,
    currentRetry: Int = 0
  )(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Future[Boolean] = {

    implicit val ec: ExecutionContext = system.dispatcher

    if (currentRetry >= maxRetries) {
      logger.warn(s"Max retries ($maxRetries) reached for document ${failed.id}")
      return Future.successful(false)
    }

    val delay = bulkOptions.retryDelay * Math
      .pow(
        bulkOptions.retryBackoffMultiplier,
        currentRetry
      )
      .toLong

    logger.info(
      s"Retrying document ${failed.id} (attempt ${currentRetry + 1}/$maxRetries) " +
      s"after ${delay.toMillis}ms"
    )

    akka.pattern.after(delay, system.scheduler) {
      executeSingleDocument(failed)
        .flatMap {
          case true =>
            logger.info(s"✅ Successfully retried document ${failed.id}")
            Future.successful(true)

          case false =>
            logger.warn(s"❌ Retry failed for document ${failed.id}")
            retryWithBackoff(failed, maxRetries, currentRetry + 1)
        }
        .recoverWith { case ex: Throwable =>
          logger.error(s"Exception during retry of ${failed.id}: ${ex.getMessage}")
          retryWithBackoff(failed, maxRetries, currentRetry + 1)
        }
    }
  }

  /** Execute a single document (for retry).
    *
    * @param failed
    *   failed document to retry
    * @return
    *   Future[Boolean] indicating success
    */
  private def executeSingleDocument(
    failed: FailedDocument
  )(implicit system: ActorSystem): Future[Boolean] = {
    implicit val ec: ExecutionContext = system.dispatcher
    indexAsync(failed.index, failed.document, failed.id).flatMap {
      case ElasticSuccess(true) =>
        Future.successful(true)

      case _ =>
        Future.successful(false)
    }
  }

  private def toBulkItem[D](
    toDocument: D => String,
    indexKey: Option[String],
    idKey: Option[Set[String]],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String],
    item: D
  )(implicit bulkOptions: BulkOptions): BulkItem = {
    val document = toDocument(item)
    val jsonNode = mapper.readTree(document)
    val jsonMap = mapper.convertValue(jsonNode, classOf[Map[String, Any]])

    // extract id
    val id = idKey.map { keys =>
      keys.map(i => jsonMap.getOrElse(i, "").toString).mkString(sqlConfig.compositeKeySeparator)
    }

    // extract final index name
    val indexFromKeyOrOptions = indexKey
      .flatMap { i =>
        jsonMap.get(i).map(_.toString)
      }
      .getOrElse(bulkOptions.defaultIndex)
    val index = suffixDateKey
      .flatMap { s =>
        jsonMap.get(s).map { d =>
          val pattern = suffixDatePattern.getOrElse("yyyy-MM-dd")
          val strDate = d.toString.substring(0, pattern.length)
          val date = LocalDate.parse(strDate, DateTimeFormatter.ofPattern(pattern))
          date.format(
            suffixDatePattern
              .map(DateTimeFormatter.ofPattern)
              .getOrElse(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          )
        }
      }
      .map(s => s"$indexFromKeyOrOptions-$s")
      .getOrElse(indexFromKeyOrOptions)

    // extract parent key
    val parent = parentIdKey.flatMap { i =>
      jsonMap.get(i).map(_.toString)
    }

    val action = delete match {
      case Some(d) if d => BulkAction.DELETE
      case _ =>
        update match {
          case Some(u) if u => BulkAction.UPDATE
          case _            => BulkAction.INDEX
        }
    }

    BulkItem(index, action, document, id, parent)
  }

}
