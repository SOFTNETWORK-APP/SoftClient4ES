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

package app.softnetwork.elastic.client.jest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.client.{BulkApi, IndexApi, RefreshApi, SettingsApi}
import app.softnetwork.elastic.client.bulk.{
  BulkAction,
  BulkElasticAction,
  BulkError,
  BulkItem,
  BulkOptions,
  FailedDocument,
  SuccessfulDocument
}
import io.searchbox.action.BulkableAction
import io.searchbox.core.{Bulk, Delete, Index, Update}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

trait JestBulkApi extends BulkApi {
  _: RefreshApi with SettingsApi with IndexApi with JestClientCompanion =>

  // ========================================================================
  // TYPE ALIASES FOR JEST
  // ========================================================================

  override type BulkActionType = BulkableAction[_]
  override type BulkResultType = io.searchbox.core.BulkResult

  // ========================================================================
  // BULK ACTION CONVERSION
  // ========================================================================

  override implicit def toBulkElasticAction(a: BulkActionType): BulkElasticAction = {
    new BulkElasticAction {
      override def index: String = {
        a match {
          case idx: Index  => idx.getIndex
          case upd: Update => upd.getIndex
          case del: Delete => del.getIndex
          case _           => ""
        }
      }
    }
  }

  // ========================================================================
  // BULK FLOW IMPLEMENTATION
  // ========================================================================

  override private[client] def bulkFlow(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    Flow[Seq[BulkActionType]]
      .mapAsync(1) { actions =>
        Future {
          val bulkBuilder = new Bulk.Builder()
            .defaultIndex(bulkOptions.defaultIndex)
            .defaultType(bulkOptions.defaultType)

          actions.foreach {
            case idx: Index =>
              bulkBuilder.addAction(idx)
            case upd: Update =>
              bulkBuilder.addAction(upd)
            case del: Delete =>
              bulkBuilder.addAction(del)
            case other =>
              logger.warn(s"Unsupported action type: ${other.getClass.getName}")
          }

          val bulk = bulkBuilder.build()

          Try(apply().execute(bulk)) match {
            case Success(result) =>
              if (!result.isSucceeded) {
                logger.warn(
                  s"Bulk operation completed with errors: ${result.getErrorMessage}"
                )
              } else {
                logger.info(s"Bulk operation succeeded with ${actions.size} actions.")
              }
              result

            case Failure(ex) =>
              logger.error(s"Bulk execution failed: ${ex.getMessage}", ex)
              throw ex
          }
        }
      }
  }

  // ========================================================================
  // EXTRACT BULK RESULTS
  // ========================================================================

  override private[client] def extractBulkResults(
    result: BulkResultType,
    originalBatch: Seq[BulkItem]
  ): Seq[Either[FailedDocument, SuccessfulDocument]] = {

    // no results at all
    if (
      originalBatch.nonEmpty &&
      (result == null || (result.getItems == null && result.getFailedItems == null))
    ) {
      logger.error("Bulk result is null or has no items")
      return originalBatch.map { item =>
        Left(
          FailedDocument(
            id = item.id.getOrElse("unknown"),
            index = item.index,
            document = item.document,
            error = BulkError(
              message = "Null bulk result",
              `type` = "internal_error",
              status = 500
            ),
            retryable = false
          )
        )
      }
    }

    // process failed items
    val failedItems = result.getFailedItems.asScala.toSeq.map { item =>
      val itemIndex = Option(item.index).getOrElse("unknown_index")
      val itemId = Option(item.id).getOrElse("unknown_id")

      val bulkError = parseJestError(item.error, item.status)

      Left(
        FailedDocument(
          id = itemId,
          index = itemIndex,
          document = originalBatch
            .find(o => o.id.contains(itemId) && o.index == itemIndex)
            .map(_.document)
            .getOrElse("{}"),
          error = bulkError,
          retryable = bulkError.isRetryable
        )
      )
    }

    // process successful items
    val items =
      result.getItems.asScala.toSeq.filter(i => i.error == null || i.error.trim.isEmpty).map {
        item =>
          //val itemIndex = Option(item.index).getOrElse("unknown_index")
          //val itemId = Option(item.id).getOrElse("unknown_id")
          Right(SuccessfulDocument(id = item.id, index = item.index))
      }

    val results = failedItems ++ items

    // if no individual results but overall failure, mark all as failed
    if (results.isEmpty && originalBatch.nonEmpty && !result.isSucceeded) {
      logger.error(s"Bulk operation completed with errors: ${result.getErrorMessage}")
      implicit val formats: DefaultFormats = org.json4s.DefaultFormats
      val errorString = result.getJsonString
      val bulkError =
        Try {

          val json = JsonMethods.parse(errorString, useBigDecimalForDouble = false)

          val errorType = (json \ "error" \ "type").extractOpt[String].getOrElse("unknown_error")
          val reason = (json \ "error" \ "reason").extractOpt[String].getOrElse(errorString)
          val status = (json \ "status").extractOpt[Int].getOrElse(500)

          // Extract caused_by if present
          val causedBy =
            (json \ "error" \ "root_cause").extract[Seq[Map[String, Any]]].headOption.map {
              caused =>
                BulkError(
                  message = caused.getOrElse("reason", "Unknown cause").toString,
                  `type` = caused.getOrElse("type", "unknown").toString,
                  status = status
                )
            }

          BulkError(
            message = reason,
            `type` = errorType,
            status = status,
            causedBy = causedBy
          )
        }.getOrElse {
          BulkError(
            message = errorString,
            `type` = "parse_error",
            status = 500
          )
        }
      return originalBatch.map { item =>
        Left(
          FailedDocument(
            id = item.id.getOrElse("unknown"),
            index = item.index,
            document = item.document,
            error = bulkError,
            retryable = false
          )
        )
      }
    }

    results
  }

  // ========================================================================
  // BULK ITEM TO ACTION CONVERSION
  // ========================================================================

  override private[client] implicit def toBulkAction(bulkItem: BulkItem): BulkActionType = {
    bulkItem.action match {
      case BulkAction.INDEX =>
        val indexBuilder = new Index.Builder(bulkItem.document)
          .index(bulkItem.index)

        bulkItem.id.foreach(id => indexBuilder.id(id))
        bulkItem.parent.foreach(parent => indexBuilder.setParameter("parent", parent))

        indexBuilder.build()

      case BulkAction.UPDATE =>
        // Use docAsUpsert helper
        val upsertDoc = docAsUpsert(bulkItem.document)

        val updateBuilder = new Update.Builder(upsertDoc)
          .index(bulkItem.index)
          .id(bulkItem.id.getOrElse(""))

        bulkItem.parent.foreach(parent => updateBuilder.setParameter("parent", parent))

        updateBuilder.build()

      case BulkAction.DELETE =>
        val deleteBuilder = new Delete.Builder(bulkItem.id.getOrElse(""))
          .index(bulkItem.index)

        bulkItem.parent.foreach(parent => deleteBuilder.setParameter("parent", parent))

        deleteBuilder.build()
    }
  }

  // ========================================================================
  // ACTION TO BULK ITEM CONVERSION
  // ========================================================================

  override private[client] def actionToBulkItem(action: BulkActionType): BulkItem = {
    action match {
      case idx: Index =>
        BulkItem(
          index = idx.getIndex,
          action = BulkAction.INDEX,
          document = Option(idx.getData(null)).getOrElse("{}"),
          id = Option(idx.getId),
          parent = Option(idx.getParameter("parent")).map(_.toString)
        )

      case upd: Update =>
        // Extract original document from update payload
        val updatePayload = Option(upd.getData(null)).getOrElse("{}")
        val document = extractDocFromUpdate(updatePayload)

        BulkItem(
          index = upd.getIndex,
          action = BulkAction.UPDATE,
          document = document,
          id = Option(upd.getId),
          parent = Option(upd.getParameter("parent")).map(_.toString)
        )

      case del: Delete =>
        BulkItem(
          index = del.getIndex,
          action = BulkAction.DELETE,
          document = "{}",
          id = Option(del.getId),
          parent = Option(del.getParameter("parent")).map(_.toString)
        )

      case _ =>
        BulkItem(
          index = "",
          action = BulkAction.INDEX,
          document = "{}",
          id = None,
          parent = None
        )
    }
  }

  // ========================================================================
  // HELPER METHODS
  // ========================================================================

  /** Parse error from Jest bulk item error string
    */
  private def parseJestError(errorString: String, status: Int): BulkError = {
    implicit val formats: DefaultFormats = org.json4s.DefaultFormats

    Try {
      val json = JsonMethods.parse(errorString, useBigDecimalForDouble = false)

      val errorType = (json \ "type").extractOpt[String].getOrElse("unknown_error")
      val reason = (json \ "reason").extractOpt[String].getOrElse(errorString)

      // Extract caused_by if present
      val causedBy = (json \ "caused_by").extractOpt[Map[String, Any]].map { caused =>
        BulkError(
          message = caused.getOrElse("reason", "Unknown cause").toString,
          `type` = caused.getOrElse("type", "unknown").toString,
          status = status
        )
      }

      BulkError(
        message = reason,
        `type` = errorType,
        status = status,
        causedBy = causedBy
      )
    }.getOrElse {
      BulkError(
        message = errorString,
        `type` = "parse_error",
        status = status
      )
    }
  }

  /** Extract document from update payload (removes doc_as_upsert wrapper)
    */
  private def extractDocFromUpdate(updatePayload: String): String = {
    implicit val formats: DefaultFormats = org.json4s.DefaultFormats

    Try {
      val json = JsonMethods.parse(updatePayload, useBigDecimalForDouble = false)
      (json \ "doc").extractOpt[Map[String, Any]] match {
        case Some(doc) => org.json4s.jackson.Serialization.write(doc)
        case None      => updatePayload
      }
    }.getOrElse(updatePayload)
  }

  /** Helper to create doc_as_upsert payload
    */
  private def docAsUpsert(doc: String): String = s"""{"doc":$doc,"doc_as_upsert":true}"""

}
