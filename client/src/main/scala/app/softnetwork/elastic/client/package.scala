package app.softnetwork.elastic

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import app.softnetwork.elastic.client.BulkAction.BulkAction
import app.softnetwork.serialization._
import com.google.gson.{Gson, JsonElement, JsonObject}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import configs.Configs
import org.json4s.Formats

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 30/06/2018.
  */
package object client {

  case class ElasticCredentials(
    url: String = "http://localhost:9200",
    username: String = "",
    password: String = ""
  )

  case class ElasticConfig(
    credentials: ElasticCredentials = ElasticCredentials(),
    multithreaded: Boolean = true,
    discoveryEnabled: Boolean = false
  )

  object ElasticConfig extends StrictLogging {
    def apply(config: Config): ElasticConfig = {
      Configs[ElasticConfig]
        .get(config.withFallback(ConfigFactory.load("softnetwork-elastic.conf")), "elastic")
        .toEither match {
        case Left(configError) =>
          logger.error(s"Something went wrong with the provided arguments $configError")
          throw configError.configException
        case Right(r) => r
      }
    }
  }

  object BulkAction extends Enumeration {
    type BulkAction = Value
    val INDEX: client.BulkAction.Value = Value(0, "INDEX")
    val UPDATE: client.BulkAction.Value = Value(1, "UPDATE")
    val DELETE: client.BulkAction.Value = Value(2, "DELETE")
  }

  case class BulkItem(
    index: String,
    action: BulkAction,
    body: String,
    id: Option[String],
    parent: Option[String]
  )

  case class BulkOptions(
    index: String,
    documentType: String,
    maxBulkSize: Int = 100,
    balance: Int = 1,
    disableRefresh: Boolean = false
  )

  trait BulkElasticAction { def index: String }

  trait BulkElasticResult { def items: List[BulkElasticResultItem] }

  trait BulkElasticResultItem { def index: String }

  case class BulkSettings[A](disableRefresh: Boolean = false)(implicit
    settingsApi: SettingsApi,
    toBulkElasticAction: A => BulkElasticAction
  ) extends GraphStage[FlowShape[A, A]] {

    val in: Inlet[A] = Inlet[A]("Filter.in")
    val out: Outlet[A] = Outlet[A]("Filter.out")

    val shape: FlowShape[A, A] = FlowShape.of(in, out)

    val indices = mutable.Set.empty[String]

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) {
        setHandler(
          in,
          () => {
            val elem = grab(in)
            val index = elem.index
            if (!indices.contains(index)) {
              if (disableRefresh) {
                settingsApi.updateSettings(
                  index,
                  """{"index" : {"refresh_interval" : "-1", "number_of_replicas" : 0} }"""
                )
              }
              indices.add(index)
            }
            push(out, elem)
          }
        )
        setHandler(
          out,
          () => {
            pull(in)
          }
        )
      }
    }
  }

  def docAsUpsert(doc: String): String = s"""{"doc":$doc,"doc_as_upsert":true}"""

  implicit class InnerHits(searchResult: JsonObject) {
    import scala.collection.JavaConverters._
    def ~>[M, I](
      innerField: String
    )(implicit formats: Formats, m: Manifest[M], i: Manifest[I]): List[(M, List[I])] = {
      def innerHits(result: JsonElement) = {
        result.getAsJsonObject
          .get("inner_hits")
          .getAsJsonObject
          .get(innerField)
          .getAsJsonObject
          .get("hits")
          .getAsJsonObject
          .get("hits")
          .getAsJsonArray
          .iterator()
      }
      val gson = new Gson()
      val results = searchResult.get("hits").getAsJsonObject.get("hits").getAsJsonArray.iterator()
      (for (result <- results.asScala)
        yield (
          result match {
            case obj: JsonObject =>
              Try {
                serialization.read[M](gson.toJson(obj.get("_source")))
              } match {
                case Success(s) => s
                case Failure(f) =>
                  throw f
              }
            case _ => serialization.read[M](result.getAsString)
          },
          (for (innerHit <- innerHits(result).asScala) yield innerHit match {
            case obj: JsonObject =>
              Try {
                serialization.read[I](gson.toJson(obj.get("_source")))
              } match {
                case Success(s) => s
                case Failure(f) =>
                  throw f
              }
            case _ => serialization.read[I](innerHit.getAsString)
          }).toList
        )).toList
    }
  }

  case class JSONQuery(query: String, indices: Seq[String], types: Seq[String] = Seq.empty)

  case class JSONQueries(queries: List[JSONQuery])

  def tryOrElse[T](block: => T, default: => T)(implicit logger: Logger): T = {
    try {
      block
    } catch {
      case e: Exception =>
        logger.error("An error occurred while executing the block", e)
        default
    }
  }
}
