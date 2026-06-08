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

package app.softnetwork.elastic

import akka.actor.ActorSystem
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.Logger

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.{implicitConversions, reflectiveCalls}

/** Created by smanciot on 30/06/2018.
  */
package object client extends SerializationApi {

  /** Type alias for JSON query
    */
  type JSONQuery = String

  /** Elastic response case class
    * @param sql
    *   - the SQL query if any
    * @param query
    *   - the JSON query
    * @param results
    *   - the results as a sequence of rows
    * @param fieldAliases
    *   - the field aliases used
    * @param aggregations
    *   - the aggregations expected
    */
  case class ElasticResponse(
    sql: Option[String] = None,
    query: JSONQuery,
    results: Seq[ListMap[String, Any]],
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, ClientAggregation]
  )

  sealed trait ElasticAuthMethod {

    /** Create Authorization header value based on credentials
      * @param elasticCredentials
      *   - the elasticsearch credentials
      * @return
      *   - the Authorization header value
      */
    def createAuthHeader(elasticCredentials: ElasticCredentials): String
  }

  case object BasicAuth extends ElasticAuthMethod {

    /** Create Basic Authorization header value based on credentials
      * @param elasticCredentials
      *   - the elasticsearch credentials
      * @return
      *   - the Basic Authorization header value
      */
    def createAuthHeader(elasticCredentials: ElasticCredentials): String = {
      if (elasticCredentials.username.isEmpty || elasticCredentials.password.isEmpty) {
        throw new IllegalArgumentException(
          "Basic auth requires non-empty username and password"
        )
      }
      import elasticCredentials._
      val credentials = s"$username:$password"
      val encoded = Base64.getEncoder.encodeToString(
        credentials.getBytes(StandardCharsets.UTF_8)
      )
      s"Basic $encoded"
    }
  }

  case object ApiKeyAuth extends ElasticAuthMethod {

    /** Create API Key Authorization header value based on credentials
      * @param elasticCredentials
      *   - the elasticsearch credentials
      * @return
      *   - the API Key Authorization header value
      */
    def createAuthHeader(elasticCredentials: ElasticCredentials): String = {
      val encodedApiKey = elasticCredentials.encodedApiKey.getOrElse {
        throw new IllegalArgumentException("API Key auth requires non-empty apiKey")
      }
      s"ApiKey $encodedApiKey"
    }
  }

  case object BearerTokenAuth extends ElasticAuthMethod {

    /** Create Bearer Token Authorization header value based on credentials
      * @param elasticCredentials
      *   - the elasticsearch credentials
      * @return
      *   - the Bearer Token Authorization header value
      */
    def createAuthHeader(elasticCredentials: ElasticCredentials): String = {
      val bearerToken = elasticCredentials.bearerToken.getOrElse {
        throw new IllegalArgumentException("Bearer token auth requires non-empty bearerToken")
      }
      s"Bearer $bearerToken"
    }
  }

  case object NoAuth extends ElasticAuthMethod {

    /** Create empty Authorization header value
      * @param elasticCredentials
      *   - the elasticsearch credentials
      * @return
      *   - an empty Authorization header value
      */
    def createAuthHeader(elasticCredentials: ElasticCredentials): String = {
      ""
    }
  }

  object ElasticAuthMethod {
    def fromCredentials(credentials: ElasticCredentials): Option[ElasticAuthMethod] = {
      if (credentials.apiKey.exists(_.nonEmpty)) {
        Some(ApiKeyAuth)
      } else if (credentials.bearerToken.exists(_.nonEmpty)) {
        Some(BearerTokenAuth)
      } else if (credentials.username.nonEmpty && credentials.password.nonEmpty) {
        Some(BasicAuth)
      } else {
        None
      }
    }

    def apply(method: String): Option[ElasticAuthMethod] = method.toLowerCase match {
      case "basic"  => Some(BasicAuth)
      case "apikey" => Some(ApiKeyAuth)
      case "bearer" => Some(BearerTokenAuth)
      case "noauth" => Some(NoAuth)
      case _        => None
    }
  }

  /** Elastic connection credentials
    * @param scheme
    *   - the connection scheme (http or https)
    * @param host
    *   - the elasticsearch host
    * @param port
    *   - the elasticsearch port
    * @param method
    *   - the authentication method (basic, apikey, bearer)
    * @param username
    *   - the elasticsearch username
    * @param password
    *   - the elasticsearch password
    * @param apiKey
    *   - the elasticsearch api key
    * @param bearerToken
    *   - the elasticsearch bearer token
    */
  case class ElasticCredentials(
    scheme: String = "http",
    host: String = "localhost",
    port: Int = 9200,
    method: Option[String] = None,
    username: String = "",
    password: String = "",
    apiKey: Option[String] = None,
    bearerToken: Option[String] = None
  ) extends LazyLogging {
    lazy val url = s"$scheme://$host:$port"

    lazy val authMethod: Option[ElasticAuthMethod] = {
      method.flatMap(ElasticAuthMethod(_)).orElse {
        ElasticAuthMethod.fromCredentials(this)
      }
    }

    /** Get encoded API Key for Authorization header */
    lazy val encodedApiKey: Option[String] = {
      apiKey.map { key =>
        if (key.contains(":")) {
          // Format "id:api_key" -> encode to Base64
          Base64.getEncoder.encodeToString(key.getBytes(StandardCharsets.UTF_8))
        } else {
          // Already encoded
          key
        }
      }
    }

    def isBasicAuth: Boolean = authMethod.contains(BasicAuth)

    def isApiKeyAuth: Boolean = authMethod.contains(ApiKeyAuth)

    def isBearerTokenAuth: Boolean = authMethod.contains(BearerTokenAuth)

    /** Validate credentials based on selected auth method */
    def validate(): Either[String, Unit] = {
      authMethod match {
        case Some(BasicAuth) =>
          if (username.isEmpty || password.isEmpty) {
            Left("Basic auth requires non-empty username and password")
          } else {
            Right(())
          }
        case Some(ApiKeyAuth) =>
          if (apiKey.forall(_.isEmpty)) {
            Left("API Key auth requires non-empty apiKey")
          } else {
            Right(())
          }
        case Some(BearerTokenAuth) =>
          if (bearerToken.forall(_.isEmpty)) {
            Left("Bearer token auth requires non-empty bearerToken")
          } else {
            Right(())
          }
        case _ =>
          Right(()) // No auth needed
      }
    }

  }

  /** Elastic query wrapper
    * @param query
    *   - the elasticsearch JSON query
    * @param indices
    *   - the target indices
    * @param types
    *   - the target types @deprecated types are deprecated in ES 7+
    * @param sql
    *   - the optional SQL query for reference
    * @param explodeNested
    *   - whether to explode nested fields in the results
    */
  case class ElasticQuery(
    query: JSONQuery,
    indices: Seq[String],
    types: Seq[String] = Seq.empty,
    sql: Option[String] = None,
    explodeNested: Boolean = true
  ) {
    override def toString: String = s"""ElasticQuery:
        |  Indices: ${indices.mkString(",")}
        |  Types: ${types.mkString(",")}
        |  SQL: ${sql.getOrElse("")}
        |  Query: $query
        |  Explode Nested: $explodeNested
        |""".stripMargin
  }

  case class ElasticQueries(
    queries: List[ElasticQuery],
    sql: Option[String] = None,
    explodeNested: Boolean = true
  ) {
    val multiQuery: String = queries.map(_.query).mkString("\n")

    val sqlQuery: String = sql
      .orElse(
        Option(queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
      )
      .getOrElse("")

    override def toString: String = s"""
        |ElasticQueries:
        |  SQL: ${sql.getOrElse(sqlQuery)}
        |  Multiquery: $multiQuery
        |""".stripMargin
  }

  /** Retry configuration
    */
  case class RetryConfig(
    maxRetries: Int = 3,
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 10.seconds,
    backoffFactor: Double = 2.0
  )

  /** Retry logic with exponential backoff
    */
  // Passer le scheduler en paramètre implicite
  private[client] def retryWithBackoff[T](config: RetryConfig)(
    operation: => Future[T]
  )(implicit
    system: ActorSystem,
    logger: Logger
  ): Future[T] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val scheduler = system.scheduler
    def attempt(retriesLeft: Int, delay: FiniteDuration): Future[T] = {
      operation.recoverWith {
        case ex if retriesLeft > 0 && isRetriableError(ex) =>
          logger.warn(s"Retrying after failure ($retriesLeft retries left): ${ex.getMessage}")
          akka.pattern.after(delay, scheduler) {
            val nextDelay = FiniteDuration(
              (delay * config.backoffFactor).min(config.maxDelay).toMillis,
              TimeUnit.MILLISECONDS
            )
            attempt(retriesLeft - 1, nextDelay)
          }
      }
    }
    attempt(config.maxRetries, config.initialDelay)
  }

  /** Determine if an error is retriable
    */
  private[client] def isRetriableError(ex: Throwable): Boolean = ex match {
    case _: java.net.SocketTimeoutException => true
    case _: java.io.IOException             => true
    // case _: TransportException => true
    case _ => false
  }

  /** Aggregation types
    */
  object AggregationType extends Enumeration {
    type AggregationType = Value
    val Count, Min, Max, Avg, Sum, FirstValue, LastValue, ArrayAgg,
    // Ranking-style window functions. Each top_hits hit gets a per-row
    // ordinal computed Scala-side by the searchWithWindowEnrichment
    // pipeline (RankingKind in function.aggregate); the ordinal is then
    // injected into the base-query row by (partitionKey, _id) lookup.
    RowNumber, Rank, DenseRank,
    // STDDEV / VARIANCE family — all back the same ES `extended_stats`
    // aggregation; the specific result key is carried separately on
    // ClientAggregation.aggResultField so extractMetrics knows which
    // field to project from the response.
    Stddev, StddevSamp, StddevPop, Variance, VarSamp, VarPop,
    // PERCENTILE_CONT / PERCENTILE_DISC — both back the ES `percentiles`
    // aggregation; the requested percentile key (e.g. "99.0") is carried on
    // ClientAggregation.aggResultField and projected from the response `values`.
    PercentileCont, PercentileDisc = Value
  }

  /** Client Aggregation
    * @param aggName
    *   - the name of the aggregation
    * @param aggType
    *   - the type of the aggregation
    * @param distinct
    *   - when the aggregation is multivalued define if its values should be returned distinct or
    *     not
    * @param sourceField
    *   - the source field of the aggregation
    * @param windowing
    *   - whether the aggregation is a window function with partitioning
    * @param bucketPath
    *   - the bucket path for pipeline aggregations
    */
  case class ClientAggregation(
    aggName: String,
    aggType: AggregationType.AggregationType,
    distinct: Boolean,
    sourceField: String,
    windowing: Boolean,
    bucketPath: String,
    bucketRoot: String,
    auxiliary: Boolean = false,
    // Response field projected from a multi-key ES aggregation (currently
    // `extended_stats` — e.g. "std_deviation_sampling", "variance"). The
    // un-suffixed "std_deviation"/"variance" keys are the population values
    // (ES 6+); the "_sampling" keys are the sample values (ES 7.7+).
    // None for plain `value`-style metrics.
    aggResultField: Option[String] = None,
    // When several percentile columns coalesce into one ES `percentiles`
    // aggregation, the delegates name the shared response node here (the owner
    // column's aggName). extractMetrics reads `values[aggResultField]` from that
    // node for the delegate column. None ⇒ this column reads its own node.
    sourceAgg: Option[String] = None
  ) {
    def multivalued: Boolean =
      aggType == AggregationType.ArrayAgg ||
      // Ranking windows return a per-row stream from the underlying
      // top_hits sub-aggregation; the enrichment pipeline consumes the
      // list to compute ordinals (Scala-side) and look them up by _id.
      aggType == AggregationType.RowNumber ||
      aggType == AggregationType.Rank ||
      aggType == AggregationType.DenseRank
    def singleValued: Boolean = !multivalued

    def ranking: Boolean =
      aggType == AggregationType.RowNumber ||
      aggType == AggregationType.Rank ||
      aggType == AggregationType.DenseRank
  }

  implicit def sqlAggregationToClientAggregation(agg: SQLAggregation): ClientAggregation = {
    val aggType = agg.aggType match {
      case COUNT         => AggregationType.Count
      case MIN           => AggregationType.Min
      case MAX           => AggregationType.Max
      case AVG           => AggregationType.Avg
      case SUM           => AggregationType.Sum
      case STDDEV        => AggregationType.Stddev
      case STDDEV_SAMP   => AggregationType.StddevSamp
      case STDDEV_POP    => AggregationType.StddevPop
      case VARIANCE      => AggregationType.Variance
      case VAR_SAMP      => AggregationType.VarSamp
      case VAR_POP       => AggregationType.VarPop
      case _: FirstValue => AggregationType.FirstValue
      case _: LastValue  => AggregationType.LastValue
      case _: ArrayAgg   => AggregationType.ArrayAgg
      case _: CountAgg   => AggregationType.Count
      case _: MinAgg     => AggregationType.Min
      case _: MaxAgg     => AggregationType.Max
      case _: AvgAgg     => AggregationType.Avg
      case _: SumAgg     => AggregationType.Sum
      case _: RowNumber  => AggregationType.RowNumber
      case _: Ranking    => AggregationType.Rank
      case _: DenseRank  => AggregationType.DenseRank
      case e: ExtendedStatsAgg =>
        e.kind match {
          case ExtendedStatsKind.Stddev     => AggregationType.Stddev
          case ExtendedStatsKind.StddevSamp => AggregationType.StddevSamp
          case ExtendedStatsKind.StddevPop  => AggregationType.StddevPop
          case ExtendedStatsKind.Variance   => AggregationType.Variance
          case ExtendedStatsKind.VarSamp    => AggregationType.VarSamp
          case ExtendedStatsKind.VarPop     => AggregationType.VarPop
        }
      case p: PercentileAgg =>
        if (p.cont) AggregationType.PercentileCont else AggregationType.PercentileDisc
      case _ => throw new IllegalArgumentException(s"Unsupported aggregation type: ${agg.aggType}")
    }
    // `extended_stats` is multi-key — pick which one to project. Plain
    // tokens (STDDEV / STDDEV_POP / …) get a fixed key matching the SQL
    // semantic; the wrapped ExtendedStatsAgg carries it on the kind.
    val aggResultField: Option[String] = agg.aggType match {
      case STDDEV              => Some(ExtendedStatsKind.Stddev.resultField)
      case STDDEV_SAMP         => Some(ExtendedStatsKind.StddevSamp.resultField)
      case STDDEV_POP          => Some(ExtendedStatsKind.StddevPop.resultField)
      case VARIANCE            => Some(ExtendedStatsKind.Variance.resultField)
      case VAR_SAMP            => Some(ExtendedStatsKind.VarSamp.resultField)
      case VAR_POP             => Some(ExtendedStatsKind.VarPop.resultField)
      case e: ExtendedStatsAgg => Some(e.kind.resultField)
      // `percentiles` is multi-key — project the requested percentile (e.g. "99.0")
      // from the response `values` object (see extractMetrics).
      case p: PercentileAgg => Some(p.resultField)
      case _                => None
    }
    ClientAggregation(
      agg.aggName,
      aggType,
      agg.distinct,
      agg.sourceField,
      agg.aggType.isWindowing,
      agg.bucketPath,
      agg.bucketRoot,
      agg.auxiliary,
      aggResultField
    )
  }

  sealed trait ConversionContext

  case object NativeContext extends ConversionContext

  case object EntityContext extends ConversionContext
}
