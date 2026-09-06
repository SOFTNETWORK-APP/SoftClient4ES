package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/** Issue #222 (story BIDC-3) -- the ONE core boundary the per-major refusal relies on.
  *
  * The SQL -> Elasticsearch translation runs synchronously inside `searchAsync` / `scroll`, before
  * any Future exists. A client module refuses a statement there by throwing a status-bearing
  * `ElasticError` (the ES 6 / ES 7 modules do so for STDDEV / VARIANCE over a transformed
  * expression). `GatewayApi.run` must surface that refusal as the `ElasticFailure` every other error
  * is -- on EVERY route a DQL statement can take: the `SearchExecutor` (aggregation-shaped, or an
  * explicit LIMIT) AND the `CoreDqlExtension`'s quota-capped scroll (an un-LIMITed row query),
  * which calls `client.scroll` directly and never enters the executor. That second route is why
  * the boundary lives in `run`, not in `SearchExecutor`: a first cut there let the refusal escape
  * as a raw exception on exactly the path BI tools take for a plain projection. One route
  * translates LAZILY (the windowed row query's scroll): there the refusal fails the stream when it
  * is materialised -- loud, never silent -- and that surface is pinned too.
  *
  * Docker-free: a `NopeClientApi` whose `singleSearchToJsonQuery` refuses like a client module.
  */
class GatewayRefusalBoundarySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("gateway-refusal-boundary")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(5.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val refusalMessage =
    "STDDEV/VARIANCE over a transformed expression is not supported on Elasticsearch 7 (test double)"

  private class RefusingClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    override private[client] implicit def singleSearchToJsonQuery(
      sqlSearch: SingleSearch
    )(implicit timestamp: Long, contextType: PainlessContextType): String =
      throw ElasticError(
        message = refusalMessage,
        statusCode = Some(400),
        operation = Some("search")
      )
  }

  private val client = new RefusingClient

  private def assertRefused(result: ElasticResult[QueryResult]): Unit =
    result match {
      case ElasticFailure(error) =>
        error.message shouldBe refusalMessage
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("dql")
      case other => fail(s"a refused translation must be an ElasticFailure, got $other")
    }

  "GatewayApi.run" should "surface a client-module refusal as an ElasticFailure on the SearchExecutor route (aggregation shape)" in {
    assertRefused(
      client.run("SELECT id, STDDEV(YEAR(createdAt)) AS s FROM t GROUP BY id").futureValue
    )
  }

  it should "surface it on the SearchExecutor route (explicit LIMIT, one-shot row query)" in {
    assertRefused(client.run("SELECT id, name FROM t LIMIT 5").futureValue)
  }

  it should "surface it on the CoreDqlExtension quota-capped scroll route (un-LIMITed row query)" in {
    // Community quota is finite, so `capOrReject` rule (2) calls `client.scroll` directly -- the
    // route that bypasses every executor. This is the case that found the misplaced boundary.
    assertRefused(client.run("SELECT id, name FROM t").futureValue)
  }

  it should "surface it on the windowed row route (un-LIMITed STDDEV OVER PARTITION BY) when the stream runs" in {
    // The quota-capped scroll of a WINDOWED row query builds its source lazily
    // (`scrollWithWindowEnrichment` -> `Source.futureSource`), so the translation -- and the
    // refusal -- happen when the stream is materialised, not when `run` returns. The refusal is
    // still the same status-bearing ElasticError, and it FAILS the stream: nothing is returned
    // silently. Pinned here so the surface is known, not assumed.
    client
      .run("SELECT id, name, STDDEV(YEAR(createdAt)) OVER (PARTITION BY id) AS s FROM t")
      .futureValue match {
      case ElasticFailure(error) =>
        error.message shouldBe refusalMessage
        error.statusCode shouldBe Some(400)
      case ElasticSuccess(QueryStream(stream, _)) =>
        val failure = intercept[ElasticError](Await.result(stream.runWith(Sink.seq), 5.seconds))
        failure.message shouldBe refusalMessage
        failure.statusCode shouldBe Some(400)
      case other => fail(s"a refused translation must fail, got $other")
    }
  }

  it should "not manufacture a refusal for a client that does not refuse" in {
    // NopeClientApi answers a search with no response body, which core reports as an ordinary
    // execution failure -- proving the boundary only relabels a refusal the client raised.
    val plain = new NopeClientApi {
      override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    }
    plain.run("SELECT id, name FROM t LIMIT 5").futureValue match {
      case ElasticFailure(error) =>
        error.message should not include refusalMessage
        error.statusCode should not be Some(400)
      case other => fail(s"unexpected $other")
    }
  }
}
