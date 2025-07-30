package app.softnetwork.elastic.scalatest

import app.softnetwork.concurrent.scalatest.CompletionTestKit
import app.softnetwork.elastic.Softclient4es7TestkitBuildInfo
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.indexes.admin.RefreshIndexResponse
import com.sksamuel.elastic4s.{ElasticClient, ElasticDsl, Indexes}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.HttpHost
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.client.RestClient
import org.elasticsearch.transport.RemoteTransportException
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

/** Created by smanciot on 18/05/2021.
  */
trait ElasticTestKit extends ElasticDsl with CompletionTestKit with BeforeAndAfterAll { _: Suite =>

  def log: Logger

  def elasticVersion: String = Softclient4es7TestkitBuildInfo.elasticVersion

  def elasticURL: String

  lazy val elasticConfig: Config = ConfigFactory
    .parseString(elasticConfigAsString)
    .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))

  lazy val elasticConfigAsString: String =
    s"""
       |elastic {
       |  credentials {
       |    url = "$elasticURL"
       |  }
       |  multithreaded     = false
       |  discovery-enabled = false
       |}
       |""".stripMargin

  lazy val elasticClient: ElasticClient = ElasticClient(
    new JavaClient(
      RestClient
        .builder(
          HttpHost.create(elasticURL)
        )
        .build()
    )
  )

  def start(): Unit = ()

  def stop(): Unit = ()

  override def beforeAll(): Unit = {
    start()
    elasticClient
      .execute {
        createIndexTemplate("all_templates", "*").settings(
          Map("number_of_shards" -> 1, "number_of_replicas" -> 0)
        )
      }
      .complete() match {
      case Success(_) => ()
      case Failure(f) => throw f
    }
  }

  override def afterAll(): Unit = {
    elasticClient.close()
    stop()
  }

  // Rewriting methods from IndexMatchers in elastic4s with the ElasticClient
  def haveCount(expectedCount: Int): Matcher[String] =
    (index: String) => {
      elasticClient.execute(search(index).size(0)).complete() match {
        case Success(s) =>
          val count = s.result.totalHits
          MatchResult(
            count == expectedCount,
            s"Index $index had count $count but expected $expectedCount",
            s"Index $index had document count $expectedCount"
          )
        case Failure(f) => throw f
      }
    }

  def containDoc(expectedId: String): Matcher[String] =
    (index: String) => {
      elasticClient.execute(get(index, expectedId)).complete() match {
        case Success(s) =>
          val exists = s.result.exists
          MatchResult(
            exists,
            s"Index $index did not contain expected document $expectedId",
            s"Index $index contained document $expectedId"
          )
        case Failure(f) => throw f
      }
    }

  def beCreated(): Matcher[String] =
    (index: String) => {
      elasticClient.execute(indexExists(index)).complete() match {
        case Success(s) =>
          val exists = s.result.isExists
          MatchResult(
            exists,
            s"Index $index did not exist",
            s"Index $index exists"
          )
        case Failure(f) => throw f
      }
    }

  def beEmpty(): Matcher[String] =
    (index: String) => {
      elasticClient.execute(search(index).size(0)).complete() match {
        case Success(s) =>
          val count = s.result.totalHits
          MatchResult(
            count == 0,
            s"Index $index was not empty",
            s"Index $index was empty"
          )
        case Failure(f) => throw f
      }
    }

  // Copy/paste methos HttpElasticSugar as it is not available yet

  // refresh all indexes
  def refreshAll(): RefreshIndexResponse = refresh(Indexes.All)

  // refreshes all specified indexes
  def refresh(indexes: Indexes): RefreshIndexResponse = {
    elasticClient
      .execute {
        refreshIndex(indexes)
      }
      .complete() match {
      case Success(s) => s.result
      case Failure(f) => throw f
    }
  }

  def blockUntilGreen(): Unit = {
    blockUntil("Expected cluster to have green status") { () =>
      elasticClient
        .execute {
          clusterHealth()
        }
        .complete() match {
        case Success(s) => s.result.status.toUpperCase == "GREEN"
        case Failure(f) => throw f
      }
    }
  }

  def blockUntil(explain: String)(predicate: () => Boolean): Unit = {
    blockUntil(explain, 16, 200)(predicate)
  }

  def ensureIndexExists(index: String): Unit = {
    elasticClient
      .execute {
        createIndex(index)
      }
      .complete() match {
      case Success(_) => ()
      case Failure(f) =>
        f match {
          case _: ResourceAlreadyExistsException => // Ok, ignore.
          case _: RemoteTransportException       => // Ok, ignore.
          case other                             => throw other
        }
    }
  }

  def doesIndexExists(name: String): Boolean = {
    elasticClient
      .execute {
        indexExists(name)
      }
      .complete() match {
      case Success(s) => s.result.isExists
      case _          => false
    }
  }

  def isIndexOpened(name: String): Boolean = {
    elasticClient
      .execute {
        indexStats(name)
      }
      .complete() match {
      case Success(s) =>
        Try(s.result.indices.contains(name)).toOption.getOrElse(false)
      case _ => false
    }
  }

  def isIndexClosed(name: String): Boolean = {
    doesIndexExists(name) && !isIndexOpened(name)
  }

  def doesAliasExists(name: String): Boolean = {
    elasticClient
      .execute {
        aliasExists(name)
      }
      .complete() match {
      case Success(s) => s.result.isExists
      case _          => false
    }
  }

  def deleteIndex(name: String): Unit = {
    if (doesIndexExists(name)) {
      elasticClient
        .execute {
          ElasticDsl.deleteIndex(name)
        }
        .complete() match {
        case Success(_) => ()
        case Failure(f) => throw f
      }
    }
  }

  def truncateIndex(index: String): Unit = {
    deleteIndex(index)
    ensureIndexExists(index)
    blockUntilEmpty(index)
  }

  def blockUntilDocumentExists(id: String, index: String, _type: String): Unit = {
    blockUntil(s"Expected to find document $id") { () =>
      elasticClient
        .execute {
          get(index, id)
        }
        .complete() match {
        case Success(s) => s.result.exists
        case _          => false
      }
    }
  }

  def blockUntilCount(expected: Long, index: String): Unit = {
    blockUntil(s"Expected count of $expected") { () =>
      elasticClient
        .execute {
          search(index).matchAllQuery().size(0)
        }
        .complete() match {
        case Success(s) => expected <= s.result.totalHits
        case Failure(f) => throw f
      }
    }
  }

  /** Will block until the given index and optional types have at least the given number of
    * documents.
    */
  def blockUntilCount(expected: Long, index: String, types: String*): Unit = {
    blockUntil(s"Expected count of $expected") { () =>
      elasticClient
        .execute {
          search(index).matchAllQuery().size(0)
        }
        .complete() match {
        case Success(s) => expected <= s.result.totalHits
        case Failure(f) => throw f
      }
    }
  }

  def blockUntilExactCount(expected: Long, index: String, types: String*): Unit = {
    blockUntil(s"Expected count of $expected") { () =>
      elasticClient
        .execute {
          search(index).size(0)
        }
        .complete() match {
        case Success(s) => expected == s.result.totalHits
        case Failure(f) => throw f
      }
    }
  }

  def blockUntilEmpty(index: String): Unit = {
    blockUntil(s"Expected empty index $index") { () =>
      elasticClient
        .execute {
          search(Indexes(index)).size(0)
        }
        .complete() match {
        case Success(s) => s.result.totalHits == 0
        case Failure(f) => throw f
      }
    }
  }

  def blockUntilIndexExists(index: String): Unit = {
    blockUntil(s"Expected exists index $index") { () =>
      doesIndexExists(index)
    }
  }

  def blockUntilIndexNotExists(index: String): Unit = {
    blockUntil(s"Expected not exists index $index") { () =>
      !doesIndexExists(index)
    }
  }

  def blockUntilAliasExists(alias: String): Unit = {
    blockUntil(s"Expected exists alias $alias") { () =>
      doesAliasExists(alias)
    }
  }

  def blockUntilDocumentHasVersion(
    index: String,
    _type: String,
    id: String,
    version: Long
  ): Unit = {
    blockUntil(s"Expected document $id to have version $version") { () =>
      elasticClient
        .execute {
          get(index, id)
        }
        .complete() match {
        case Success(s) => s.result.version == version
        case Failure(f) => throw f
      }
    }
  }
}
