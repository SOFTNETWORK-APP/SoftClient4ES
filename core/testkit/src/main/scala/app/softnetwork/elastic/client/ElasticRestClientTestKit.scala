package app.softnetwork.elastic.client

import app.softnetwork.concurrent.scalatest.CompletionTestKit
import app.softnetwork.serialization.commonFormats
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.HttpHost
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Request, Response, RestClient}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.slf4j.Logger

import scala.io.Source
import scala.util.Try

trait ElasticRestClientTestKit extends CompletionTestKit { _: { def log: Logger } =>
  implicit def formats: Formats = commonFormats

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

  lazy val restClient: RestClient =
    RestClient
      .builder(
        HttpHost.create(elasticURL)
      )
      .build()

  def createIndexTemplate(
    templateName: String,
    indexPatterns: List[String],
    settings: Map[String, Any]
  ): Response = {
    val payload = Map(
      "index_patterns" -> indexPatterns,
      "settings"       -> settings
    )
    val request = new Request("PUT", s"/_template/$templateName")
    request.setEntity(new NStringEntity(write(payload), ContentType.APPLICATION_JSON))
    restClient.performRequest(request)
  }

  def doesIndexExists(index: String): Boolean = {
    val request = new Request("HEAD", s"/$index")
    val response = restClient.performRequest(request)
    response.getStatusLine.getStatusCode == 200
  }

  def doesAliasExists(alias: String): Boolean = {
    val request = new Request("HEAD", s"/_alias/$alias")
    val response = restClient.performRequest(request)
    response.getStatusLine.getStatusCode == 200
  }

  def createIndex(index: String): Response = {
    val request = new Request("PUT", s"/$index")
    restClient.performRequest(request)
  }

  def deleteIndex(index: String): Unit = {
    if (doesIndexExists(index)) {
      val request = new Request("DELETE", s"/$index")
      restClient.performRequest(request)
    }
  }

  def getDocument(index: String, id: String): Option[JValue] = {
    val request = new Request("GET", s"/$index/_doc/$id")
    val response = restClient.performRequest(request)
    val json = EntityUtils.toString(response.getEntity)
    val parsed = parse(json)
    (parsed \ "found").extractOpt[Boolean].filter(identity).map(_ => parsed)
  }

  def documentExists(index: String, id: String): Boolean = {
    getDocument(index, id).isDefined
  }

  def isIndexOpened(index: String): Boolean = {
    Try {
      val res = restClient.performRequest(new Request("GET", "/_cluster/state"))
      val body = Source.fromInputStream(res.getEntity.getContent).mkString
      val json = parse(body)
      (json \ "metadata" \ "indices" \ index \ "state").extractOpt[String].contains("open")
    }.getOrElse(false)
  }

  def isIndexClosed(name: String): Boolean = {
    doesIndexExists(name) && !isIndexOpened(name)
  }

  def truncateIndex(index: String): Unit = {
    deleteIndex(index)
    createIndex(index)
    blockUntilEmpty(index)
  }

  def refresh(index: String = "_all"): Response = {
    val request = new Request("POST", s"/$index/_refresh")
    restClient.performRequest(request)
  }

  def searchAll(indices: Set[String]): JValue = {
    val payload = Map("query" -> Map("match_all" -> Map.empty[String, String]))
    val request = new Request("POST", s"/${indices.mkString(",")}/_search")
    request.setEntity(new NStringEntity(write(payload), ContentType.APPLICATION_JSON))
    val response = restClient.performRequest(request)
    val json = EntityUtils.toString(response.getEntity)
    Console.err.println(s"Search response: $json")
    parse(json)
  }

  def searchCount(index: String): Long = {
    val hits = searchAll(Set(index)) \ "hits" \ "total"
    (hits \ "value").extractOpt[Long] match {
      case Some(count) => count
      case None        =>
        // For ES versions < 7.3, the total count is not an object but a number
        hits.extractOpt[Long].getOrElse(0L)
    }
  }

  def blockUntil(explain: String)(predicate: () => Boolean): Unit = {
    blockUntil(explain, 16, 200)(predicate)
  }

  def blockUntilGreen(): Unit = {
    blockUntil("Expected cluster to have green status") { () =>
      Try {
        val res = restClient.performRequest(new Request("GET", "/_cluster/health"))
        val body = Source.fromInputStream(res.getEntity.getContent).mkString
        val json = parse(body)
        (json \ "status").extract[String].toUpperCase == "GREEN"
      }.getOrElse(false)
    }
  }

  def blockUntilCount(expected: Long, index: String): Unit = {
    blockUntil(s"Expected count of $expected") { () =>
      searchCount(index) >= expected
    }
  }

  def blockUntilExactCount(expected: Long, index: String): Unit = {
    blockUntil(s"Expected exact count of $expected") { () =>
      searchCount(index) == expected
    }
  }

  def blockUntilEmpty(index: String): Unit = {
    blockUntil(s"Expected empty index $index") { () =>
      searchCount(index) == 0
    }
  }

  def blockUntilIndexExists(index: String): Unit = {
    blockUntil(s"Index $index should exist") { () =>
      doesIndexExists(index)
    }
  }

  def blockUntilIndexNotExists(index: String): Unit = {
    blockUntil(s"Index $index should not exist") { () =>
      !doesIndexExists(index)
    }
  }

  def blockUntilAliasExists(alias: String): Unit = {
    blockUntil(s"Alias $alias should exist") { () =>
      doesAliasExists(alias)
    }
  }

  def blockUntilDocumentExists(id: String, index: String): Unit = {
    blockUntil(s"Expected to find document $id in index $index") { () =>
      getDocument(index, id).isDefined
    }
  }

  def blockUntilDocumentHasVersion(index: String, id: String, version: Long): Unit = {
    blockUntil(s"Document $id should have version $version") { () =>
      getDocument(index, id).exists(json => (json \ "_version").extractOpt[Long].contains(version))
    }
  }
}
