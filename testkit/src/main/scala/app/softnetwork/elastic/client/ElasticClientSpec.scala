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
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.model.{Binary, Child, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.elastic.sql.schema.{Table, TableAlias}
import app.softnetwork.persistence._
import app.softnetwork.persistence.person.model.Person
import com.fasterxml.jackson.core.JsonParseException
import com.google.gson.JsonParser
import org.json4s.JArray
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.io.ByteArrayInputStream
import _root_.java.nio.file.{Files, Paths}
import _root_.java.time.format.DateTimeFormatter
import _root_.java.util.UUID
import _root_.java.util.concurrent.TimeUnit
import java.time.temporal.Temporal
import java.time.{LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  import ElasticProviders._

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  lazy val pClient: ElasticProvider[Person] = new PersonProvider(
    elasticConfig
  )
  lazy val sClient: ElasticProvider[Sample] = new SampleProvider(
    elasticConfig
  )
  lazy val bClient: ElasticProvider[Binary] = new BinaryProvider(
    elasticConfig
  )
  lazy val parentClient: ElasticProvider[Parent] = new ParentProvider(
    elasticConfig
  )

  import scala.language.implicitConversions

  implicit def toSQLQuery(sqlQuery: String): SelectStatement = SelectStatement(sqlQuery)

  implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
    Source.fromIterator(() => list.iterator)

  override def beforeAll(): Unit = {
    super.beforeAll()
    pClient.createIndex("person", mappings = None, aliases = Nil)
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  private val persons: List[String] = List(
    """ { "uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0} """,
    """ { "uuid": "A14", "name": "Moe Szyslak",   "birthDate": "1967-11-21", "childrenCount": 0} """,
    """ { "uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09", "childrenCount": 0} """
  )

  private val personsWithUpsert =
    persons :+ """ { "uuid": "A16", "name": "Barney Gumble2", "birthDate": "1969-05-09", "children": [{ "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09"}, { "parentId": "A16", "name": "Josh Gumble", "birthDate": "2002-05-09"}], "childrenCount": 2 } """

  val children: List[String] = List(
    """ { "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09"} """,
    """ { "parentId": "A16", "name": "Josh Gumble", "birthDate": "1999-05-09"} """
  )

  "Creating an index and then delete it" should "work fine" in {
    pClient.createIndex("create_delete", mappings = None, aliases = Nil)
    blockUntilIndexExists("create_delete")
    "create_delete" should beCreated

    pClient.deleteIndex("create_delete")
    blockUntilIndexNotExists("create_delete")
    "create_delete" should not(beCreated())
  }

  "Creating an index with mappings and aliases" should "work fine" in {
    val mappings =
      """{
        |    "properties": {
        |        "birthDate": {
        |            "type": "date"
        |        },
        |        "uuid": {
        |            "type": "keyword"
        |        },
        |        "name": {
        |            "type": "text",
        |            "analyzer": "ngram_analyzer",
        |            "search_analyzer": "search_analyzer",
        |            "fields": {
        |                "raw": {
        |                    "type": "keyword"
        |                },
        |                "fr": {
        |                    "type": "text",
        |                    "analyzer": "french"
        |                }
        |            }
        |        }
        |    },
        |    "_meta": {
        |        "primary_key": ["uuid"],
        |        "partition_by": {
        |            "column": "birthDate",
        |            "granularity": "M"
        |        }
        |    }
        |}""".stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    val aliases: Seq[TableAlias] = Seq(
      TableAlias(table = "create_mappings_aliases", alias = "create_mappings_aliases_alias1"),
      TableAlias(
        table = "create_mappings_aliases",
        alias = "create_mappings_aliases_alias2",
        filter = ListMap("term" -> ListMap("name.raw" -> "Homer Simpson"))
      )
    )

    pClient
      .createIndex("create_mappings_aliases", mappings = Some(mappings), aliases = aliases)
      .get shouldBe true
    "create_mappings_aliases" should beCreated()

    val index: Option[Index] = pClient.getIndex("create_mappings_aliases").get

    index match {
      case Some(idx) =>
        val table: Table = idx.schema
        log.info(table.sql)
        table.columns.size shouldBe 3
        table.columns.map(_.name) should contain allOf ("uuid", "name", "birthDate")
        table.primaryKey should contain("uuid")
        table.partitionBy match {
          case Some(partitionBy) =>
            partitionBy.column shouldBe "birthDate"
            partitionBy.granularity.sql shouldBe "MONTH"
          case None => fail("Partition by not found")
        }
        val tableAliases = table.aliases.keys
        tableAliases should contain("create_mappings_aliases_alias1")
        tableAliases should contain("create_mappings_aliases_alias2")
      case None => fail("Index not found")
    }

    pClient.deleteIndex("create_mappings_aliases").get shouldBe true
    "create_mappings_aliases" should not(beCreated())
  }

  "Checking for an alias that does not exist" should "return false" in {
    pClient.aliasExists("person_alias").get shouldBe false
  }

  "Adding an alias and then removing it" should "work" in {
    pClient.addAlias("person", "person_alias")

    pClient.aliasExists("person_alias").get shouldBe true

    pClient.getAliases("person") match {
      case ElasticSuccess(aliases) =>
        aliases.map(_.alias) should contain("person_alias")
      case ElasticFailure(elasticError) =>
        fail(elasticError.fullMessage)
    }

    pClient.removeAlias("person", "person_alias")

    doesAliasExists("person_alias") shouldBe false
  }

  "Toggle refresh" should "work" in {
    pClient.toggleRefresh("person", enable = false).get shouldBe true
    var settings = pClient.loadSettings("person").get
    JsonParser
      .parseString(settings)
      .getAsJsonObject
      .get("refresh_interval")
      .getAsString shouldBe "-1"

    pClient.toggleRefresh("person", enable = true).get shouldBe true
    settings = pClient.loadSettings("person").get
    JsonParser
      .parseString(settings)
      .getAsJsonObject
      .get("refresh_interval")
      .getAsString shouldBe "1s"
  }

  "Opening an index and then closing it" should "work" in {
    pClient.openIndex("person")

    isIndexOpened("person") shouldBe true

    pClient.closeIndex("person")
    isIndexClosed("person") shouldBe true
  }

  "Updating number of replicas" should "work" in {
    pClient.setReplicas("person", 3)

    JsonParser
      .parseString(pClient.loadSettings("person").get)
      .getAsJsonObject
      .get("number_of_replicas")
      .getAsString shouldBe "3"

    pClient.setReplicas("person", 0)
    JsonParser
      .parseString(pClient.loadSettings("person").get)
      .getAsJsonObject
      .get("number_of_replicas")
      .getAsString shouldBe "0"
  }

  "Setting a mapping" should "work" in {
    pClient.createIndex("person_mapping", mappings = None, aliases = Nil)
    blockUntilIndexExists("person_mapping")
    "person_mapping" should beCreated()

    val mapping =
      """{
        |    "properties": {
        |        "birthDate": {
        |            "type": "date"
        |        },
        |        "uuid": {
        |            "type": "keyword"
        |        },
        |        "name": {
        |            "type": "text",
        |            "analyzer": "ngram_analyzer",
        |            "search_analyzer": "search_analyzer",
        |            "fields": {
        |                "raw": {
        |                    "type": "keyword"
        |                },
        |                "fr": {
        |                    "type": "text",
        |                    "analyzer": "french"
        |                }
        |            }
        |        }
        |    }
        |}""".stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    pClient.setMapping("person_mapping", mapping).get shouldBe true

    val properties = pClient.getMappingProperties("person_mapping").get
    log.info(s"properties: $properties")
    MappingComparator.isMappingDifferent(
      properties,
      mapping
    ) shouldBe false

    implicit val bulkOptions: BulkOptions = BulkOptions("person_mapping")
    val result = pClient.bulk[String](persons, identity, idKey = Some(Set("uuid"))).get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person_mapping")

    indices should contain only "person_mapping"

    blockUntilCount(3, "person_mapping")

    "person_mapping" should haveCount(3)

    pClient.searchAs[Person](
      "select uuid, name, birthDate, createdDate, lastUpdated from person_mapping"
    ) match {
      case ElasticSuccess(value) =>
        value match {
          case r if r.size == 3 =>
            r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
          case other => fail(other.toString)
        }
      case ElasticFailure(elasticError) =>
        fail(elasticError.fullMessage)
    }

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person_mapping where uuid = 'A16'"
      )
      .get match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person_mapping where match (name) against ('gum')"
      )
      .get match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person_mapping where uuid <> 'A16' and match (name) against ('gum')"
      )
      .get match {
      case r if r.isEmpty =>
      case other          => fail(other.toString)
    }
  }

  "Updating a mapping" should "work" in {
    pClient.createIndex("person_migration", mappings = None, aliases = Nil).get shouldBe true
    val mapping =
      """{
        |  "properties": {
        |    "name": {
        |      "type": "keyword"
        |    },
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    pClient.setMapping("person_migration", mapping).get shouldBe true
    blockUntilIndexExists("person_migration")
    "person_migration" should beCreated()

    implicit val bulkOptions: BulkOptions = BulkOptions("person_migration")
    val result = pClient
      .bulk[String](
        Source.fromIterator(() => persons.iterator),
        identity,
        idKey = Some(Set("uuid"))
      )
      .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person_migration")

    indices should contain only "person_migration"

    blockUntilCount(3, "person_migration")

    "person_migration" should haveCount(3)

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person_migration where match (name) against ('gum')"
      )
      .get match {
      case r if r.isEmpty =>
      case other          => fail(other.toString)
    }

    val newMapping =
      """{
        |  "properties": {
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "name": {
        |      "type": "text",
        |      "analyzer": "ngram_analyzer",
        |      "search_analyzer": "search_analyzer",
        |      "fields": {
        |        "raw": {
        |          "type": "keyword"
        |        },
        |        "fr": {
        |          "type": "text",
        |          "analyzer": "french"
        |        }
        |      }
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    },
        |    "children": {
        |      "type": "nested",
        |      "include_in_parent": true,
        |      "properties": {
        |        "name": {
        |          "type": "keyword"
        |        },
        |        "birthDate": {
        |          "type": "date"
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    pClient.shouldUpdateMapping("person_migration", newMapping).get shouldBe true
    pClient.updateMapping("person_migration", newMapping).get shouldBe true

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person_migration where match (name) against ('gum')"
      )
      .get match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

  }

  "Bulk index valid json without id key and suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions =
      BulkOptions("person1", "person", 2) // small chunk size to test multiple bulk requests
    val result = pClient.bulk[String](persons, identity).get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices

    indices should contain only "person1"

    blockUntilCount(3, "person1")

    "person1" should haveCount(3)

    pClient
      .searchAs[Person]("select uuid, name, birthDate, createdDate, lastUpdated from person1")
      .get match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
      case other => fail(other.toString)
    }

  }

  "Bulk index valid json with an id key but no suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person2")
    val result = pClient.bulk[String](persons, identity, idKey = Some(Set("uuid"))).get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person2").get shouldBe true

    indices should contain only "person2"

    blockUntilCount(3, "person2")

    "person2" should haveCount(3)

    pClient
      .searchAs[Person]("select uuid, name, birthDate, createdDate, lastUpdated from person2")
      .get match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
      case other => fail(other.toString)
    }

    searchAll(Set("person2")) \ "hits" \ "hits" match {
      case JArray(hits) =>
        hits.foreach { h =>
          (h \ "_id").extract[String] shouldBe (h \ "_source" \ "uuid").extract[String]
        }
      case other => fail(s"Unexpected result: $other")
    }

  }

  "Bulk index valid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person", "person")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid")),
          suffixDateKey = Some("birthDate")
        )
        .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain allOf ("person-1967-11-21", "person-1969-05-09")

    blockUntilCount(2, "person-1967-11-21")
    blockUntilCount(1, "person-1969-05-09")

    "person-1967-11-21" should haveCount(2)
    "person-1969-05-09" should haveCount(1)

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person-1967-11-21, person-1969-05-09"
      )
      .get match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
      case other => fail(other.toString)
    }

    searchAll(Set("person-1967-11-21", "person-1969-05-09")) \ "hits" \ "hits" match {
      case JArray(hits) =>
        hits.foreach { h =>
          (h \ "_id").extract[String] shouldBe (h \ "_source" \ "uuid").extract[String]
        }
      case other => fail(s"Unexpected result: $other")
    }

  }

  "Bulk index invalid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person_error")
    intercept[JsonParseException] {
      val invalidJson = persons :+ "fail"
      pClient.bulk[String](invalidJson, identity).get
    }
  }

  "Bulk upsert valid json with an id key but no suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person4")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person4"

    blockUntilCount(3, "person4")

    "person4" should haveCount(3)

    pClient
      .searchAs[Person]("select uuid, name, birthDate, createdDate, lastUpdated from person4")
      .get match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
      case other => fail(other.toString)
    }

    searchAll(Set("person4")) \ "hits" \ "hits" match {
      case JArray(hits) =>
        hits.foreach { h =>
          (h \ "_id").extract[String] shouldBe (h \ "_source" \ "uuid").extract[String]
        }
      case other => fail(s"Unexpected result: $other")
    }

  }

  "Bulk upsert valid json with an id key and a suffix key" should "work" in {
    pClient.createIndex("person5", mappings = None, aliases = Nil).get shouldBe true
    implicit val bulkOptions: BulkOptions = BulkOptions("person5")
    val result = pClient
      .bulk[String](
        personsWithUpsert,
        identity,
        idKey = Some(Set("uuid")),
        suffixDateKey = Some("birthDate"),
        update = Some(true)
      )
      .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true // personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain allOf ("person5-1967-11-21", "person5-1969-05-09")

    blockUntilCount(2, "person5-1967-11-21")
    blockUntilCount(1, "person5-1969-05-09")

    "person5-1967-11-21" should haveCount(2)
    "person5-1969-05-09" should haveCount(1)

    pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person5-1967-11-21, person5-1969-05-09"
      )
      .get match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
      case other => fail(other.toString)
    }

    searchAll(Set("person5-1967-11-21", "person5-1969-05-09")) \ "hits" \ "hits" match {
      case JArray(hits) =>
        hits.foreach { h =>
          (h \ "_id").extract[String] shouldBe (h \ "_source" \ "uuid").extract[String]
        }
      case other => fail(s"Unexpected result: $other")
    }

  }

  "Count" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person6")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person6"

    blockUntilCount(3, "person6")

    "person6" should haveCount(3)

    import scala.collection.immutable._

    pClient
      .count(ElasticQuery("{}", Seq[String]("person6"), Seq[String]()))
      .get
      .getOrElse(0d)
      .toInt should ===(3)

    pClient.countAsync(ElasticQuery("{}", Seq[String]("person6"), Seq[String]())).complete() match {
      case Success(s) => s.get.getOrElse(0d).toInt should ===(3)
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Search" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person7")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person7"

    blockUntilCount(3, "person7")

    "person7" should haveCount(3)

    val r1 = pClient
      .searchAs[Person]("select uuid, name, birthDate, createdDate, lastUpdated from person7")
      .get
    r1.size should ===(3)
    r1.map(_.uuid) should contain allOf ("A12", "A14", "A16")

    pClient.searchAsyncAs[Person](
      "select uuid, name, birthDate, createdDate, lastUpdated from person7"
    ) onComplete {
      case Success(s) =>
        val r = s.get
        r.size should ===(3)
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case Failure(f) => fail(f.getMessage)
    }

    val r2 = pClient
      .searchAs[Person](
        "select uuid, name, birthDate, createdDate, lastUpdated from person7 where _id=\"A16\""
      )
      .get
    r2.size should ===(1)
    r2.map(_.uuid) should contain("A16")

    pClient.searchAsyncAs[Person](
      "select uuid, name, birthDate, createdDate, lastUpdated from person7 where _id=\"A16\""
    ) onComplete {
      case Success(s) =>
        val r = s.get
        r.size should ===(1)
        r.map(_.uuid) should contain("A16")
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Get all" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person8")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person8"

    blockUntilCount(3, "person8")

    "person8" should haveCount(3)

    val response = pClient
      .searchAs[Person]("select uuid, name, birthDate, createdDate, lastUpdated from person8")
      .get

    response.size should ===(3)

  }

  "Get" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person9")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person9"

    blockUntilCount(3, "person9")

    "person9" should haveCount(3)

    val response = pClient.getAs[Person]("A16", Some("person9")).get

    response.isDefined shouldBe true
    response.get.uuid shouldBe "A16"

    pClient.getAsyncAs[Person]("A16", Some("person9")).complete() match {
      case Success(s) =>
        val r = s.get
        r.isDefined shouldBe true
        r.get.uuid shouldBe "A16"
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Index" should "work" in {
    val uuid = UUID.randomUUID().toString
    val index = s"sample-$uuid"
    sClient.createIndex(index, mappings = None, aliases = Nil).get shouldBe true
    val sample = Sample(uuid)
    val result = sClient.indexAs(sample, uuid, Some(index)).get
    result shouldBe true

    sClient.toggleRefresh(index, enable = true).get shouldBe true
    sClient.indexAsyncAs(sample, uuid, Some(index), wait = true).complete() match {
      case Success(r) => r.get shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    val result2 = sClient.getAs[Sample](uuid, Some(index)).get
    result2 match {
      case Some(r) =>
        r.uuid shouldBe uuid
      case _ =>
        fail("Sample not found")
    }
  }

  "Update" should "work" in {
    val uuid = UUID.randomUUID().toString
    val index = s"sample-$uuid"
    sClient.createIndex(index, mappings = None, aliases = Nil).get shouldBe true
    val sample = Sample(uuid)
    val result = sClient.updateAs(sample, uuid, Some(index)).get
    result shouldBe true

    sClient.toggleRefresh(index, enable = true).get shouldBe true
    sClient.updateAsyncAs(sample, uuid, Some(index), wait = true).complete() match {
      case Success(r) => r.get shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    val result2 = sClient.getAs[Sample](uuid, Some(index)).get
    result2 match {
      case Some(r) =>
        r.uuid shouldBe uuid
      case _ =>
        fail("Sample not found")
    }
  }

  "Delete" should "work" in {
    val uuid = UUID.randomUUID().toString
    val index = s"sample-$uuid"
    sClient.createIndex(index, mappings = None, aliases = Nil).get shouldBe true
    val sample = Sample(uuid)
    val result = sClient.indexAs(sample, uuid, Some(index)).get
    result shouldBe true

    sClient.delete(sample.uuid, index).get shouldBe true

    //blockUntilEmpty(index)

    sClient.getAs[Sample](uuid, Some(index)) match {
      case ElasticSuccess(_)     => fail("Sample should have been deleted")
      case ElasticFailure(error) => error.statusCode.getOrElse(0) shouldBe 404
    }
  }

  "Delete asynchronously" should "work" in {
    val uuid = UUID.randomUUID().toString
    val index = s"sample-$uuid"
    sClient.createIndex(index, mappings = None, aliases = Nil).get shouldBe true
    val sample = Sample(uuid)
    val result = sClient.indexAs(sample, uuid, Some(index)).get
    result shouldBe true

    sClient.toggleRefresh(index, enable = true).get shouldBe true
    sClient.deleteAsync(sample.uuid, index, wait = true).complete() match {
      case Success(r) => r.get shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    // blockUntilEmpty(index)

    sClient.getAs[Sample](uuid, Some(index)) match {
      case ElasticSuccess(_)     => fail("Sample should have been deleted")
      case ElasticFailure(error) => error.statusCode.getOrElse(0) shouldBe 404
    }
  }

  "Index binary data" should "work" in {
    bClient.createIndex("binaries", mappings = None, aliases = Nil).get shouldBe true
    val mapping =
      """{
        |  "properties": {
        |    "lastUpdated": {
        |      "type": "date"
        |    },
        |    "createdDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "content": {
        |      "type": "binary"
        |    },
        |    "md5": {
        |      "type": "keyword"
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    bClient.setMapping("binaries", mapping).get shouldBe true
    bClient.shouldUpdateMapping("binaries", mapping).get shouldBe false
    for (uuid <- Seq("png", "jpg", "pdf")) {
      Try(
        Paths.get(Thread.currentThread().getContextClassLoader.getResource(s"avatar.$uuid").getPath)
      ) match {
        case Success(path) =>
          import app.softnetwork.utils.Base64Tools._
          import app.softnetwork.utils.HashTools._
          import app.softnetwork.utils.ImageTools._
          val encoded = encodeImageBase64(path).getOrElse("")
          val binary = Binary(
            uuid,
            content = encoded,
            md5 = hashStream(new ByteArrayInputStream(decodeBase64(encoded))).getOrElse("")
          )
          bClient.indexAs(binary, uuid).get shouldBe true
          bClient.getAs[Binary](uuid).get match {
            case Some(result) =>
              val decoded = decodeBase64(result.content)
              val out = Paths.get(s"/tmp/${path.getFileName}")
              val fos = Files.newOutputStream(out)
              fos.write(decoded)
              fos.close()
              hashFile(out).getOrElse("") shouldBe binary.md5
            case _ => fail("no result found for \"" + uuid + "\"")
          }
        case _ =>
          fail(s"Resource avatar.$uuid not found")
      }
    }
  }

  "Aggregations" should "work" in {
    pClient.createIndex("person10", mappings = None, aliases = Nil).get shouldBe true
    val mapping =
      """{
        |  "properties": {
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "name": {
        |      "type": "keyword"
        |    },
        |    "children": {
        |      "type": "nested",
        |      "include_in_parent": true,
        |      "properties": {
        |        "name": {
        |          "type": "keyword"
        |        },
        |        "birthDate": {
        |          "type": "date"
        |        }
        |      }
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    log.info(s"mapping: $mapping")
    pClient.setMapping("person10", mapping).get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person10")
    val result =
      pClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    result.failedCount shouldBe 0
    result.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person10").get shouldBe true

    indices should contain only "person10"

    blockUntilCount(3, "person10")

    "person10" should haveCount(3)

    pClient.getAs[Person]("A16", Some("person10")).get match {
      case Some(p) =>
        p.uuid shouldBe "A16"
        p.birthDate shouldBe "1969-05-09"
      case None => fail("Person A16 not found")
    }

    // test distinct count aggregation
    pClient
      .aggregate(
        "select count(distinct p.uuid) as c from person10 p"
      )
      .complete() match {
      case Success(s) =>
        s.get.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test count aggregation
    pClient.aggregate("select count(p.uuid) as c from person10 p").complete() match {
      case Success(s) =>
        s.get.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test max aggregation on date field
    pClient.aggregate("select max(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        // The maximum date should be the latest birthDate in the dataset
        s.get.headOption match {
          case Some(value) =>
            value.asDoubleSafe.orElse(value.asStringSafe).toOption match {
              case Some(d: Double) =>
                d should ===(
                  LocalDate.parse("1969-05-09").toEpochDay.toDouble * 3600 * 24 * 1000
                )
              case Some(s: String) =>
                s should ===("1969-05-09T00:00:00.000Z")
              case _ => fail(s"Unexpected value type: ${value.prettyPrint}")
            }
          case None => fail("No result found for max aggregation")
        }
      case Failure(f) => fail(f.getMessage)
    }

    // test min aggregation on date field
    pClient.aggregate("select min(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        // The minimum date should be the earliest birthDate in the dataset
        s.get.headOption match {
          case Some(value) =>
            value.asDoubleSafe.orElse(value.asStringSafe).toOption match {
              case Some(d: Double) =>
                d should ===(
                  LocalDate.parse("1967-11-21").toEpochDay.toDouble * 3600 * 24 * 1000
                )
              case Some(s: String) =>
                s should ===("1967-11-21T00:00:00.000Z")
              case _ => fail(s"Unexpected value type: ${value.prettyPrint}")
            }
          case None => fail("No result found for min aggregation")
        }
      case Failure(f) => fail(f.getMessage)
    }

    // test avg aggregation on date field
    pClient.aggregate("select avg(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        // The average date should be the midpoint between the min and max dates
        s.get.headOption match {
          case Some(value) =>
            value.asDoubleSafe.orElse(value.asStringSafe).toOption match {
              case Some(d: Double) =>
                d should ===(
                  LocalDateTime
                    .parse("1968-05-17T08:00:00.000Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli
                    .toDouble
                )
              case Some(s: String) =>
                s should ===("1968-05-17T08:00:00.000Z")
              case _ => fail(s"Unexpected value type: ${value.prettyPrint}")
            }
          case None => fail("No result found for avg aggregation")
        }
      case Failure(f) => fail(f.getMessage)
    }

    // test sum aggregation on integer field
    pClient
      .aggregate(
        "select sum(p.childrenCount) as c from person10 p"
      )
      .complete() match {
      case Success(s) =>
        s.get.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(2d)
      case Failure(f) => fail(f.getMessage)
    }

    // test first aggregation on date field
    pClient.aggregate("select first(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.get.headOption match {
          case Some(value) =>
            value.asDoubleSafe.orElse(value.asStringSafe).orElse(value.asMapSafe).toOption match {
              case Some(d: Double) =>
                d should ===(
                  LocalDate.parse("1967-11-21").toEpochDay.toDouble * 3600 * 24 * 1000
                )
              case Some(s: String) =>
                s should ===("1967-11-21T00:00:00.000Z")
              case Some(m: Map[_, _]) =>
                // Elasticsearch 7.14+ returns an object for first/last aggregations on date fields
                m.asInstanceOf[Map[String, Any]].get("birthDate") match {
                  case Some(t: Temporal) =>
                    t should ===(LocalDate.parse("1967-11-21"))
                  case Some(d: Double) =>
                    d should ===(
                      LocalDate
                        .parse("1967-11-21")
                        .toEpochDay
                        .toDouble * 3600 * 24 * 1000
                    )
                  case Some(s: String) =>
                    s should ===("1967-11-21T00:00:00.000Z")
                  case other => fail(s"Unexpected value type: $other")
                }
            }
          case None => fail("No result found for first aggregation")
        }
      case Failure(f) => fail(f.getMessage)
    }

    // test last aggregation on date field
    pClient.aggregate("select last(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.get.headOption match {
          case Some(value) =>
            value.asDoubleSafe.orElse(value.asStringSafe).orElse(value.asMapSafe).toOption match {
              case Some(d: Double) =>
                d should ===(
                  LocalDate.parse("1969-05-09").toEpochDay.toDouble * 3600 * 24 * 1000
                )
              case Some(s: String) =>
                s should ===("1969-05-09T00:00:00.000Z")
              case Some(m: Map[_, _]) =>
                // Elasticsearch 7.14+ returns an object for first/last aggregations on date fields
                m.asInstanceOf[Map[String, Any]].get("birthDate") match {
                  case Some(t: Temporal) =>
                    t should ===(LocalDate.parse("1969-05-09"))
                  case Some(d: Double) =>
                    d should ===(
                      LocalDate
                        .parse("1969-05-09")
                        .toEpochDay
                        .toDouble * 3600 * 24 * 1000
                    )
                  case Some(s: String) =>
                    s should ===("1969-05-09T00:00:00.000Z")
                  case other => fail(s"Unexpected value type: $other")
                }
            }
          case None => fail("No result found for last aggregation")
        }
      case Failure(f) => fail(f.getMessage)
    }

    // test array aggregation on String field
    pClient
      .aggregate(
        "select array(child.name) as names from person10 p JOIN UNNEST(p.children) as child LIMIT 10"
      )
      .complete() match {
      case Success(s) =>
        val names = s.get.headOption.flatMap(_.asSeqSafe.toOption).getOrElse(Seq.empty).map {
          case s: String => s
          case other     => fail(s"Unexpected name type: $other")
        }
        names should contain allOf ("Josh Gumble", "Steve Gumble")
      case Failure(f) => fail(f.getMessage)
    }

    // test array aggregation on date field
    pClient
      .aggregate(
        "select array(DISTINCT child.birthDate) as birthDates from person10 p JOIN UNNEST(p.children) as child LIMIT 10"
      )
      .complete() match {
      case Success(s) =>
        val birthDates = s.get.headOption
          .flatMap(_.asSeqSafe.toOption)
          .getOrElse(Seq.empty)
          .map {
            case t: Temporal => t
            case other       => fail(s"Unexpected birthDate type: $other")
          }
          .map(_.toString)
        birthDates.nonEmpty shouldBe true
        birthDates should contain allOf ("1999-05-09", "2002-05-09") // LocalDate instances sorted ASC
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Nested queries" should "work" in {
    parentClient.createIndex("parent", mappings = None, aliases = Nil).get shouldBe true
    val mapping =
      """{
        |  "properties": {
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "name": {
        |      "type": "keyword"
        |    },
        |    "createdDate": {
        |      "type": "date",
        |      "null_value": "1970-01-01"
        |    },
        |    "lastUpdated": {
        |      "type": "date",
        |      "null_value": "1970-01-01"
        |    },
        |    "children": {
        |      "type": "nested",
        |      "include_in_parent": true,
        |      "properties": {
        |        "name": {
        |          "type": "keyword"
        |        },
        |        "birthDate": {
        |          "type": "date"
        |        },
        |        "parentId": {
        |          "type": "keyword"
        |        }
        |      }
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    log.info(s"mapping: $mapping")
    parentClient.setMapping("parent", mapping).get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("parent")
    val bulkResult =
      parentClient
        .bulk[String](
          personsWithUpsert,
          identity,
          idKey = Some(Set("uuid")),
          update = Some(true)
        )
        .get
    bulkResult.failedCount shouldBe 0
    bulkResult.successCount > 0 shouldBe true //personsWithUpsert.size
    val indices = bulkResult.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    parentClient.flush("parent").get shouldBe true
    parentClient.refresh("parent").get shouldBe true

    indices should contain only "parent"

    blockUntilCount(3, "parent")

    "parent" should haveCount(3)

    val parents = parentClient.searchAs[Parent](
      """SELECT
        | p.uuid,
        | p.name,
        | p.birthDate,
        | children.name,
        | children.birthDate,
        | children.parentId
        | FROM
        | parent as p
        | JOIN UNNEST(p.children) as children
        |WHERE
        | children.name is not null
        |""".stripMargin
    )
    parents match {
      case ElasticSuccess(ps) =>
        ps.size shouldBe 1
        ps.head.children.size shouldBe 2
      case ElasticFailure(error) =>
        error.cause match {
          case Some(cause) => log.error("Error during search", cause)
          case None        =>
        }
        fail(s"Error during search: ${error.message}")
    }

    val results = parentClient
      .searchWithInnerHits[Parent, Child](
        """SELECT
          | p.uuid,
          | p.name,
          | p.birthDate,
          | p.children,
          | inner_children.name,
          | inner_children.birthDate,
          | inner_children.parentId
          | FROM
          | parent as p
          | JOIN UNNEST(p.children) as inner_children
          |WHERE
          | inner_children.name is not null AND p.uuid = 'A16'
          |""".stripMargin,
        "inner_children"
      )
      .get
    results.size shouldBe 1
    val result = results.head
    result._1.uuid shouldBe "A16"
    result._1.children.size shouldBe 2
    result._2.size shouldBe 2
    result._2.map(_.name) should contain allOf ("Steve Gumble", "Josh Gumble")
    result._2.map(
      _.birthDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    ) should contain allOf ("1999-05-09", "2002-05-09")
    result._2.map(_.parentId) should contain only "A16"

    val searchResults = parentClient
      .searchAs[Parent](
        """SELECT
          | p.uuid,
          | p.name,
          | p.birthDate,
          | children.name,
          | children.birthDate,
          | children.parentId
          | FROM
          | parent as p
          | JOIN UNNEST(p.children) as children
          |WHERE
          | children.name is not null AND p.uuid = 'A16'
          |""".stripMargin
      )
      .get
    searchResults.size shouldBe 1
    val searchResult = searchResults.head
    searchResult.uuid shouldBe "A16"
    searchResult.children.size shouldBe 2
    searchResult.children.map(_.name) should contain allOf ("Steve Gumble", "Josh Gumble")
    searchResult.children.map(
      _.birthDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    ) should contain allOf ("1999-05-09", "2002-05-09")
    searchResult.children.map(_.parentId) should contain only "A16"

    val scrollResults: Future[Seq[(Parent, ScrollMetrics)]] = parentClient
      .scrollAs[Parent](
        """SELECT
          | p.uuid,
          | p.name,
          | p.birthDate,
          | children.name,
          | children.birthDate,
          | children.parentId
          | FROM
          | parent as p
          | JOIN UNNEST(p.children) as children
          |WHERE
          | children.name is not null AND p.uuid = 'A16'
          |""".stripMargin,
        ScrollConfig(logEvery = 1)
      )
      .runWith(Sink.seq)
    scrollResults await { rows =>
      val parents = rows.map(_._1)
      parents.size shouldBe 1
      val scrollResult = parents.head
      scrollResult.uuid shouldBe "A16"
      scrollResult.children.size shouldBe 2
      scrollResult.children.map(_.name) should contain allOf ("Steve Gumble", "Josh Gumble")
      scrollResult.children.map(
        _.birthDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      ) should contain allOf ("1999-05-09", "2002-05-09")
      scrollResult.children.map(_.parentId) should contain only "A16"
    }
  }

  "Truncate index" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person_to_truncate")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person_to_truncate"

    blockUntilCount(3, "person_to_truncate")

    "person_to_truncate" should haveCount(3)

    val truncateResult = pClient.truncateIndex("person_to_truncate").get
    truncateResult shouldBe 3L

    "person_to_truncate" should haveCount(0)
  }

  "Delete by query on closed index" should "work depending on ES version" in {
    pClient.createIndex("closed_index_test").get shouldBe true

    pClient.closeIndex("closed_index_test").get shouldBe true

    val deleted = pClient
      .deleteByQuery(
        "closed_index_test",
        """{"query": {"match_all": {}}}"""
      )
      .get

    deleted shouldBe 0L // index is empty anyway
  }

  "Delete by query with invalid JSON" should "fail" in {
    pClient.createIndex("delete_by_query_invalid_json").get shouldBe true
    val result = pClient.deleteByQuery("idx", "{not valid json}")
    result.isFailure shouldBe true
  }

  "Delete by query with invalid SQL" should "fail" in {
    pClient.createIndex("delete_by_query_invalid_sql").get shouldBe true
    val result = pClient.deleteByQuery("delete_by_query_invalid_sql", "DELETE FROM")
    result.isFailure shouldBe true
  }

  "Delete by query with SQL referencing wrong index" should "fail" in {
    pClient.createIndex("index_a").get shouldBe true
    pClient.createIndex("index_b").get shouldBe true
    val result = pClient.deleteByQuery(
      "index_a",
      "DELETE FROM index_b WHERE x = 1"
    )
    result.isFailure shouldBe true
  }

  "Delete by query with valid JSON" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person_to_delete_by_json_query")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person_to_delete_by_json_query"

    blockUntilCount(3, "person_to_delete_by_json_query")

    "person_to_delete_by_json_query" should haveCount(3)

    val deleteByQuery = pClient
      .deleteByQuery("person_to_delete_by_json_query", """{"query": {"match_all": {}}}""")
      .get
    deleteByQuery shouldBe 3L

    "person_to_delete_by_json_query" should haveCount(0)
  }

  "Delete by query with SQL DELETE" should "work" in {
    val mapping =
      """{
        |  "properties": {
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "name": {
        |      "type": "keyword"
        |    },
        |    "children": {
        |      "type": "nested",
        |      "include_in_parent": true,
        |      "properties": {
        |        "name": {
        |          "type": "keyword"
        |        },
        |        "birthDate": {
        |          "type": "date"
        |        }
        |      }
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    log.info(s"mapping: $mapping")
    pClient
      .createIndex("person_to_delete_by_sql_delete_query", mappings = Some(mapping))
      .get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_to_delete_by_sql_delete_query")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => pClient.refresh(index).get) shouldBe true

    indices should contain only "person_to_delete_by_sql_delete_query"

    blockUntilCount(3, "person_to_delete_by_sql_delete_query")

    "person_to_delete_by_sql_delete_query" should haveCount(3)

    val deleteByQuery = pClient
      .deleteByQuery(
        "person_to_delete_by_sql_delete_query",
        """DELETE FROM person_to_delete_by_sql_delete_query WHERE uuid = 'A16'"""
      )
      .get
    deleteByQuery shouldBe 1L

    "person_to_delete_by_sql_delete_query" should haveCount(2)

    pClient
      .deleteByQuery(
        "person_to_delete_by_sql_delete_query",
        "DELETE FROM person_to_delete_by_sql_delete_query"
      )
      .get shouldBe 2L

    "person_to_delete_by_sql_delete_query" should haveCount(0)
  }

  "Delete by query with SQL SELECT" should "work" in {
    val mapping =
      """{
        |  "properties": {
        |    "birthDate": {
        |      "type": "date"
        |    },
        |    "uuid": {
        |      "type": "keyword"
        |    },
        |    "name": {
        |      "type": "keyword"
        |    },
        |    "children": {
        |      "type": "nested",
        |      "include_in_parent": true,
        |      "properties": {
        |        "name": {
        |          "type": "keyword"
        |        },
        |        "birthDate": {
        |          "type": "date"
        |        }
        |      }
        |    },
        |    "childrenCount": {
        |      "type": "integer"
        |    }
        |  }
        |}
      """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")
    log.info(s"mapping: $mapping")
    pClient
      .createIndex("person_to_delete_by_sql_select_query", mappings = Some(mapping))
      .get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_to_delete_by_sql_select_query")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get
    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size
    val indices = result.indices
    indices.forall(index => pClient.refresh(index).get) shouldBe true

    indices should contain only "person_to_delete_by_sql_select_query"

    blockUntilCount(3, "person_to_delete_by_sql_select_query")

    "person_to_delete_by_sql_select_query" should haveCount(3)

    val deleteByQuery = pClient
      .deleteByQuery(
        "person_to_delete_by_sql_select_query",
        """SELECT * FROM person_to_delete_by_sql_select_query WHERE uuid = 'A16'"""
      )
      .get
    deleteByQuery shouldBe 1L

    "person_to_delete_by_sql_select_query" should haveCount(2)

    pClient
      .deleteByQuery(
        "person_to_delete_by_sql_select_query",
        "SELECT * FROM person_to_delete_by_sql_select_query"
      )
      .get shouldBe 2L

    "person_to_delete_by_sql_select_query" should haveCount(0)
  }

  "Update by query with missing index" should "fail with 404" in {
    val result = pClient.updateByQuery(
      "person_update_with_missing_index",
      """{"query": {"match_all": {}}}"""
    )

    result.isFailure shouldBe true
    result.toEither.left.get.statusCode shouldBe Some(404)
  }

  "Update by query with missing pipeline" should "fail with 404" in {
    pClient.createIndex("person_update_with_missing_pipeline").get shouldBe true
    val result = pClient.updateByQuery(
      "person_update_with_missing_pipeline",
      """{"query": {"match_all": {}}}""",
      pipelineId = Some("does-not-exist")
    )

    result.isFailure shouldBe true
    result.toEither.left.get.statusCode shouldBe Some(404)
  }

  "Update by query with invalid SQL" should "fail" in {
    pClient.createIndex("person_update_with_invalid_sql").get shouldBe true
    val result = pClient.updateByQuery(
      "person_update_with_invalid_sql",
      """UPDATE person_update_sql_where WHERE uuid = 'A16'"""
    )

    result.isFailure shouldBe true
  }

  "Update by query with invalid JSON" should "fail" in {
    pClient.createIndex("person_update_with_invalid_json").get shouldBe true
    val result = pClient.updateByQuery(
      "person_update_with_invalid_json",
      """{ invalid json }"""
    )

    result.isFailure shouldBe true
  }

  "Update by query with SQL UPDATE and WHERE" should "update only matching documents" in {
    val mapping =
      """{
        |  "properties": {
        |    "uuid": { "type": "keyword" },
        |    "name": { "type": "keyword" },
        |    "birthDate": { "type": "date" },
        |    "childrenCount": { "type": "integer" }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    pClient
      .createIndex("person_update_sql_where", mappings = Some(mapping))
      .get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_update_sql_where")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get

    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size

    blockUntilCount(3, "person_update_sql_where")
    "person_update_sql_where" should haveCount(3)

    val updated = pClient
      .updateByQuery(
        "person_update_sql_where",
        """UPDATE person_update_sql_where SET name = 'Another Name' WHERE uuid = 'A16'"""
      )
      .get

    updated shouldBe 1L

    pClient.refresh("person_update_sql_where").get shouldBe true

    pClient.getAs[Person]("A16", Some("person_update_sql_where")).get match {
      case Some(doc) => doc.name shouldBe "Another Name"
      case None      => fail("Document A16 not found")
    }
  }

  "Update by query with SQL UPDATE without WHERE" should "update all documents" in {
    val mapping =
      """{
        |  "properties": {
        |    "uuid": { "type": "keyword" },
        |    "name": { "type": "keyword" },
        |    "birthDate": { "type": "date" },
        |    "childrenCount": { "type": "integer" }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    pClient
      .createIndex("person_update_all_sql", mappings = Some(mapping))
      .get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_update_all_sql")
    val result =
      pClient
        .bulk[String](
          persons,
          identity,
          idKey = Some(Set("uuid"))
        )
        .get

    result.failedCount shouldBe 0
    result.successCount shouldBe persons.size

    blockUntilCount(3, "person_update_all_sql")
    "person_update_all_sql" should haveCount(3)

    val updated = pClient
      .updateByQuery(
        "person_update_all_sql",
        """UPDATE person_update_all_sql SET birthDate = '1972-12-26'"""
      )
      .get

    updated shouldBe 3L

    pClient.refresh("person_update_all_sql").get shouldBe true

    val updatedPersons = pClient.searchDocuments("SELECT * FROM person_update_all_sql")
    updatedPersons.size shouldBe 3
    updatedPersons.forall(person => person.birthDate == "1972-12-26") shouldBe true
  }

  "Update by query with JSON query and user pipeline" should "apply the pipeline to all documents" in {
    val mapping =
      """{
        |  "properties": {
        |    "uuid": { "type": "keyword" },
        |    "name": { "type": "keyword" },
        |    "birthDate": { "type": "date" },
        |    "childrenCount": { "type": "integer" }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    pClient
      .createIndex("person_update_json", mappings = Some(mapping))
      .get shouldBe true

    // User Pipeline
    val userPipelineJson =
      """{
        |  "processors": [
        |    { "set": { "field": "birthDate", "value": "1972-12-26" } }
        |  ]
        |}
    """.stripMargin

    pClient.createPipeline("set-birthdate-1972-12-26", userPipelineJson).get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_update_json")
    pClient
      .bulk[String](persons, identity, idKey = Some(Set("uuid")))
      .get

    blockUntilCount(3, "person_update_json")
    "person_update_json" should haveCount(3)

    val updated = pClient
      .updateByQuery(
        "person_update_json",
        """{"query": {"match_all": {}}}""",
        pipelineId = Some("set-birthdate-1972-12-26")
      )
      .get

    updated shouldBe 3L

    pClient.refresh("person_update_json").get shouldBe true

    val updatedPersons = pClient.searchDocuments("SELECT * FROM person_update_json")
    updatedPersons.size shouldBe 3
    updatedPersons.forall(person => person.birthDate == "1972-12-26") shouldBe true
  }

  "Update by query with SQL UPDATE and user pipeline" should "merge user and SQL pipelines" in {
    val mapping =
      """{
        |  "properties": {
        |    "uuid": { "type": "keyword" },
        |    "name": { "type": "keyword" },
        |    "birthDate": { "type": "date" },
        |    "childrenCount": { "type": "integer" }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    pClient
      .createIndex("person_update_merge_pipeline", mappings = Some(mapping))
      .get shouldBe true

    val userPipelineJson =
      """{
        |  "processors": [
        |    { "set": { "field": "name", "value": "UPDATED_NAME" } }
        |  ]
        |}
    """.stripMargin

    pClient.createPipeline("user-update-name", userPipelineJson).get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_update_merge_pipeline")
    pClient
      .bulk[String](persons, identity, idKey = Some(Set("uuid")))
      .get

    blockUntilCount(3, "person_update_merge_pipeline")
    "person_update_merge_pipeline" should haveCount(3)

    val updated = pClient
      .updateByQuery(
        "person_update_merge_pipeline",
        """UPDATE person_update_merge_pipeline SET birthDate = '1972-12-26' WHERE uuid = 'A16'""",
        pipelineId = Some("user-update-name")
      )
      .get

    updated shouldBe 1L

    pClient.refresh("person_update_merge_pipeline").get shouldBe true

    pClient.getAs[Person]("A16", Some("person_update_merge_pipeline")).get match {
      case Some(doc) =>
        doc.name shouldBe "UPDATED_NAME"
        doc.birthDate shouldBe "1972-12-26"
      case None => fail("Document A16 not found")
    }
  }

  "Update by query on a closed index" should "open, update, and restore closed state" in {
    val mapping =
      """{
        |  "properties": {
        |    "uuid": { "type": "keyword" },
        |    "name": { "type": "keyword" },
        |    "birthDate": { "type": "date" },
        |    "childrenCount": { "type": "integer" }
        |  }
        |}
    """.stripMargin.replaceAll("\n", "").replaceAll("\\s+", "")

    pClient
      .createIndex("person_update_closed_index", mappings = Some(mapping))
      .get shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person_update_closed_index")
    pClient
      .bulk[String](persons, identity, idKey = Some(Set("uuid")))
      .get

    blockUntilCount(3, "person_update_closed_index")
    "person_update_closed_index" should haveCount(3)

    pClient.closeIndex("person_update_closed_index").get shouldBe true
    pClient.isIndexClosed("person_update_closed_index").get shouldBe true

    val updated = pClient
      .updateByQuery(
        "person_update_closed_index",
        """UPDATE person_update_closed_index SET birthDate = '1972-12-26' WHERE uuid = 'A16'"""
      )
      .get

    updated shouldBe 1L

    pClient.isIndexClosed("person_update_closed_index").get shouldBe true
  }
}
