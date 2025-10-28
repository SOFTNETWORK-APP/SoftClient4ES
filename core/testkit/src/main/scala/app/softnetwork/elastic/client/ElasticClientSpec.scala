package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.model.{Binary, Child, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SQLQuery
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
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
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

  implicit def toSQLQuery(sqlQuery: String): SQLQuery = SQLQuery(sqlQuery)

  override def beforeAll(): Unit = {
    super.beforeAll()
    pClient.createIndex("person")
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
    pClient.createIndex("create_delete")
    blockUntilIndexExists("create_delete")
    "create_delete" should beCreated

    pClient.deleteIndex("create_delete")
    blockUntilIndexNotExists("create_delete")
    "create_delete" should not(beCreated())
  }

  "Adding an alias and then removing it" should "work" in {
    pClient.addAlias("person", "person_alias")

    doesAliasExists("person_alias") shouldBe true

    pClient.removeAlias("person", "person_alias")

    doesAliasExists("person_alias") shouldBe false
  }

  "Toggle refresh" should "work" in {
    pClient.toggleRefresh("person", enable = false)
    new JsonParser()
      .parse(pClient.loadSettings("person"))
      .getAsJsonObject
      .get("refresh_interval")
      .getAsString shouldBe "-1"

    pClient.toggleRefresh("person", enable = true)
    new JsonParser()
      .parse(pClient.loadSettings("person"))
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

    new JsonParser()
      .parse(pClient.loadSettings("person"))
      .getAsJsonObject
      .get("number_of_replicas")
      .getAsString shouldBe "3"

    pClient.setReplicas("person", 0)
    new JsonParser()
      .parse(pClient.loadSettings("person"))
      .getAsJsonObject
      .get("number_of_replicas")
      .getAsString shouldBe "0"
  }

  "Setting a mapping" should "work" in {
    pClient.createIndex("person_mapping")
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
    pClient.setMapping("person_mapping", mapping) shouldBe true

    val properties = pClient.getMappingProperties("person_mapping")
    log.info(s"properties: $properties")
    MappingComparator.isMappingDifferent(
      properties,
      mapping
    ) shouldBe false

    implicit val bulkOptions: BulkOptions = BulkOptions("person_mapping", "_doc", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person_mapping")

    indices should contain only "person_mapping"

    blockUntilCount(3, "person_mapping")

    "person_mapping" should haveCount(3)

    pClient.searchAs[Person]("select * from person_mapping") match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case other => fail(other.toString)
    }

    pClient.searchAs[Person]("select * from person_mapping where uuid = 'A16'") match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient.searchAs[Person](
      "select * from person_mapping where match (name) against ('gum')"
    ) match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient.searchAs[Person](
      "select * from person_mapping where uuid <> 'A16' and match (name) against ('gum')"
    ) match {
      case r if r.isEmpty =>
      case other          => fail(other.toString)
    }
  }

  "Updating a mapping" should "work" in {
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
    pClient.updateMapping("person_migration", mapping) shouldBe true
    blockUntilIndexExists("person_migration")
    "person_migration" should beCreated()

    implicit val bulkOptions: BulkOptions = BulkOptions("person_migration", "_doc", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person_migration")

    indices should contain only "person_migration"

    blockUntilCount(3, "person_migration")

    "person_migration" should haveCount(3)

    pClient.searchAs[Person](
      "select * from person_migration where match (name) against ('gum')"
    ) match {
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
    pClient.shouldUpdateMapping("person_migration", newMapping) shouldBe true
    pClient.updateMapping("person_migration", newMapping) shouldBe true

    pClient.searchAs[Person](
      "select * from person_migration where match (name) against ('gum')"
    ) match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

  }

  "Bulk index valid json without id key and suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person1", "person", 2)
    val indices = pClient.bulk[String](persons.iterator, identity, None, None, None)

    indices should contain only "person1"

    blockUntilCount(3, "person1")

    "person1" should haveCount(3)

    pClient.searchAs[Person]("select * from person1") match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
        r.map(_.name) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
      case other => fail(other.toString)
    }

  }

  "Bulk index valid json with an id key but no suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person2", "person", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person2")

    indices should contain only "person2"

    blockUntilCount(3, "person2")

    "person2" should haveCount(3)

    pClient.searchAs[Person]("select * from person2") match {
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
    implicit val bulkOptions: BulkOptions = BulkOptions("person", "person", 1000)
    val indices =
      pClient.bulk[String](persons.iterator, identity, Some("uuid"), Some("birthDate"), None, None)
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain allOf ("person-1967-11-21", "person-1969-05-09")

    blockUntilCount(2, "person-1967-11-21")
    blockUntilCount(1, "person-1969-05-09")

    "person-1967-11-21" should haveCount(2)
    "person-1969-05-09" should haveCount(1)

    pClient.searchAs[Person]("select * from person-1967-11-21, person-1969-05-09") match {
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
    implicit val bulkOptions: BulkOptions = BulkOptions("person_error", "person", 1000)
    intercept[JsonParseException] {
      val invalidJson = persons :+ "fail"
      pClient.bulk[String](invalidJson.iterator, identity, None, None, None)
    }
  }

  "Bulk upsert valid json with an id key but no suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person4", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person4"

    blockUntilCount(3, "person4")

    "person4" should haveCount(3)

    pClient.searchAs[Person]("select * from person4") match {
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
    implicit val bulkOptions: BulkOptions = BulkOptions("person5", "person", 1000)
    val indices = pClient.bulk[String](
      personsWithUpsert.iterator,
      identity,
      Some("uuid"),
      Some("birthDate"),
      None,
      Some(true)
    )
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain allOf ("person5-1967-11-21", "person5-1969-05-09")

    blockUntilCount(2, "person5-1967-11-21")
    blockUntilCount(1, "person5-1969-05-09")

    "person5-1967-11-21" should haveCount(2)
    "person5-1969-05-09" should haveCount(1)

    pClient.searchAs[Person]("select * from person5-1967-11-21, person5-1969-05-09") match {
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
    implicit val bulkOptions: BulkOptions = BulkOptions("person6", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person6"

    blockUntilCount(3, "person6")

    "person6" should haveCount(3)

    import scala.collection.immutable._

    pClient
      .count(ElasticQuery("{}", Seq[String]("person6"), Seq[String]()))
      .getOrElse(0d)
      .toInt should ===(3)

    pClient.countAsync(ElasticQuery("{}", Seq[String]("person6"), Seq[String]())).complete() match {
      case Success(s) => s.getOrElse(0d).toInt should ===(3)
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Search" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person7", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person7"

    blockUntilCount(3, "person7")

    "person7" should haveCount(3)

    val r1 = pClient.searchAs[Person]("select * from person7")
    r1.size should ===(3)
    r1.map(_.uuid) should contain allOf ("A12", "A14", "A16")

    pClient.searchAsyncAs[Person]("select * from person7") onComplete {
      case Success(r) =>
        r.size should ===(3)
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case Failure(f) => fail(f.getMessage)
    }

    val r2 = pClient.searchAs[Person]("select * from person7 where _id=\"A16\"")
    r2.size should ===(1)
    r2.map(_.uuid) should contain("A16")

    pClient.searchAsyncAs[Person]("select * from person7 where _id=\"A16\"") onComplete {
      case Success(r) =>
        r.size should ===(1)
        r.map(_.uuid) should contain("A16")
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Get all" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person8", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person8"

    blockUntilCount(3, "person8")

    "person8" should haveCount(3)

    val response = pClient.searchAs[Person]("select * from person8")

    response.size should ===(3)

  }

  "Get" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person9", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true

    indices should contain only "person9"

    blockUntilCount(3, "person9")

    "person9" should haveCount(3)

    val response = pClient.get[Person]("A16", Some("person9"))

    response.isDefined shouldBe true
    response.get.uuid shouldBe "A16"

    pClient.getAsync[Person]("A16", Some("person9")).complete() match {
      case Success(r) =>
        r.isDefined shouldBe true
        r.get.uuid shouldBe "A16"
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Index" should "work" in {
    val uuid = UUID.randomUUID().toString
    val sample = Sample(uuid)
    val result = sClient.index(sample)
    result shouldBe true

    sClient.indexAsync(sample).complete() match {
      case Success(r) => r shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    val result2 = sClient.get[Sample](uuid)
    result2 match {
      case Some(r) =>
        r.uuid shouldBe uuid
      case _ =>
        fail("Sample not found")
    }
  }

  "Update" should "work" in {
    val uuid = UUID.randomUUID().toString
    val sample = Sample(uuid)
    val result = sClient.update(sample)
    result shouldBe true

    sClient.updateAsync(sample).complete() match {
      case Success(r) => r shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    val result2 = sClient.get[Sample](uuid)
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
    sClient.createIndex(index) shouldBe true
    val sample = Sample(uuid)
    val result = sClient.index(sample, Some(index))
    result shouldBe true

    sClient.delete(sample.uuid, index) shouldBe true

    //blockUntilEmpty(index)

    sClient.get[Sample](uuid).isEmpty shouldBe true
  }

  "Delete asynchronously" should "work" in {
    val uuid = UUID.randomUUID().toString
    val index = s"sample-$uuid"
    sClient.createIndex(index) shouldBe true
    val sample = Sample(uuid)
    val result = sClient.index(sample, Some(index))
    result shouldBe true

    sClient.deleteAsync(sample.uuid, index).complete() match {
      case Success(r) => r shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    // blockUntilEmpty(index)

    sClient.get[Sample](uuid).isEmpty shouldBe true
  }

  "Index binary data" should "work" in {
    bClient.createIndex("binaries") shouldBe true
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
    bClient.setMapping("binaries", mapping) shouldBe true
    bClient.shouldUpdateMapping("binaries", mapping) shouldBe false
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
          bClient.index(binary) shouldBe true
          bClient.get[Binary](uuid) match {
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
    pClient.createIndex("person10") shouldBe true
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
    pClient.setMapping("person10", mapping) shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person10", "_doc", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    pClient.flush("person10")

    indices should contain only "person10"

    blockUntilCount(3, "person10")

    "person10" should haveCount(3)

    pClient.get[Person]("A16", Some("person10")) match {
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
      case Success(s) => s.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test count aggregation
    pClient.aggregate("select count(p.uuid) as c from person10 p").complete() match {
      case Success(s) => s.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test max aggregation on date field
    pClient.aggregate("select max(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        // The maximum date should be the latest birthDate in the dataset
        s.headOption match {
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
        s.headOption match {
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
        s.headOption match {
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
        s.headOption.flatMap(_.asDoubleSafe.toOption).getOrElse(0d) should ===(2d)
      case Failure(f) => fail(f.getMessage)
    }

    // test first aggregation on date field
    pClient.aggregate("select first(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.headOption match {
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
        s.headOption match {
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
        val names = s.headOption.flatMap(_.asSeqSafe.toOption).getOrElse(Seq.empty).map {
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
        val birthDates = s.headOption.flatMap(_.asSeqSafe.toOption).getOrElse(Seq.empty).map {
          case t: Temporal => t
          case other       => fail(s"Unexpected birthDate type: $other")
        }.map(_.toString)
        birthDates.nonEmpty shouldBe true
        birthDates should contain allOf ("1999-05-09", "2002-05-09") // LocalDate instances sorted ASC
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Nested queries" should "work" in {
    parentClient.createIndex("parent") shouldBe true
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
    parentClient.setMapping("parent", mapping) shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("parent", "_doc", 1000)
    val indices =
      parentClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    indices.forall(index => refresh(index).getStatusLine.getStatusCode < 400) shouldBe true
    parentClient.flush("parent")
    parentClient.refresh("parent")

    indices should contain only "parent"

    blockUntilCount(3, "parent")

    "parent" should haveCount(3)

    val parents = parentClient.searchAs[Parent]("select * from parent")
    assert(parents.size == 3)

    val results = parentClient.searchWithInnerHits[Parent, Child](
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

    val query =
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

    val searchResults = parentClient.searchAs[Parent](query)
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
      .scrollAs[Parent](query, config = ScrollConfig(logEvery = 1))
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
}
