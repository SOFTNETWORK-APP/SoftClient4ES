package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.sql.SQLQuery
import com.fasterxml.jackson.core.JsonParseException
import com.sksamuel.elastic4s.requests.searches.queries.matches.MatchAllQuery
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import app.softnetwork.persistence._
import app.softnetwork.serialization._
import app.softnetwork.elastic.model._
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.person.model.Person
import com.google.gson.JsonParser
import com.typesafe.scalalogging.StrictLogging
import org.json4s.Formats
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.io.ByteArrayInputStream
import _root_.java.nio.file.{Files, Paths}
import _root_.java.time.format.DateTimeFormatter
import _root_.java.util.concurrent.TimeUnit
import _root_.java.util.UUID
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers
    with StrictLogging {

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override implicit def ec: ExecutionContext = executionContext

  implicit val formats: Formats = commonFormats

  def pClient: ElasticProvider[Person] with ElasticClientApi
  def sClient: ElasticProvider[Sample] with ElasticClientApi
  def bClient: ElasticProvider[Binary] with ElasticClientApi
  def parentClient: ElasticProvider[Parent] with ElasticClientApi

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

  val persons: List[String] = List(
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
    "create_delete" should beCreated()

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

  private def settings: Map[String, String] = {
    elasticClient
      .execute {
        getSettings("person")
      }
      .complete() match {
      case Success(s) => s.result.settingsForIndex("person")
      case Failure(f) => throw f
    }
  }

  "Toggle refresh" should "work" in {
    pClient.toggleRefresh("person", enable = false)
    new JsonParser()
      .parse(pClient.loadSettings("person"))
      .getAsJsonObject
      .get("refresh_interval")
      .getAsString shouldBe "-1"
    // settings.getOrElse("index.refresh_interval", "") shouldBe "-1"

    pClient.toggleRefresh("person", enable = true)
    //    settings.getOrElse("index.refresh_interval", "") shouldBe "1s"
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
    settings.getOrElse("index.number_of_replicas", "") shouldBe "3"

    pClient.setReplicas("person", 0)
    settings.getOrElse("index.number_of_replicas", "") shouldBe "0"
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
    logger.info(s"properties: $properties")
    MappingComparator.isMappingDifferent(
      properties,
      mapping
    ) shouldBe false

    implicit val bulkOptions: BulkOptions = BulkOptions("person_mapping", "person", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    refresh(indices)
    pClient.flush("person_mapping")

    indices should contain only "person_mapping"

    blockUntilCount(3, "person_mapping")

    "person_mapping" should haveCount(3)

    pClient.search[Person]("select * from person_mapping") match {
      case r if r.size == 3 =>
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case other => fail(other.toString)
    }

    pClient.search[Person]("select * from person_mapping where uuid = 'A16'") match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient.search[Person]("select * from person_mapping where match(name, 'gum')") match {
      case r if r.size == 1 =>
        r.map(_.uuid) should contain only "A16"
      case other => fail(other.toString)
    }

    pClient.search[Person](
      "select * from person_mapping where uuid <> 'A16' and match(name, 'gum')"
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

    implicit val bulkOptions: BulkOptions = BulkOptions("person_migration", "person", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    refresh(indices)
    pClient.flush("person_migration")

    indices should contain only "person_migration"

    blockUntilCount(3, "person_migration")

    "person_migration" should haveCount(3)

    pClient.search[Person]("select * from person_migration where name like '%gum%'") match {
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

    pClient.search[Person]("select * from person_migration where name like '%gum%'") match {
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

    val response = elasticClient
      .execute {
        search("person1").query(MatchAllQuery())
      }
      .complete()

    response.result.hits.hits.foreach { h =>
      h.id should not be h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
  }

  "Bulk index valid json with an id key but no suffix key" should "work" in {
    elasticClient
      .execute(
        createIndex("person2").mapping(
          properties(
            objectField("child").copy(properties = Seq(textField("name").copy(index = Some(false))))
          )
        )
      )
      .complete() match {
      case Success(_) => ()
      case Failure(f) => throw f
    }

    implicit val bulkOptions: BulkOptions = BulkOptions("person2", "person", 1000)
    val indices = pClient.bulk[String](persons.iterator, identity, Some("uuid"), None, None)
    refresh(indices)
    pClient.flush("person2")

    indices should contain only "person2"

    blockUntilCount(3, "person2")

    "person2" should haveCount(3)

    val response = elasticClient
      .execute {
        search("person2").query(MatchAllQuery())
      }
      .complete()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")

  }

  "Bulk index valid json with an id key and a suffix key" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person", "person", 1000)
    val indices =
      pClient.bulk[String](persons.iterator, identity, Some("uuid"), Some("birthDate"), None, None)
    refresh(indices)

    indices should contain allOf ("person-1967-11-21", "person-1969-05-09")

    blockUntilCount(2, "person-1967-11-21")
    blockUntilCount(1, "person-1969-05-09")

    "person-1967-11-21" should haveCount(2)
    "person-1969-05-09" should haveCount(1)

    val response = elasticClient
      .execute {
        search("person-1967-11-21", "person-1969-05-09").query(MatchAllQuery())
      }
      .complete()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceField("uuid")
    }

    response.result.hits.hits
      .map(
        _.sourceField("name")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble")
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
    refresh(indices)

    indices should contain only "person4"

    blockUntilCount(3, "person4")

    "person4" should haveCount(3)

    val response = elasticClient
      .execute {
        search("person4").query(MatchAllQuery())
      }
      .complete()

    logger.info(s"response: ${response.result.hits.hits.mkString("\n")}")

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceAsMap.getOrElse("uuid", "")
    }

    response.result.hits.hits
      .map(
        _.sourceAsMap.getOrElse("name", "")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
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
    refresh(indices)

    indices should contain allOf ("person5-1967-11-21", "person5-1969-05-09")

    blockUntilCount(2, "person5-1967-11-21")
    blockUntilCount(1, "person5-1969-05-09")

    "person5-1967-11-21" should haveCount(2)
    "person5-1969-05-09" should haveCount(1)

    val response = elasticClient
      .execute {
        search("person5-1967-11-21", "person5-1969-05-09").query(MatchAllQuery())
      }
      .complete()

    response.result.hits.hits.foreach { h =>
      h.id shouldBe h.sourceAsMap.getOrElse("uuid", "")
    }

    response.result.hits.hits
      .map(
        _.sourceAsMap.getOrElse("name", "")
      ) should contain allOf ("Homer Simpson", "Moe Szyslak", "Barney Gumble2")
  }

  "Count" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person6", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person6"

    blockUntilCount(3, "person6")

    "person6" should haveCount(3)

    import scala.collection.immutable.Seq

    pClient
      .count(JSONQuery("{}", Seq[String]("person6"), Seq[String]()))
      .getOrElse(0d)
      .toInt should ===(3)

    pClient.countAsync(JSONQuery("{}", Seq[String]("person6"), Seq[String]())).complete() match {
      case Success(s) => s.getOrElse(0d).toInt should ===(3)
      case Failure(f) => fail(f.getMessage)
    }
  }

  "Search" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person7", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

    indices should contain only "person7"

    blockUntilCount(3, "person7")

    "person7" should haveCount(3)

    val r1 = pClient.search[Person]("select * from person7")
    r1.size should ===(3)
    r1.map(_.uuid) should contain allOf ("A12", "A14", "A16")

    pClient.searchAsync[Person]("select * from person7") onComplete {
      case Success(r) =>
        r.size should ===(3)
        r.map(_.uuid) should contain allOf ("A12", "A14", "A16")
      case Failure(f) => fail(f.getMessage)
    }

    val r2 = pClient.search[Person]("select * from person7 where _id=\"A16\"")
    r2.size should ===(1)
    r2.map(_.uuid) should contain("A16")

    pClient.searchAsync[Person]("select * from person7 where _id=\"A16\"") onComplete {
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
    refresh(indices)

    indices should contain only "person8"

    blockUntilCount(3, "person8")

    "person8" should haveCount(3)

    val response = pClient.search[Person]("select * from person8")

    response.size should ===(3)

  }

  "Get" should "work" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person9", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)

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
    val sample = Sample(uuid)
    val result = sClient.index(sample)
    result shouldBe true

    val result2 = sClient.delete(sample.uuid, Some("sample"))
    result2 shouldBe true

    sClient.deleteAsync(sample.uuid, Some("sample")).complete() match {
      case Success(r) => r shouldBe true
      case Failure(f) => fail(f.getMessage)
    }

    val result3 = sClient.get[Sample](uuid)
    result3.isEmpty shouldBe true
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
    logger.info(s"mapping: $mapping")
    bClient.setMapping("binaries", mapping) shouldBe true
    val mappings = bClient.getMapping("binaries")
    logger.info(s"mappings: $mappings")
    assert("{\"mappings\":" + mapping + "}" == mappings)
    for (uuid <- Seq("png", "jpg", "pdf")) {
      val path =
        Paths.get(Thread.currentThread().getContextClassLoader.getResource(s"avatar.$uuid").getPath)
      import app.softnetwork.utils.ImageTools._
      import app.softnetwork.utils.HashTools._
      import app.softnetwork.utils.Base64Tools._
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
    logger.info(s"mapping: $mapping")
    pClient.setMapping("person10", mapping) shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("person10", "person", 1000)
    val indices =
      pClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)
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
      case Success(s) => s.headOption.flatMap(_.asDoubleOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test count aggregation
    pClient.aggregate("select count(p.uuid) as c from person10 p").complete() match {
      case Success(s) => s.headOption.flatMap(_.asDoubleOption).getOrElse(0d) should ===(3d)
      case Failure(f) => fail(f.getMessage)
    }

    // test max aggregation on date field
    pClient.aggregate("select max(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.headOption.flatMap(_.asStringOption).getOrElse("") should ===("1969-05-09T00:00:00.000Z")
      case Failure(f) => fail(f.getMessage)
    }

    // test min aggregation on date field
    pClient.aggregate("select min(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.headOption.flatMap(_.asStringOption).getOrElse("") should ===("1967-11-21T00:00:00.000Z")
      case Failure(f) => fail(f.getMessage)
    }

    // test avg aggregation on date field
    pClient.aggregate("select avg(p.birthDate) as c from person10 p").complete() match {
      case Success(s) =>
        s.headOption.flatMap(_.asStringOption).getOrElse("") should ===("1968-05-17T08:00:00.000Z")
      case Failure(f) => fail(f.getMessage)
    }

    // test sum aggregation on integer field
    pClient
      .aggregate(
        "select sum(p.childrenCount) as c from person10 p"
      )
      .complete() match {
      case Success(s) =>
        s.headOption.flatMap(_.asDoubleOption).getOrElse(0d) should ===(2d)
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
    logger.info(s"mapping: $mapping")
    parentClient.setMapping("parent", mapping) shouldBe true

    implicit val bulkOptions: BulkOptions = BulkOptions("parent", "parent", 1000)
    val indices =
      parentClient
        .bulk[String](personsWithUpsert.iterator, identity, Some("uuid"), None, None, Some(true))
    refresh(indices)
    parentClient.flush("parent")
    parentClient.refresh("parent")

    indices should contain only "parent"

    blockUntilCount(3, "parent")

    "parent" should haveCount(3)

    val parents = parentClient.search[Parent]("select * from parent")
    assert(parents.size == 3)

    val results = parentClient.searchWithInnerHits[Parent, Child](
      """SELECT
        | p.uuid,
        | p.name,
        | p.birthDate,
        | p.children,
        | inner_children.name,
        | inner_children.birthDate
        |FROM
        | parent as p,
        | UNNEST(p.children) as inner_children
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
  }
}
