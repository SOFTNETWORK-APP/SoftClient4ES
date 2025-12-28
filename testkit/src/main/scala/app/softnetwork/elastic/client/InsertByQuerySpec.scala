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
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.bulk.BulkOptions
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor
import scala.language.implicitConversions

trait InsertByQuerySpec
    extends AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  def client: ElasticClientApi

  // ---------------------------------------------------------------------------
  // FIXTURES : MAPPINGS
  // ---------------------------------------------------------------------------

  private val customersMapping: String =
    """{
      |"properties":{
      | "customer_id":{"type":"keyword"},
      | "name":{"type":"text"},
      | "email":{"type":"keyword"},
      | "country":{"type":"keyword"}
      |},
      |"_meta":{"primary_key":["customer_id"]}
      |}""".stripMargin.replaceAll("\\s+", "")

  private val productsMapping: String =
    """{
      |"properties":{
      | "sku":{"type":"keyword"},
      | "name":{"type":"text"},
      | "price":{"type":"double"},
      | "category":{"type":"keyword"}
      |},
      |"_meta":{"primary_key":["sku"]}
      |}""".stripMargin.replaceAll("\\s+", "")

  private val ordersMapping: String =
    """{
      |"properties":{
      | "order_id":{"type":"keyword"},
      | "customer_id":{"type":"keyword"},
      | "order_date":{"type":"date"},
      | "total":{"type":"double"},
      | "items":{
      |   "type":"nested",
      |   "properties":{
      |     "sku":{"type":"keyword"},
      |     "qty":{"type":"integer"}
      |   }
      | }
      |},
      |"_meta":{
      | "primary_key":["order_id","customer_id"],
      | "partition_by":{"column":"order_date","granularity":"d"}
      |}
      |}""".stripMargin.replaceAll("\\s+", "")

  private val stagingOrdersMapping: String =
    """{
      |"properties":{
      | "id":{"type":"keyword"},
      | "cust":{"type":"keyword"},
      | "date":{"type":"date"},
      | "amount":{"type":"double"}
      |}
      |}""".stripMargin.replaceAll("\\s+", "")

  private val stagingCustomersMapping: String =
    """{
      |"properties":{
      | "id":{"type":"keyword"},
      | "fullname":{"type":"text"},
      | "email":{"type":"keyword"}
      |}
      |}""".stripMargin.replaceAll("\\s+", "")

  // ---------------------------------------------------------------------------
  // FIXTURES : DATASETS JSON PURS
  // ---------------------------------------------------------------------------

  private val customers: List[String] = List(
    """{"customer_id":"C001","name":"Alice","email":"alice@example.com","country":"FR"}""",
    """{"customer_id":"C002","name":"Bob","email":"bob@example.com","country":"US"}""",
    """{"customer_id":"C003","name":"Charlie","email":"charlie@example.com","country":"DE"}"""
  )

  private val products: List[String] = List(
    """{"sku":"SKU-001","name":"Laptop","price":1299.99,"category":"electronics"}""",
    """{"sku":"SKU-002","name":"Mouse","price":29.99,"category":"electronics"}""",
    """{"sku":"SKU-003","name":"Desk Chair","price":199.99,"category":"furniture"}"""
  )

  private val orders: List[String] = List(
    """{"order_id":"O1001","customer_id":"C001","order_date":"2024-01-10","total":1299.99,"items":[{"sku":"SKU-001","qty":1}]}""",
    """{"order_id":"O1002","customer_id":"C002","order_date":"2024-01-11","total":29.99,"items":[{"sku":"SKU-002","qty":1}]}"""
  )

  private val stagingOrders: List[String] = List(
    """{"id":"O2001","cust":"C001","date":"2024-02-01","amount":1299.99}""",
    """{"id":"O2002","cust":"C003","date":"2024-02-02","amount":199.99}""",
    """{"id":"O2003","cust":"C002","date":"2024-02-03","amount":29.99}"""
  )

  private val stagingOrdersUpdates: List[String] = List(
    """{"id":"O1001","cust":"C001","date":"2024-01-10","amount":1499.99}""",
    """{"id":"O1002","cust":"C002","date":"2024-01-11","amount":39.99}"""
  )

  private val stagingCustomers: List[String] = List(
    """{"id":"C010","fullname":"Bob Martin","email":"bob.martin@example.com"}""",
    """{"id":"C011","fullname":"Jane Doe","email":"jane.doe@example.com"}"""
  )

  // ---------------------------------------------------------------------------
  // FIXTURE : INITIALISATION DES INDEXES
  // ---------------------------------------------------------------------------

  implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
    Source.fromIterator(() => list.iterator)

  private def initIndex(name: String, mapping: String, docs: List[String]): Unit = {
    client.createIndex(name, mappings = Some(mapping))
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = name)
    client.bulk[String](docs, identity, Some(name))
    client.refresh(name)
  }

  // ---------------------------------------------------------------------------
  // TESTS D’INTÉGRATION COMPLETS
  // ---------------------------------------------------------------------------

  behavior of "insertByQuery"

  it should "init all indices" in {
    initIndex("customers", customersMapping, customers)
    initIndex("products", productsMapping, products)
    initIndex("orders", ordersMapping, orders)
    initIndex("staging_orders", stagingOrdersMapping, stagingOrders)
    initIndex("staging_orders_updates", stagingOrdersMapping, stagingOrdersUpdates)
    initIndex("staging_customers", stagingCustomersMapping, stagingCustomers)
  }

  it should "insert a customer with INSERT ... VALUES" in {
    val sql =
      """INSERT INTO customers (customer_id, name, email, country)
        |VALUES ('C010', 'Bob', 'bob@example.com', 'FR')""".stripMargin

    val result = client.insertByQuery("customers", sql).futureValue
    result shouldBe ElasticSuccess(1L)
  }

  it should "upsert a product with ON CONFLICT DO UPDATE" in {
    val sql =
      """INSERT INTO products (sku, name, price, category)
        |VALUES ('SKU-001', 'Laptop Pro', 1499.99, 'electronics')
        |ON CONFLICT DO UPDATE""".stripMargin.replaceAll("\\s+", " ")

    val result = client.insertByQuery("products", sql).futureValue
    result shouldBe ElasticSuccess(1L)
  }

  it should "insert orders from a SELECT with alias mapping" in {
    val sql =
      """INSERT INTO orders (order_id, customer_id, order_date, total)
        |AS SELECT
        |  id AS order_id,
        |  cust AS customer_id,
        |  date AS order_date,
        |  amount AS total
        |FROM staging_orders""".stripMargin.replaceAll("\\s+", " ")

    val result = client.insertByQuery("orders", sql).futureValue
    result shouldBe ElasticSuccess(3L)
  }

  it should "upsert orders with composite PK using ON CONFLICT DO UPDATE" in {
    val sql =
      """INSERT INTO orders (order_id, customer_id, order_date, total)
        |AS SELECT
        |  id AS order_id,
        |  cust AS customer_id,
        |  date AS order_date,
        |  amount AS total
        |FROM staging_orders_updates
        |ON CONFLICT (order_id, customer_id) DO UPDATE""".stripMargin

    val result = client.insertByQuery("orders", sql).futureValue
    result shouldBe ElasticSuccess(2L)
  }

  it should "fail when conflictTarget does not match PK" in {
    val sql =
      """INSERT INTO orders (order_id, customer_id, order_date, total)
        |VALUES ('O1001', 'C001', '2024-01-10', 1299.99)
        |ON CONFLICT (order_id) DO UPDATE""".stripMargin

    val result = client.insertByQuery("orders", sql).futureValue
    result shouldBe a[ElasticFailure]
  }

  it should "fail when SELECT does not provide all INSERT columns" in {
    val sql =
      """INSERT INTO customers (customer_id, name, email)
        |AS SELECT id AS customer_id, fullname AS name
        |FROM staging_customers""".stripMargin

    val result = client.insertByQuery("customers", sql).futureValue
    result shouldBe a[ElasticFailure]
  }

  it should "fail when ON CONFLICT DO UPDATE is used without PK or conflictTarget" in {
    val sql =
      """INSERT INTO staging_orders (id, cust, date, amount)
        |VALUES ('X', 'Y', '2024-01-01', 10.0)
        |ON CONFLICT DO UPDATE""".stripMargin

    val result = client.insertByQuery("staging_orders", sql).futureValue
    result shouldBe a[ElasticFailure]
  }
}
