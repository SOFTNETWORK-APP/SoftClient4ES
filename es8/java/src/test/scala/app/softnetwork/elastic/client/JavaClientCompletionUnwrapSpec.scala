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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.java.JavaClientCompanion
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.io.IOException
import _root_.java.net.SocketTimeoutException
import _root_.java.util.concurrent.{CompletableFuture, CompletionException}
import _root_.java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

/** #238 — the asynchronous PIT paging path rides `fromCompletableFuture`. A failed
  * `CompletableFuture` hands its cause over wrapped in a `CompletionException`, which
  * `isRetriableError` does not match: without the unwrap, `retryWithBackoff` would silently stop
  * retrying transient `IOException`s on every es8/es9 extraction. No Docker.
  */
class JavaClientCompletionUnwrapSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("completion-unwrap-spec")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  private val companion: JavaClientCompanion = new JavaClientCompanion {
    override def elasticConfig: ElasticConfig = ElasticConfig(ConfigFactory.load())
  }

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def wrappedTimeout(): CompletableFuture[String] = {
    val cf = new CompletableFuture[String]()
    cf.completeExceptionally(
      new CompletionException(new SocketTimeoutException("read timed out"))
    )
    cf
  }

  "fromCompletableFuture" should "unwrap a CompletionException to its cause" in {
    val failure = Await.result(companion.fromCompletableFuture(wrappedTimeout()).failed, 5.seconds)
    failure shouldBe a[SocketTimeoutException]
    failure.getMessage shouldBe "read timed out"
  }

  it should "leave a raw failure untouched" in {
    val cf = new CompletableFuture[String]()
    cf.completeExceptionally(new IOException("raw"))
    val failure = Await.result(companion.fromCompletableFuture(cf).failed, 5.seconds)
    failure shouldBe an[IOException]
    failure.getMessage shouldBe "raw"
  }

  it should "keep a CompletionException without a cause" in {
    val cf = new CompletableFuture[String]()
    cf.completeExceptionally(new CompletionException("no cause", null))
    Await.result(companion.fromCompletableFuture(cf).failed, 5.seconds) shouldBe a[
      CompletionException
    ]
  }

  it should "complete normally" in {
    val cf = new CompletableFuture[String]()
    cf.complete("ok")
    Await.result(companion.fromCompletableFuture(cf), 5.seconds) shouldBe "ok"
  }

  "retryWithBackoff over fromCompletableFuture" should "still retry a wrapped SocketTimeoutException" in {
    val attempts = new AtomicInteger(0)
    val result =
      retryWithBackoff(
        RetryConfig(maxRetries = 2, initialDelay = 10.millis, maxDelay = 20.millis)
      ) {
        attempts.incrementAndGet()
        companion.fromCompletableFuture(wrappedTimeout())
      }
    val failure = Await.result(result.failed, 10.seconds)
    failure shouldBe a[SocketTimeoutException]
    attempts.get() shouldBe 3 // 1 + 2 retries
  }

  it should "not retry a wrapped non-IO failure" in {
    val attempts = new AtomicInteger(0)
    val result =
      retryWithBackoff(
        RetryConfig(maxRetries = 2, initialDelay = 10.millis, maxDelay = 20.millis)
      ) {
        attempts.incrementAndGet()
        val cf = new CompletableFuture[String]()
        cf.completeExceptionally(new CompletionException(new IllegalStateException("permanent")))
        companion.fromCompletableFuture(cf)
      }
    val failure = Await.result(result.failed, 10.seconds)
    failure shouldBe an[IllegalStateException]
    attempts.get() shouldBe 1
  }
}
