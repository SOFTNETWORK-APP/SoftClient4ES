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

package app.softnetwork.elastic.client.scroll

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** #238 — the merge helper behind sliced PIT paging: every page of every slice reaches the
  * consumer, a failing slice fails the merged stream, and `onTerminate` (the single PIT close)
  * fires exactly once on completion, failure and downstream cancellation.
  */
class SliceMergeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with Eventually {

  implicit val system: ActorSystem = ActorSystem("slice-merge-spec")
  implicit val ec: ExecutionContext = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def pages(slice: Int, n: Int): Source[Seq[Int], NotUsed] =
    Source(List.tabulate(n)(p => Seq(slice * 100 + p * 10, slice * 100 + p * 10 + 1)))

  private class Probe {
    val calls = new AtomicInteger(0)
    val last = new AtomicReference[Try[Done]]()
    val onTerminate: Try[Done] => Unit = { t =>
      last.set(t) // before the counter the tests wait on, so `last` is never read as null
      calls.incrementAndGet()
    }
  }

  "SliceMerge" should "deliver every page of every slice and terminate once with Success" in {
    val probe = new Probe
    val merged = SliceMerge(Seq(pages(1, 3), pages(2, 3), pages(3, 3)))(probe.onTerminate)
    val rows = Await.result(merged.mapConcat(identity).runWith(Sink.seq), 10.seconds)
    rows should have size 18
    rows.toSet should have size 18
    eventually(timeout(5.seconds)) {
      probe.calls.get() shouldBe 1
    }
    probe.last.get() shouldBe Success(Done)
  }

  it should "fail the merged stream when one slice fails, and terminate once with Failure" in {
    val probe = new Probe
    val boom = new IllegalStateException("slice 2 broke")
    val merged = SliceMerge(
      Seq(pages(1, 3), Source.failed[Seq[Int]](boom), pages(3, 3))
    )(probe.onTerminate)
    val result = Try(Await.result(merged.mapConcat(identity).runWith(Sink.seq), 10.seconds))
    result.isFailure shouldBe true
    result.failed.get.getMessage shouldBe "slice 2 broke"
    eventually(timeout(5.seconds)) {
      probe.calls.get() shouldBe 1
    }
    probe.last.get() shouldBe a[Failure[_]]
  }

  it should "terminate once when the consumer cancels early (.take)" in {
    val probe = new Probe
    val merged = SliceMerge(Seq(pages(1, 50), pages(2, 50), pages(3, 50)))(probe.onTerminate)
    val rows = Await.result(merged.mapConcat(identity).take(5).runWith(Sink.seq), 10.seconds)
    rows should have size 5
    eventually(timeout(5.seconds)) {
      probe.calls.get() shouldBe 1
    }
    probe.last.get() shouldBe Success(Done)
  }

  it should "pass a single source through and terminate once" in {
    val probe = new Probe
    val merged = SliceMerge(Seq(pages(1, 4)))(probe.onTerminate)
    val rows = Await.result(merged.mapConcat(identity).runWith(Sink.seq), 10.seconds)
    rows shouldBe List(100, 101, 110, 111, 120, 121, 130, 131)
    eventually(timeout(5.seconds)) {
      probe.calls.get() shouldBe 1
    }
  }

  it should "produce an empty stream for no slices and still terminate once" in {
    val probe = new Probe
    val merged = SliceMerge(Seq.empty[Source[Seq[Int], NotUsed]])(probe.onTerminate)
    val rows = Await.result(merged.runWith(Sink.seq), 10.seconds)
    rows shouldBe empty
    eventually(timeout(5.seconds)) {
      probe.calls.get() shouldBe 1
    }
    probe.last.get() shouldBe Success(Done)
  }
}
