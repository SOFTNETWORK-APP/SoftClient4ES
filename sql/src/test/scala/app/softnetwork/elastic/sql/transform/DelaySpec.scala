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

package app.softnetwork.elastic.sql.transform

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DelaySpec extends AnyFlatSpec with Matchers {

  behavior of "Delay.calculateOptimal"

  // A single-table materialized view is a one-stage chain: source -> view (SoftClient4ES#185).
  it should "derive a usable delay for a single-stage chain" in {
    val frequency = Frequency(TransformTimeUnit.Seconds, 30)
    Delay.calculateOptimal(frequency, nbStages = 1) match {
      case Right(delay) =>
        delay.toSeconds shouldBe 15L
        // Whatever it returns MUST satisfy the invariant its sibling enforces.
        Delay.validate(delay, frequency, 1) shouldBe Right(())
      case Left(error) => fail(s"Expected a delay, got: $error")
    }
  }

  it should "clamp a non-positive stage count instead of surfacing an internal invariant" in {
    // 60s / (2 * 1) = 30s ; the old code returned Left("Number of stages must be positive").
    Delay.calculateOptimal(Frequency(TransformTimeUnit.Minutes, 1), nbStages = 0) shouldBe
    Right(Delay(TransformTimeUnit.Seconds, 30))
    // The scaladoc clamps "values below 1", not just 0.
    Delay.calculateOptimal(Frequency(TransformTimeUnit.Minutes, 1), nbStages = -5) shouldBe
    Right(Delay(TransformTimeUnit.Seconds, 30))
  }

  // Pins the published minimum-refresh-interval table in documentation/sql/materialized_views.md:
  // 1 transform -> 20s, 3 -> 60s, 4 -> 80s. A doc number with no test on either side of it drifts.
  it should "accept exactly the documented minimum frequency and reject one second below it" in {
    Seq(1 -> 20L, 3 -> 60L, 4 -> 80L).foreach { case (stages, minFrequency) =>
      withClue(s"stages=$stages at the documented floor ${minFrequency}s: ") {
        val accepted =
          Delay.calculateOptimal(Frequency(TransformTimeUnit.Seconds, minFrequency), stages)
        accepted.map(_.toSeconds) shouldBe Right(Delay.MinDelaySeconds)
        accepted.map(d =>
          Delay.validate(d, Frequency(TransformTimeUnit.Seconds, minFrequency), stages)
        ) shouldBe Right(Right(()))
      }
      withClue(s"stages=$stages one second below the documented floor: ") {
        Delay.calculateOptimal(
          Frequency(TransformTimeUnit.Seconds, minFrequency - 1),
          stages
        ) match {
          case Left(error)  => error should include(s"Minimum required frequency: $minFrequency")
          case Right(delay) => fail(s"Expected a rejection below the floor, got: $delay")
        }
      }
    }
  }

  // The figure quoted by the rejection must be reachable for a non-default buffer factor too —
  // quoting only the `2 × stages × 10` ceiling would name a frequency that still fails.
  it should "name a minimum frequency that actually succeeds for a non-default buffer factor" in {
    val bufferFactor = 3.0
    Delay.calculateOptimal(
      Frequency(TransformTimeUnit.Seconds, 20),
      nbStages = 1,
      bufferFactor = bufferFactor
    ) match {
      case Left(error) =>
        error should include("Minimum required frequency: 30 seconds")
        // Taking the advice must work.
        Delay
          .calculateOptimal(
            Frequency(TransformTimeUnit.Seconds, 30),
            nbStages = 1,
            bufferFactor = bufferFactor
          )
          .map(_.toSeconds) shouldBe Right(10L)
      case Right(delay) => fail(s"Expected a rejection, got: $delay")
    }
  }

  it should "reject a frequency that is genuinely too low with an actionable message" in {
    Delay.calculateOptimal(Frequency(TransformTimeUnit.Seconds, 8), nbStages = 1) match {
      case Left(error) =>
        error should include("too small")
        error should include("Minimum required frequency: 20 seconds")
        error should not include "Number of stages must be positive"
      case Right(delay) => fail(s"Expected a rejection, got: $delay")
    }
  }

  it should "keep multi-stage chains inside the latency invariant" in {
    val frequency = Frequency(TransformTimeUnit.Minutes, 2) // 120s
    Delay.calculateOptimal(frequency, nbStages = 3) match {
      case Right(delay) =>
        delay.toSeconds shouldBe 20L
        Delay.validate(delay, frequency, 3) shouldBe Right(())
      case Left(error) => fail(s"Expected a delay, got: $error")
    }
  }

  it should "agree with validate for every stage count it accepts" in {
    // 600s is above the floor for all of 1..10 stages (600 / (2 * 10) = 30 >= 10), so every one of
    // them MUST be accepted — asserting Right rather than tolerating a Left keeps this from passing
    // against an implementation that rejects everything.
    val frequency = Frequency(TransformTimeUnit.Minutes, 10) // 600s
    (1 to 10).foreach { stages =>
      withClue(s"stages=$stages: ") {
        Delay.calculateOptimal(frequency, stages) match {
          case Right(delay) =>
            withClue(s"delay=${delay.toSeconds}s: ") {
              Delay.validate(delay, frequency, stages) shouldBe Right(())
            }
          case Left(error) => fail(s"Expected a delay, got: $error")
        }
      }
    }
  }
}
