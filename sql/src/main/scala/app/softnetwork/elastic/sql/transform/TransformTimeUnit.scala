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

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.schema.mapper
import com.fasterxml.jackson.databind.JsonNode

sealed trait TransformTimeUnit extends DdlToken {
  def name: String
  def sql: String = name
  def format: String
}

object TransformTimeUnit {
  case object Milliseconds extends TransformTimeUnit {
    val name: String = "MILLISECONDS"
    override def format: String = "ms"
  }

  case object Seconds extends TransformTimeUnit {
    val name: String = "SECONDS"
    override def format: String = "s"
  }

  case object Minutes extends TransformTimeUnit {
    val name: String = "MINUTES"
    override def format: String = "m"
  }

  case object Hours extends TransformTimeUnit {
    val name: String = "HOURS"
    override def format: String = "h"
  }

  case object Days extends TransformTimeUnit {
    val name: String = "DAYS"
    override def format: String = "d"
  }

  case object Weeks extends TransformTimeUnit {
    val name: String = "WEEKS"
    override def format: String = "w"
  }

  case object Months extends TransformTimeUnit {
    val name: String = "MONTHS"
    override def format: String = "M"
  }

  case object Years extends TransformTimeUnit {
    val name: String = "YEARS"
    override def format: String = "y"
  }

  def apply(name: String): TransformTimeUnit = name.toUpperCase() match {
    case "MILLISECOND" | "MILLISECONDS" => Milliseconds
    case "SECOND" | "SECONDS"           => Seconds
    case "MINUTE" | "MINUTES"           => Minutes
    case "HOUR" | "HOURS"               => Hours
    case "DAY" | "DAYS"                 => Days
    case "WEEK" | "WEEKS"               => Weeks
    case "MONTH" | "MONTHS"             => Months
    case "YEAR" | "YEARS"               => Years
    case other => throw new IllegalArgumentException(s"Invalid delay unit: $other")
  }
}

sealed trait TransformTimeInterval extends DdlToken {
  def interval: Long
  def timeUnit: TransformTimeUnit
  def toSeconds: Long = timeUnit match {
    case TransformTimeUnit.Milliseconds => interval / 1000
    case TransformTimeUnit.Seconds      => interval
    case TransformTimeUnit.Minutes      => interval * 60
    case TransformTimeUnit.Hours        => interval * 3600
    case TransformTimeUnit.Days         => interval * 86400
    case TransformTimeUnit.Weeks        => interval * 604800
    case TransformTimeUnit.Months       => interval * 2592000
    case TransformTimeUnit.Years        => interval * 31536000
  }
  def toTransformFormat: String = s"$interval${timeUnit.format}"

}

object TransformTimeInterval {

  /** Creates a time interval from seconds */
  def fromSeconds(seconds: Long): (TransformTimeUnit, Long) = {
    if (seconds >= 31536000 && seconds % 31536000 == 0) {
      (TransformTimeUnit.Years, seconds / 31536000)
    } else if (seconds >= 2592000 && seconds % 2592000 == 0) {
      (TransformTimeUnit.Months, seconds / 2592000)
    } else if (seconds >= 604800 && seconds % 604800 == 0) {
      (TransformTimeUnit.Weeks, seconds / 604800)
    } else if (seconds >= 86400 && seconds % 86400 == 0) {
      (TransformTimeUnit.Days, seconds / 86400)
    } else if (seconds >= 3600 && seconds % 3600 == 0) {
      (TransformTimeUnit.Hours, seconds / 3600)
    } else if (seconds >= 60 && seconds % 60 == 0) {
      (TransformTimeUnit.Minutes, seconds / 60)
    } else {
      (TransformTimeUnit.Seconds, seconds)
    }
  }

  def apply(value: String): Option[Delay] = {
    val regex = """(\d+)\s*(ms|s|m|h|d|w|M|y)""".r
    value match {
      case regex(interval, unitStr) =>
        val unit = unitStr match {
          case "ms" => TransformTimeUnit.Milliseconds
          case "s"  => TransformTimeUnit.Seconds
          case "m"  => TransformTimeUnit.Minutes
          case "h"  => TransformTimeUnit.Hours
          case "d"  => TransformTimeUnit.Days
          case "w"  => TransformTimeUnit.Weeks
          case "M"  => TransformTimeUnit.Months
          case "y"  => TransformTimeUnit.Years
          case _    => TransformTimeUnit.Seconds
        }
        Some(Delay(unit, interval.toLong))
      case _ => None
    }
  }
}

case class Delay(
  timeUnit: TransformTimeUnit,
  interval: Long
) extends TransformTimeInterval {
  def sql: String = s"WITH DELAY $interval $timeUnit"
}

object Delay {
  val Default: Delay = Delay(TransformTimeUnit.Minutes, 1)

  /** Smallest per-transform delay a calculated refresh chain may use, in seconds. */
  val MinDelaySeconds: Long = 10L

  def fromSeconds(seconds: Long): Delay = {
    val timeInterval = TransformTimeInterval.fromSeconds(seconds)
    Delay(timeInterval._1, timeInterval._2)
  }

  /** Calculates the optimal delay for one transform of a refresh chain.
    *
    * Hard constraint (identical to the one [[validate]] enforces): every transform runs every
    * `delay × 2`, so the whole chain fits inside `frequency` only when `delay ≤ frequency / (2 ×
    * nbStages)`. `bufferFactor` adds margin on top of that ceiling — it can only ever make the
    * delay smaller, never push it past the constraint. With the default `1.5` (indeed with any
    * value ≤ 2) the ceiling always wins and the buffered term is inert; it binds only above 2. Keep
    * the `min` regardless — it is what absorbs `bufferFactor = 0`, where the buffered term is
    * `Infinity.toLong == Long.MaxValue`.
    *
    * @param frequency
    *   Desired refresh frequency
    * @param nbStages
    *   Total number of stages (changelog + enrichment + computed + final). Values below 1 are
    *   clamped to 1: a materialized view always has at least one transform (the one that writes the
    *   view index), so a non-positive count is a caller-side accounting bug and must never become
    *   the user-visible face of a legitimate SQL statement (SoftClient4ES#185).
    * @param bufferFactor
    *   Safety factor (default 1.5)
    * @return
    *   Optimal delay for each Transform, or an actionable error naming the minimum frequency
    */
  def calculateOptimal(
    frequency: Frequency,
    nbStages: Int,
    bufferFactor: Double = 1.5
  ): Either[String, Delay] = {
    val stages = math.max(nbStages, 1)
    val frequencySeconds = frequency.toSeconds

    val maxDelaySeconds: Long = frequencySeconds / (2L * stages)
    val bufferedDelaySeconds: Long = (frequencySeconds / (stages * bufferFactor)).toLong
    val optimalDelaySeconds: Long = math.min(bufferedDelaySeconds, maxDelaySeconds)

    if (optimalDelaySeconds < MinDelaySeconds) {
      // The smallest frequency that would actually be accepted: BOTH terms of the `min` above must
      // reach MinDelaySeconds, so the ceiling needs `2 × stages × Min` and the buffered term needs
      // `stages × bufferFactor × Min` (rounded up, since the division truncates). Quoting only the
      // first would print an unreachable figure whenever bufferFactor > 2.
      val minFrequencySeconds: Long =
        math.max(
          2L * stages * MinDelaySeconds,
          math.ceil(stages * bufferFactor * MinDelaySeconds).toLong
        )
      Left(
        s"Calculated delay ($optimalDelaySeconds seconds) is too small. " +
        s"Consider increasing frequency or reducing number of stages. " +
        s"Minimum required frequency: $minFrequencySeconds seconds"
      )
    } else {
      Right(Delay.fromSeconds(optimalDelaySeconds))
    }
  }

  /** Validates that a chain of transforms can refresh within the given frequency
    *
    * Requirements:
    *   - Total latency (delay × nbStages) must be less than frequency
    *   - Each transform runs every (delay × 2), so: delay × 2 × nbStages ≤ frequency
    *
    * @param delay
    *   Delay for each transform
    * @param frequency
    *   Target refresh frequency
    * @param nbStages
    *   Number of stages in the chain
    * @return
    *   Success or error message
    */
  def validate(
    delay: Delay,
    frequency: Frequency,
    nbStages: Int
  ): Either[String, Unit] = {
    val totalLatency = delay.toSeconds * nbStages
    val frequencySeconds = frequency.toSeconds
    val requiredFrequency = delay.toSeconds * 2 * nbStages

    if (totalLatency > frequencySeconds) {
      Left(
        s"Total latency ($totalLatency seconds) exceeds frequency ($frequencySeconds seconds). " +
        s"Minimum required frequency: $requiredFrequency seconds"
      )
    } else if (requiredFrequency > frequencySeconds) {
      Left(
        s"Frequency ($frequencySeconds seconds) is too low for $nbStages stages with delay ${delay.toSeconds} seconds. " +
        s"Minimum required frequency: $requiredFrequency seconds"
      )
    } else {
      Right(())
    }
  }
}

case class Frequency(
  timeUnit: TransformTimeUnit,
  interval: Long
) extends TransformTimeInterval {
  def sql: String = s"REFRESH EVERY $interval $timeUnit"
}

case object Frequency {
  val Default: Frequency = apply(Delay.Default)
  def apply(delay: Delay): Frequency = Frequency(delay.timeUnit, delay.interval * 2)
  def fromSeconds(seconds: Long): Frequency = {
    val timeInterval = TransformTimeInterval.fromSeconds(seconds)
    Frequency(timeInterval._1, timeInterval._2)
  }
}

case class TransformSource(
  index: Seq[String],
  query: Option[Criteria]
) extends DdlToken {
  override def sql: String = {
    val queryStr = query.map(q => s" WHERE ${q.sql}").getOrElse("")
    s"INDEX (${index.mkString(", ")})$queryStr"
  }

  /** Converts to JSON for Elasticsearch
    */
  def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val indicesNode = mapper.createArrayNode()
    index.foreach(indicesNode.add)
    node.set("index", indicesNode)
    query.foreach { q =>
      node.set("query", implicitly[JsonNode](q))
      ()
    }
    node
  }
}
