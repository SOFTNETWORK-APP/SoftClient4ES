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

package app.softnetwork.elastic.utils

import cron4s.{Cron, CronExpr}
import cron4s.lib.javatime._

import java.time.temporal.ChronoUnit
import java.time.ZonedDateTime

object CronIntervalCalculator {

  /** Calculate interval in seconds between two consecutive cron executions
    *
    * @param cronExpression
    *   Cron expression
    * @param from
    *   Optional starting time (default: now)
    * @return
    *   Interval in seconds, or None if cron is invalid
    */
  def calculateInterval(
    cronExpression: String,
    from: ZonedDateTime = ZonedDateTime.now()
  ): Option[Long] = {
    Cron(cronExpression).toOption.flatMap { cron =>
      for {
        firstExecution  <- cron.next(from)
        secondExecution <- cron.next(firstExecution)
      } yield ChronoUnit.SECONDS.between(firstExecution, secondExecution)
    }
  }

  /** Calculate average interval for variable cron expressions (useful for expressions like "0 0 * *
    * MON,WED,FRI")
    *
    * @param cronExpression
    *   Cron expression
    * @param samples
    *   Number of samples to calculate average (default: 10)
    * @return
    *   Average interval in seconds
    */
  def calculateAverageInterval(
    cronExpression: String,
    samples: Int = 10
  ): Option[Long] = {
    Cron(cronExpression).toOption.flatMap { cron =>
      val intervals = (1 to samples)
        .foldLeft(
          (ZonedDateTime.now(), List.empty[Long])
        ) { case ((currentTime, intervals), _) =>
          cron.next(currentTime) match {
            case Some(nextTime) =>
              val interval = ChronoUnit.SECONDS.between(currentTime, nextTime)
              (nextTime, intervals :+ interval)
            case None => (currentTime, intervals)
          }
        }
        ._2

      if (intervals.nonEmpty) {
        Some(intervals.sum / intervals.length)
      } else {
        None
      }
    }
  }

  /** Get detailed schedule information */
  def getScheduleInfo(cronExpression: String): Option[ScheduleInfo] = {
    Cron(cronExpression).toOption.flatMap { cron =>
      val now = ZonedDateTime.now()

      for {
        next1 <- cron.next(now)
        next2 <- cron.next(next1)
        next3 <- cron.next(next2)
      } yield {
        val interval1 = ChronoUnit.SECONDS.between(next1, next2)
        val interval2 = ChronoUnit.SECONDS.between(next2, next3)

        ScheduleInfo(
          cronExpression = cronExpression,
          nextExecutions = Seq(next1, next2, next3),
          firstInterval = interval1,
          secondInterval = interval2,
          isConstantInterval = interval1 == interval2
        )
      }
    }
  }

  def validateAndCalculate(cron: String): Either[String, (CronExpr, Long)] = {
    Cron(cron) match {
      case Right(expr) =>
        calculateInterval(cron) match {
          case Some(interval) => Right((expr, interval))
          case None           => Left("Cannot calculate interval between executions")
        }
      case Left(error) =>
        Left(s"Parse error: ${error.getMessage}")
    }
  }
}

case class ScheduleInfo(
  cronExpression: String,
  nextExecutions: Seq[ZonedDateTime],
  firstInterval: Long,
  secondInterval: Long,
  isConstantInterval: Boolean
) {
  def prettyPrint(): String = {
    s"""Schedule Info for: $cronExpression
       |Next executions:
       |  1. ${nextExecutions.head}
       |  2. ${nextExecutions(1)}
       |  3. ${nextExecutions(2)}
       |
       |Intervals:
       |  Between 1 and 2: $firstInterval seconds (${firstInterval / 60} minutes)
       |  Between 2 and 3: $secondInterval seconds (${secondInterval / 60} minutes)
       |  Constant interval: $isConstantInterval
       |""".stripMargin
  }
}
