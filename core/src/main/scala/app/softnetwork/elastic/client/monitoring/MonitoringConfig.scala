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

package app.softnetwork.elastic.client.monitoring

import scala.concurrent.duration._

//format:off
/** Automatic monitoring configuration.
  *
  * @param enabled
  *   Enables/disables automatic monitoring
  * @param interval
  *   Interval between metric reports
  * @param failureRateThreshold
  *   Alert threshold for failure rate (%)
  * @param latencyThreshold
  *   Alert threshold for average latency (ms)
  *
  * @example
  * {{{
  * val monitoring = MonitoringConfig(
  *   enabled = true,
  *   interval = 30.seconds,
  *   failureRateThreshold = 10.0,  // Alert if > 10% failures
  *   latencyThreshold = 1000.0      // Alert if > 1000ms
  * )
  * }}}
  */
//format:on
case class MonitoringConfig(
  enabled: Boolean = true,
  interval: Duration = 30.seconds,
  failureRateThreshold: Double = 10.0,
  latencyThreshold: Double = 1000.0
)
