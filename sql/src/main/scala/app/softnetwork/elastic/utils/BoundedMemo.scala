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

import java.util.concurrent.ConcurrentHashMap

/** The memo of a PURE function of `K`, thread-safe and bounded: one entry per distinct key, at most
  * `cap` of them. Past the cap a key is computed on each call and not stored, so a caller feeding
  * unbounded distinct keys costs what it did without the memo, never unbounded memory.
  *
  * Every caller of a stored key gets the SAME instance: a first use raced by several threads keeps
  * the first insert, and each racing thread returns it. A value is therefore shared across threads
  * and must be immutable, or safe to share. Concurrent first inserts may pass the cap by the number
  * of racing threads.
  */
private[elastic] final class BoundedMemo[K <: AnyRef, V <: AnyRef](val cap: Int) {

  private[this] val values = new ConcurrentHashMap[K, V]()

  /** The value stored for `key`, else `compute`d: stored while the memo is below its cap. */
  def apply(key: K)(compute: => V): V = {
    val known = values.get(key)
    if (known ne null) known
    else {
      val computed = compute
      if (values.size() < cap) {
        val winner = values.putIfAbsent(key, computed)
        if (winner eq null) computed else winner
      } else computed
    }
  }

  /** How many keys are stored. */
  def size: Int = values.size()
}
