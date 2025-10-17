/*
 * Copyright 2015 SOFTNETWORK
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

package app.softnetwork.elastic.persistence.typed

import app.softnetwork.persistence._

import app.softnetwork.persistence.model.Timestamped

import scala.language.implicitConversions

import app.softnetwork.persistence._

/** Created by smanciot on 10/04/2020.
  */
object Elastic {

  def index(_type: String): String = {
    s"${_type}s-$environment".toLowerCase
  }

  def alias(_type: String): String = {
    s"${_type}s-$environment-v$version".toLowerCase
  }

  def getAlias[T <: Timestamped](implicit m: Manifest[T]): String = {
    alias(getType[T])
  }

  def getIndex[T <: Timestamped](implicit m: Manifest[T]): String = {
    index(getType[T])
  }

}
