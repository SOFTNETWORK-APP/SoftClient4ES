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

package app.softnetwork.elastic.model

import app.softnetwork.persistence.generateUUID
import app.softnetwork.persistence.model.Timestamped

import java.time.{Instant, LocalDate}

case class Parent(
  uuid: String,
  name: String,
  birthDate: LocalDate,
  children: Seq[Child]
) extends Timestamped {
  def addChild(child: Child): Parent = copy(children = children :+ child)
  lazy val createdDate: Instant = Instant.now()
  lazy val lastUpdated: Instant = Instant.now()
}

case class Child(name: String, birthDate: LocalDate, parentId: String)

object Parent {
  def apply(name: String, birthDate: LocalDate): Parent =
    apply(
      generateUUID(),
      name,
      birthDate
    )

  def apply(uuid: String, name: String, birthDate: LocalDate): Parent =
    apply(
      uuid,
      name,
      birthDate,
      Seq.empty[Child]
    )

  def apply(uuid: String, name: String, birthDate: LocalDate, children: Seq[Child]): Parent = {
    Parent(
      uuid = uuid,
      name = name,
      birthDate = birthDate,
      children = children
    )
  }

}
