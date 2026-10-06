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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils}
import app.softnetwork.elastic.sql.function.Function

object Validator {

  def validateChain(functions: List[Function]): Either[String, Unit] = {
    // every function of the chain, for its own syntax and structure
    functions match {
      case Nil => return Right(())
      case _   =>
    }
    functions.map(_.validate()).filter(_.isLeft) match {
      case Nil    => // ok
      case errors => return Left(errors.map { case Left(err) => err }.mkString("\n"))
    }
    // ⚠️ No chain-link TYPE check here. One used to sit at this point and its result was
    // DISCARDED (`foreach` over `validateTypesMatching`), so it never refused anything; it is not
    // moved to the post-resolution seam either, because it never applied (the lead's ruling of
    // 2026-10-05 moves the type checks that refuse, and adds none).
    Right(())
  }

  def validateTypesMatching(out: SQLType, in: SQLType): Either[String, Unit] = {
    if (SQLTypeUtils.matches(out, in)) {
      Right(())
    } else {
      Left(s"Type mismatch: output '${out.typeId}' is not compatible with input '${in.typeId}'")
    }
  }
}

trait Validation {

  /** The rules of SYNTAX and STRUCTURE, checked when the statement is parsed. */
  def validate(): Either[String, Unit] = Right(())

  /** This node's TYPE rule alone, if it breaks it: asked once the column types are known -- after
    * the schema is attached (`SingleSearch.validateResolved`) -- and never at parse time.
    *
    * 🔴 The lead's ruling of 2026-10-05: no refusal before the column types are known. At parse
    * time every column is `Any`, so a type rule asked there either accepts what it cannot judge or
    * refuses on a type it guessed: `d + 7 > CURRENT_DATE` was refused as BIGINT against DATE
    * because `d + 7` folded `[ANY, BIGINT]` to BIGINT. Every TYPE refusal waits for the types, a
    * refusal certain whatever the column's type (`x * CURRENT_DATE`) included; parsing checks
    * syntax and structure only. With no schema at all, nothing is refused on types and
    * Elasticsearch answers.
    */
  def typeError: Option[String] = None
}

case class ValidationError(message: String) extends Exception(message)
