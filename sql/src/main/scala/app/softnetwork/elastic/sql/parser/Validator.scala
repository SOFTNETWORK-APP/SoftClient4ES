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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils}
import app.softnetwork.elastic.sql.function.{Function, FunctionN}

object Validator {

  def validateChain(functions: List[Function]): Either[String, Unit] = {
    // validate function chain type compatibility
    functions match {
      case Nil => return Right(())
      case _   =>
    }
    functions.map(_.validate()).filter(_.isLeft) match {
      case Nil    => // ok
      case errors => return Left(errors.map { case Left(err) => err }.mkString("\n"))
    }
    val funcs = functions.collect { case f: FunctionN[_, _] => f }
    funcs.sliding(2).foreach {
      case Seq(f1, f2) =>
        validateTypesMatching(f2.outputType, f1.inputType)
      case _ => // ok
    }
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
  def validate(): Either[String, Unit] = Right(())
}

case class ValidationError(message: String) extends Exception(message)
