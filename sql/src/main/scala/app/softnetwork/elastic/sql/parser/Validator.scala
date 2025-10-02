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
    functions.map(_.validate()).find(_.isLeft) match {
      case Some(left) => return left
      case None       =>
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
