package app.softnetwork.elastic.sql

object SQLValidator {

  def validateChain(functions: List[SQLFunction]): Either[String, Unit] = {
    // validate function chain type compatibility
    val unaryFuncs = functions.collect { case f: SQLUnaryFunction[_, _] => f }
    unaryFuncs.sliding(2).foreach {
      case Seq(f1, f2) =>
        if (!SQLTypeCompatibility.matches(f2.outputType, f1.inputType)) {
          return Left(
            s"Type mismatch: output '${f2.outputType.typeId}' of `${f2.sql}` " +
            s"is not compatible with input '${f1.inputType.typeId}' of `${f1.sql}`"
          )
        }
      case _ => // ok
    }
    Right(())
  }
}

trait SQLValidation {
  def validate(): Either[String, Unit] = Right(())
}

case class SQLValidationError(message: String) extends Exception(message)
