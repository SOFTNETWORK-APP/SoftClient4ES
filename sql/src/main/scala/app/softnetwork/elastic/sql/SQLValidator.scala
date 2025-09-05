package app.softnetwork.elastic.sql

object SQLValidator {

  def validateChain(functions: List[SQLFunction]): Either[String, Unit] = {
    functions
      .collect { case f: SQLTypedFunction[_, _] => f }
      .sliding(2)
      .foreach {
        case Seq(f1, f2) =>
          if (!f1.in(f2)) {
            return Left(s"Type mismatch: ${f2.outputType} -> ${f1.inputType}")
          }
        case _ => // ok
      }
    Right(())
  }
}
