package app.softnetwork.elastic.sql

trait SQLOperator extends SQLToken with PainlessScript with SQLRegex {
  override def painless: String = this match {
    case And          => "&&"
    case Or           => "||"
    case Not          => "!"
    case In           => ".contains"
    case Like | Match => ".matches"
    case Eq           => "=="
    case Ne           => "!="
    case Plus         => ".plus"
    case Minus        => ".minus"
    case IsNull       => " == null"
    case IsNotNull    => " != null"
    case _            => sql
  }
}

sealed trait BinaryOperator extends SQLOperator

sealed trait ArithmeticOperator extends SQLOperator {
  override def toString: String = s" $sql "
}

sealed trait BinaryArithmeticOperator extends ArithmeticOperator with BinaryOperator

sealed trait IntervalOperator extends BinaryArithmeticOperator with MathScript {
  override def script: String = sql
}
case object Plus extends SQLExpr("+") with IntervalOperator {
  override def painless: String = ".plus"
}
case object Minus extends SQLExpr("-") with IntervalOperator {
  override def painless: String = ".minus"
}

case object Add extends SQLExpr("+") with IntervalOperator
case object Subtract extends SQLExpr("-") with IntervalOperator

case object Multiply extends SQLExpr("*") with BinaryArithmeticOperator
case object Divide extends SQLExpr("/") with BinaryArithmeticOperator
case object Modulo extends SQLExpr("%") with BinaryArithmeticOperator

sealed trait UnaryArithmeticOperator extends ArithmeticOperator {
  override def painless: String = s"Math.${sql.toLowerCase()}"
}

case object Abs extends SQLExpr("abs") with UnaryArithmeticOperator
case object Ceil extends SQLExpr("ceil") with UnaryArithmeticOperator
case object Floor extends SQLExpr("floor") with UnaryArithmeticOperator
case object Round extends SQLExpr("round") with UnaryArithmeticOperator
case object Exp extends SQLExpr("exp") with UnaryArithmeticOperator
case object Log extends SQLExpr("log") with UnaryArithmeticOperator
case object Log10 extends SQLExpr("log10") with UnaryArithmeticOperator
case object Pow extends SQLExpr("pow") with UnaryArithmeticOperator
case object Sqrt extends SQLExpr("sqrt") with UnaryArithmeticOperator
case object Sign extends SQLExpr("sign") with UnaryArithmeticOperator
case object Pi extends SQLExpr("pi") with UnaryArithmeticOperator {
  override def painless: String = "Math.PI"
}

sealed trait TrigonometricOperator extends UnaryArithmeticOperator

case object Sin extends SQLExpr("sin") with TrigonometricOperator
case object Asin extends SQLExpr("asin") with TrigonometricOperator
case object Cos extends SQLExpr("cos") with TrigonometricOperator
case object Acos extends SQLExpr("acos") with TrigonometricOperator
case object Tan extends SQLExpr("tan") with TrigonometricOperator
case object Atan extends SQLExpr("atan") with TrigonometricOperator
case object Atan2 extends SQLExpr("atan2") with TrigonometricOperator

sealed trait SQLExpressionOperator extends SQLOperator

sealed trait SQLComparisonOperator extends SQLExpressionOperator with PainlessScript {
  def not: SQLComparisonOperator = this match {
    case Eq        => Ne
    case Ne | Diff => Eq
    case Ge        => Lt
    case Gt        => Le
    case Le        => Gt
    case Lt        => Ge
  }
}

case object Eq extends SQLExpr("=") with SQLComparisonOperator
case object Ne extends SQLExpr("<>") with SQLComparisonOperator
case object Diff extends SQLExpr("!=") with SQLComparisonOperator
case object Ge extends SQLExpr(">=") with SQLComparisonOperator
case object Gt extends SQLExpr(">") with SQLComparisonOperator
case object Le extends SQLExpr("<=") with SQLComparisonOperator
case object Lt extends SQLExpr("<") with SQLComparisonOperator
case object In extends SQLExpr("in") with SQLComparisonOperator
case object Like extends SQLExpr("like") with SQLComparisonOperator
case object Between extends SQLExpr("between") with SQLComparisonOperator
case object IsNull extends SQLExpr("is null") with SQLComparisonOperator
case object IsNotNull extends SQLExpr("is not null") with SQLComparisonOperator

case object Match extends SQLExpr("match") with SQLComparisonOperator
case object Against extends SQLExpr("against") with SQLRegex

sealed trait SQLStringOperator extends SQLOperator {
  override def painless: String = s".${sql.toLowerCase()}()"
}
case object Concat extends SQLExpr("concat") with SQLStringOperator {
  override def painless: String = " + "
}
case object Lower extends SQLExpr("lower") with SQLStringOperator
case object Upper extends SQLExpr("upper") with SQLStringOperator
case object Trim extends SQLExpr("trim") with SQLStringOperator
//case object LTrim extends SQLExpr("ltrim") with SQLStringOperator
//case object RTrim extends SQLExpr("rtrim") with SQLStringOperator
case object Substring extends SQLExpr("substring") with SQLStringOperator {
  override def painless: String = ".substring"
}
case object To extends SQLExpr("to") with SQLRegex
case object Length extends SQLExpr("length") with SQLStringOperator

sealed trait SQLLogicalOperator extends SQLExpressionOperator

case object Not extends SQLExpr("not") with SQLLogicalOperator

sealed trait SQLPredicateOperator extends SQLLogicalOperator

case object And extends SQLExpr("and") with SQLPredicateOperator
case object Or extends SQLExpr("or") with SQLPredicateOperator

sealed trait SQLConditionalOperator extends SQLExpressionOperator
case object Coalesce extends SQLExpr("coalesce") with SQLConditionalOperator
case object IsNullFunction extends SQLExpr("isnull") with SQLConditionalOperator
case object IsNotNullFunction extends SQLExpr("isnotnull") with SQLConditionalOperator
case object NullIf extends SQLExpr("nullif") with SQLConditionalOperator
case object Exists extends SQLExpr("exists") with SQLConditionalOperator

case object Cast extends SQLExpr("cast") with SQLConditionalOperator
case object Case extends SQLExpr("case") with SQLConditionalOperator

case object When extends SQLExpr("when") with SQLRegex
case object Then extends SQLExpr("then") with SQLRegex
case object Else extends SQLExpr("else") with SQLRegex
case object End extends SQLExpr("end") with SQLRegex

case object Union extends SQLExpr("union") with SQLOperator with SQLRegex

sealed trait ElasticOperator extends SQLOperator with SQLRegex
case object Nested extends SQLExpr("nested") with ElasticOperator
case object Child extends SQLExpr("child") with ElasticOperator
case object Parent extends SQLExpr("parent") with ElasticOperator
