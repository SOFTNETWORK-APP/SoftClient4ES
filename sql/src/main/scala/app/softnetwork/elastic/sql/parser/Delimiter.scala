package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{Expr, Token}

sealed trait Delimiter extends Token

sealed trait StartDelimiter extends Delimiter

case object StartPredicate extends Expr("(") with StartDelimiter
case object StartCase extends Expr("case") with StartDelimiter
case object WhenCase extends Expr("when") with StartDelimiter

sealed trait EndDelimiter extends Delimiter

case object EndPredicate extends Expr(")") with EndDelimiter
case object Separator extends Expr(",") with EndDelimiter
case object EndCase extends Expr("end") with EndDelimiter
case object ThenCase extends Expr("then") with EndDelimiter
