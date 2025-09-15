package app.softnetwork.elastic.sql

sealed trait SQLDelimiter extends SQLToken

sealed trait StartDelimiter extends SQLDelimiter
case object StartPredicate extends SQLExpr("(") with StartDelimiter
case object StartCase extends SQLExpr("case") with StartDelimiter
case object WhenCase extends SQLExpr("when") with StartDelimiter

sealed trait EndDelimiter extends SQLDelimiter
case object EndPredicate extends SQLExpr(")") with EndDelimiter
case object Separator extends SQLExpr(",") with EndDelimiter
case object EndCase extends SQLExpr("end") with EndDelimiter
case object ThenCase extends SQLExpr("then") with EndDelimiter
