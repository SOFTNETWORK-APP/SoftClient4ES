package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.Expr
import app.softnetwork.elastic.sql.operator.Operator

package object geo {

  case object Distance extends Expr("distance") with Function with Operator

}
