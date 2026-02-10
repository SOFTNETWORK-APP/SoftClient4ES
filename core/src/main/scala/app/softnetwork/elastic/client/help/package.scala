package app.softnetwork.elastic.client

import org.json4s._
import org.json4s.native.JsonMethods._
import scala.io.Source
import scala.util.{Failure, Success, Try}

package object help {

  /** Help entry for SQL commands and functions
    */
  sealed trait HelpEntry {
    def name: String
    def category: HelpCategory
    def shortDescription: String
    def syntax: String
    def description: String
    def examples: Seq[HelpExample]
    def seeAlso: Seq[String]
  }

  case class HelpExample(
    description: String,
    sql: String,
    output: Option[String] = None
  )

  sealed trait HelpCategory {
    def name: String
    def order: Int
  }

  object HelpCategory {
    case object DDL extends HelpCategory { val name = "DDL"; val order = 1 }
    case object DML extends HelpCategory { val name = "DML"; val order = 2 }
    case object DQL extends HelpCategory { val name = "DQL"; val order = 3 }
    case object Function extends HelpCategory { val name = "Functions"; val order = 4 }
    case object Operator extends HelpCategory { val name = "Operators"; val order = 5 }
    case object Meta extends HelpCategory { val name = "Meta Commands"; val order = 6 }

    val all: Seq[HelpCategory] = Seq(DDL, DML, DQL, Function, Operator, Meta)
  }

  /** SQL Command help entry
    */
  case class SqlCommandHelp(
    name: String,
    category: HelpCategory,
    shortDescription: String,
    syntax: String,
    description: String,
    clauses: Seq[ClauseHelp] = Seq.empty,
    examples: Seq[HelpExample] = Seq.empty,
    seeAlso: Seq[String] = Seq.empty,
    notes: Seq[String] = Seq.empty,
    limitations: Seq[String] = Seq.empty
  ) extends HelpEntry

  case class ClauseHelp(
    name: String,
    description: String,
    optional: Boolean = false
  )

  /** Function help entry
    */
  case class FunctionHelp(
    name: String,
    category: HelpCategory = HelpCategory.Function,
    shortDescription: String,
    syntax: String,
    description: String,
    parameters: Seq[ParameterHelp] = Seq.empty,
    returnType: String,
    examples: Seq[HelpExample] = Seq.empty,
    seeAlso: Seq[String] = Seq.empty,
    notes: Seq[String] = Seq.empty
  ) extends HelpEntry

  case class ParameterHelp(
    name: String,
    dataType: String,
    description: String,
    optional: Boolean = false,
    defaultValue: Option[String] = None
  )

  // ==================== Help JSON Loader ====================

  /** JSON model classes for help documentation
    */
  case class HelpClauseJson(
    name: String,
    description: String,
    optional: Boolean = false,
    variants: Option[List[String]] = None,
    modifiers: Option[List[String]] = None
  )

  case class HelpExampleJson(
    title: String,
    description: Option[String] = None,
    sql: String,
    output: Option[String] = None
  )

  case class HelpParameterJson(
    name: String,
    `type`: String,
    description: String,
    optional: Boolean = false,
    defaultValue: Option[String] = None
  )

  case class SqlCommandJson(
    name: String,
    category: String,
    shortDescription: String,
    syntax: List[String],
    description: String,
    clauses: Option[List[HelpClauseJson]] = None,
    examples: List[HelpExampleJson] = List.empty,
    notes: List[String] = List.empty,
    limitations: List[String] = List.empty,
    seeAlso: List[String] = List.empty,
    minVersion: Option[String] = None,
    aliases: List[String] = List.empty
  )

  case class FunctionJson(
    name: String,
    category: String,
    shortDescription: String,
    syntax: List[String],
    description: String,
    parameters: List[HelpParameterJson] = List.empty,
    returnType: String,
    examples: List[HelpExampleJson] = List.empty,
    notes: List[String] = List.empty,
    seeAlso: List[String] = List.empty,
    aliases: List[String] = List.empty
  )

}
