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

package app.softnetwork.elastic.sql.macros

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SQLSearchRequest

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/** Reusable core validation logic for all SQL macros.
  */
trait SQLQueryValidator {

  /** Validates an SQL query against a type T. Returns the SQL query if valid, otherwise aborts
    * compilation.
    * @note
    *   query fields must not exist in case class because we are using Jackson to deserialize the
    *   results with the following option DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false
    */
  protected def validateSQLQuery[T: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String]
  ): String = {

    debug(c)("🚀 MACRO IS BEING CALLED!")

    // 1. Extract the SQL query (must be a literal)
    val sqlQuery = extractSQLString(c)(query)

    if (sys.props.get("elastic.sql.debug").contains("true")) {
      c.info(c.enclosingPosition, s"Validating SQL: $sqlQuery", force = false)
    }

    // 2. Parse the SQL query
    val parsedQuery = parseSQLQuery(c)(sqlQuery)

    // ============================================================
    // Reject SELECT *
    // ============================================================
    rejectSelectStar(c)(parsedQuery, sqlQuery)

    // 3. Extract the selected fields from the query
    val queryFields = extractQueryFields(parsedQuery)

    debug(c)(s"🔍 Parsed fields: ${queryFields.mkString(", ")}")

    // 4. Extract the required fields from case class T
    val tpe = c.weakTypeOf[T]
    val requiredFields = getRequiredFields(c)(tpe)
    debug(c)(s"📦 Case class fields: ${requiredFields.mkString(", ")}")

    // 5. Validate: missing case class fields must have defaults or be Option
    validateRequiredFields(c)(queryFields)

    // 7. Validate the types
    validateTypes(c)(parsedQuery, requiredFields.map(values => values._1 -> values._2._1))

    debug(c)("=" * 80)
    debug(c)("✅ SQL Query Validation Complete")
    debug(c)("=" * 80)

    // 8. Return the validated request
    sqlQuery
  }

  // ============================================================
  // Helper Methods
  // ============================================================

  /** Extracts SQL string from various tree structures. Supports: literals, .stripMargin, and simple
    * expressions.
    */
  private def extractSQLString(c: blackbox.Context)(query: c.Expr[String]): String = {
    import c.universe._

    debug(c)("=" * 80)
    debug(c)("🔍 Starting SQL Query Validation")
    debug(c)("=" * 80)

    val sqlString =
      (query match {
        // Case 1: Direct string literal
        // Example: "SELECT * FROM table"
        case Literal(Constant(sql: String)) =>
          debug(c)("📝 Detected: Direct string literal")
          Some(sql)

        // Case 2: String with .stripMargin
        // Example: """SELECT * FROM table""".stripMargin
        case Select(Literal(Constant(sql: String)), TermName("stripMargin")) =>
          debug(c)("📝 Detected: String with .stripMargin")
          Some(sql.stripMargin)

        // Case 3: Try to evaluate as compile-time constant
        case _ =>
          debug(c)(s"⚠️  Attempting to evaluate: ${showCode(query.tree)}")
          try {
            val evaluated = c.eval(c.Expr[String](c.untypecheck(query.tree.duplicate)))
            debug(c)(s"✅ Successfully evaluated to: $evaluated")
            Some(evaluated)
          } catch {
            case e: Throwable =>
              debug(c)(s"❌ Could not evaluate: ${e.getMessage}")
              None
          }
      }).getOrElse {
        c.abort(
          c.enclosingPosition,
          s"""❌ SQL query must be a compile-time constant for validation.
           |
           |Found: ${showCode(query.tree)}
           |Tree structure: ${showRaw(query.tree)}
           |
           |✅ Valid usage:
           |  scrollAs[Product]("SELECT id, name FROM products")
           |  scrollAs[Product](\"\"\"SELECT id, name FROM products\"\"\".stripMargin)
           |
           |❌ For dynamic queries, use:
           |  scrollAsUnchecked[Product](SQLQuery(dynamicSql), ScrollConfig())
           |
           |""".stripMargin
        )
      }

    debug(c)(s"📝 Extracted SQL: $sqlString")

    sqlString
  }

  // ============================================================
  // Helper: Parse SQL query into SQLSearchRequest
  // ============================================================
  private def parseSQLQuery(c: blackbox.Context)(sqlQuery: String): SQLSearchRequest = {
    Parser(sqlQuery) match {
      case Right(Left(request)) =>
        request

      case Right(Right(multi)) =>
        multi.requests.headOption.getOrElse {
          c.abort(c.enclosingPosition, "Empty multi-search query")
        }

      case Left(error) =>
        c.abort(
          c.enclosingPosition,
          s"❌ SQL parsing error: ${error.msg}\n" +
          s"Query: $sqlQuery"
        )
    }
  }

  // ============================================================
  // Reject SELECT * (incompatible with compile-time validation)
  // ============================================================
  private def rejectSelectStar(c: blackbox.Context)(
    parsedQuery: SQLSearchRequest,
    sqlQuery: String
  ): Unit = {

    // Check if any field is a wildcard (*)
    val hasWildcard = parsedQuery.select.fields.exists { field =>
      field.identifier.name == "*"
    }

    if (hasWildcard) {
      c.abort(
        c.enclosingPosition,
        s"""❌ SELECT * is not allowed with compile-time validation.
           |
           |Query: $sqlQuery
           |
           |Reason:
           |  • Cannot validate field existence at compile-time
           |  • Cannot validate type compatibility at compile-time
           |  • Schema changes will break silently at runtime
           |
           |Solution:
           |  1. Explicitly list all required fields:
           |     SELECT id, name, price FROM products
           |
           |  2. Use the *Unchecked() variant for dynamic queries:
           |     searchAsUnchecked[Product](SQLQuery("SELECT * FROM products"))
           |
           |Best Practice:
           |  Always explicitly select only the fields you need.
           |""".stripMargin
      )
    }

    debug(c)("✅ No SELECT * detected")
  }

  // ============================================================
  // Helper: Detect if a type is a collection
  // ============================================================
  private def isCollectionType(c: blackbox.Context)(tpe: c.universe.Type): Boolean = {
    import c.universe._

    val collectionTypes = Set(
      typeOf[List[_]].typeConstructor,
      typeOf[Seq[_]].typeConstructor,
      typeOf[Vector[_]].typeConstructor,
      typeOf[Set[_]].typeConstructor,
      typeOf[Array[_]].typeConstructor
    )

    collectionTypes.exists(collType => tpe.typeConstructor <:< collType)
  }

  // ============================================================
  // Helper: Extract the element type from a collection
  // ============================================================
  private def getCollectionElementType(
    c: blackbox.Context
  )(tpe: c.universe.Type): Option[c.universe.Type] = {
    if (isCollectionType(c)(tpe)) {
      tpe.typeArgs.headOption
    } else {
      None
    }
  }

  // ============================================================
  // Helper: Extract the required fields from a class case
  // ============================================================
  private def getRequiredFields(
    c: blackbox.Context
  )(tpe: c.universe.Type): Map[String, (c.universe.Type, Boolean, Boolean)] = {
    import c.universe._

    val constructor = tpe.decls
      .collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      }
      .getOrElse {
        c.abort(c.enclosingPosition, s"No primary constructor found for $tpe")
      }

    constructor.paramLists.flatten.flatMap { param =>
      val paramName = param.name.decodedName.toString
      val paramType = param.typeSignature

      // Check if the parameter has a default value or is an option.
      val isOption = paramType <:< typeOf[Option[_]]

      val hasDefault = param.asTerm.isParamWithDefault

      /* We should not filter out optional parameters here,
         because we need to know all fields to validate their types later.

      if (isOption || hasDefault) {
        None
      } else {
        Some((paramName, (paramType, isOption, hasDefault)))
      }*/

      Some((paramName, (paramType, isOption, hasDefault)))

    }.toMap
  }

  /** Extracts selected fields from the parsed SQL query.
    */
  private def extractQueryFields(parsedQuery: SQLSearchRequest): Set[String] = {
    parsedQuery.select.fields.flatMap { field =>
      val f = field.fieldAlias.map(_.alias).getOrElse(field.identifier.name)
      /*field.identifier.nestedElement match {
        case Some(nested) => List(f, nested.innerHitsName)
        case None         => List(f)
      }*/
      List(f)
    }.toSet
  }

  // ============================================================
  // Helper: Validate required vs. selected fields
  // ============================================================
  private def validateRequiredFields[T: c.WeakTypeTag](
    c: blackbox.Context
  )(
    queryFields: Set[String]
  ): Unit = {
    import c.universe._

    val tpe = weakTypeOf[T]
    val requiredFields = getRequiredFields(c)(tpe)

    val missingFields = requiredFields.filterNot {
      case (fieldName, (fieldType, isOption, hasDefault)) =>
        // ✅ Check if the field is selected
        val isSelected = queryFields.contains(fieldName)

        if (!isSelected) {
          debug(c)(s"⚠️ Missing field: $fieldName")

          if (isOption) {
            debug(c)(s"✅ Field '$fieldName' is Option - OK")
            true
          } else if (hasDefault) {
            debug(c)(s"✅ Field '$fieldName' has default value - OK")
            true
          }
          // ✅ If it's a collection, check if its nested fields are selected.
          else if (isCollectionType(c)(fieldType)) {
            getCollectionElementType(c)(fieldType) match {
              case Some(elementType) =>
                // Check if the nested fields of the collection are selected
                // Eg: "children.name", "children.birthDate"
                val nestedFields = getRequiredFields(c)(elementType)
                val hasNestedFields = nestedFields.forall { case (nestedFieldName, _) =>
                  queryFields.exists(f => f.startsWith(s"$fieldName.$nestedFieldName"))
                }

                if (hasNestedFields) {
                  // ✅ The nested fields are present, so the collection is considered valid.
                  debug(c)(s"✅ Collection field '$fieldName' validated via nested fields")
                }

                hasNestedFields

              case None => false
            }
          } else {
            false
          }
        } else {
          true
        }
    }

    if (missingFields.nonEmpty) {
      val missingFieldNames = missingFields.keys.mkString(", ")
      val exampleFields = (queryFields ++ missingFields.keys).mkString(", ")

      val unknownFields = queryFields.filterNot(f => requiredFields.contains(f))
      val suggestions = unknownFields.flatMap { unknown =>
        findClosestMatch(unknown, missingFields.keys.toSeq)
      }
      val suggestionMsg = if (suggestions.nonEmpty) {
        s"\nDid you mean: ${suggestions.mkString(", ")}?"
      } else ""

      c.abort(
        c.enclosingPosition,
        s"""❌ SQL query does not select the following required fields from ${tpe.typeSymbol.name}:
           |$missingFieldNames$suggestionMsg
           |
           |These fields are missing from the query:
           |SELECT ${exampleFields} FROM ...
           |
           |To fix this, either:
           |  1. Add them to the SELECT clause
           |  2. Make them Option[T] in the case class
           |  3. Provide default values in the case class definition""".stripMargin
      )
    }
  }

  // ============================================================
  // Helper: Validate Type compatibility
  // ============================================================
  private def validateTypes(c: blackbox.Context)(
    parsedQuery: SQLSearchRequest,
    requiredFields: Map[String, c.universe.Type]
  ): Unit = {

    parsedQuery.select.fields.foreach { field =>
      val fieldName = field.fieldAlias.map(_.alias).getOrElse(field.identifier.name)

      (field.out, requiredFields.get(fieldName)) match {
        case (sqlType, Some(scalaType)) =>
          if (!areTypesCompatible(c)(sqlType, scalaType)) {
            c.abort(
              c.enclosingPosition,
              s"Type mismatch for field '$fieldName': " +
              s"SQL type $sqlType is incompatible with Scala type ${scalaType.toString}\n" +
              s"Expected one of: ${getCompatibleScalaTypes(sqlType)}"
            )
          }
        case _ => // Cannot validate without type info
      }
    }

    debug(c)("✅ Type validation passed")
  }

  // ============================================================
  // Helper: Check if SQL type is compatible with Scala type
  // ============================================================
  private def areTypesCompatible(c: blackbox.Context)(
    sqlType: SQLType,
    scalaType: c.universe.Type
  ): Boolean = {
    import c.universe._

    sqlType match {
      case SQLTypes.TinyInt =>
        scalaType =:= typeOf[Byte] ||
          scalaType =:= typeOf[Short] ||
          scalaType =:= typeOf[Int] ||
          scalaType =:= typeOf[Long] ||
          scalaType =:= typeOf[Option[Byte]] ||
          scalaType =:= typeOf[Option[Short]] ||
          scalaType =:= typeOf[Option[Int]] ||
          scalaType =:= typeOf[Option[Long]]

      case SQLTypes.SmallInt =>
        scalaType =:= typeOf[Short] ||
          scalaType =:= typeOf[Int] ||
          scalaType =:= typeOf[Long] ||
          scalaType =:= typeOf[Option[Short]] ||
          scalaType =:= typeOf[Option[Int]] ||
          scalaType =:= typeOf[Option[Long]]

      case SQLTypes.Int =>
        scalaType =:= typeOf[Int] ||
          scalaType =:= typeOf[Long] ||
          scalaType =:= typeOf[Option[Int]] ||
          scalaType =:= typeOf[Option[Long]]

      case SQLTypes.BigInt =>
        scalaType =:= typeOf[Long] ||
          scalaType =:= typeOf[BigInt] ||
          scalaType =:= typeOf[Option[Long]] ||
          scalaType =:= typeOf[Option[BigInt]]

      case SQLTypes.Double | SQLTypes.Real =>
        scalaType =:= typeOf[Double] ||
          scalaType =:= typeOf[Float] ||
          scalaType =:= typeOf[Option[Double]] ||
          scalaType =:= typeOf[Option[Float]]

      case SQLTypes.Char =>
        scalaType =:= typeOf[String] || // CHAR(n) → String
          scalaType =:= typeOf[Char] || // CHAR(1) → Char
          scalaType =:= typeOf[Option[String]] ||
          scalaType =:= typeOf[Option[Char]]

      case SQLTypes.Varchar =>
        scalaType =:= typeOf[String] ||
          scalaType =:= typeOf[Option[String]]

      case SQLTypes.Boolean =>
        scalaType =:= typeOf[Boolean] ||
          scalaType =:= typeOf[Option[Boolean]]

      case SQLTypes.Time =>
        scalaType.toString.contains("Instant") ||
          scalaType.toString.contains("LocalTime")

      case SQLTypes.Date =>
        scalaType.toString.contains("Date") ||
          scalaType.toString.contains("Instant") ||
          scalaType.toString.contains("LocalDate")

      case SQLTypes.DateTime | SQLTypes.Timestamp =>
        scalaType.toString.contains("LocalDateTime") ||
          scalaType.toString.contains("ZonedDateTime") ||
          scalaType.toString.contains("Instant")

      case SQLTypes.Struct =>
        if (scalaType.typeSymbol.isClass && scalaType.typeSymbol.asClass.isCaseClass) {
          // validateStructFields(c)(sqlField, scalaType)
          true
        } else {
          false
        }

      case _ =>
        true // Cannot validate unknown types
    }
  }

  // ============================================================
  // Helper: Get compatible Scala types for a given SQL type
  // ============================================================
  private def getCompatibleScalaTypes(sqlType: SQLType): String = {
    sqlType match {
      case SQLTypes.TinyInt =>
        "Byte, Short, Int, Long, Option[Byte], Option[Short], Option[Int], Option[Long]"
      case SQLTypes.SmallInt => "Short, Int, Long, Option[Short], Option[Int], Option[Long]"
      case SQLTypes.Int      => "Int, Long, Option[Int], Option[Long]"
      case SQLTypes.BigInt   => "Long, BigInt, Option[Long], Option[BigInt]"
      case SQLTypes.Double | SQLTypes.Real => "Double, Float, Option[Double], Option[Float]"
      case SQLTypes.Varchar                => "String, Option[String]"
      case SQLTypes.Char                   => "String, Char, Option[String], Option[Char]"
      case SQLTypes.Boolean                => "Boolean, Option[Boolean]"
      case SQLTypes.Time                   => "java.time.LocalTime, java.time.Instant"
      case SQLTypes.Date => "java.time.LocalDate, java.time.Instant, java.util.Date"
      case SQLTypes.DateTime | SQLTypes.Timestamp =>
        "java.time.LocalDateTime, java.time.ZonedDateTime, java.time.Instant"
      case SQLTypes.Struct => "Case Class"
      case _               => "Unknown"
    }
  }

  // ============================================================
  // Helper: Find closest matching field name
  // ============================================================
  private def findClosestMatch(target: String, candidates: Seq[String]): Option[String] = {
    if (candidates.isEmpty) None
    else {
      val distances = candidates.map { candidate =>
        (candidate, levenshteinDistance(target.toLowerCase, candidate.toLowerCase))
      }
      val closest = distances.minBy(_._2)
      if (closest._2 <= 3) Some(closest._1) else None
    }
  }

  // ============================================================
  // Helper: Compute Levenshtein distance between two strings
  // ============================================================
  private def levenshteinDistance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) =>
      if (j == 0) i else if (i == 0) j else 0
    }

    for {
      j <- 1 to s2.length
      i <- 1 to s1.length
    } {
      dist(j)(i) =
        if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else (dist(j - 1)(i) min dist(j)(i - 1) min dist(j - 1)(i - 1)) + 1
    }

    dist(s2.length)(s1.length)
  }

  // ============================================================
  // Helper: Debug logging
  // ============================================================
  private def debug(c: blackbox.Context)(msg: String): Unit = {
    if (SQLQueryValidator.DEBUG) {
      c.info(c.enclosingPosition, msg, force = true)
    }
  }
}

object SQLQueryValidator {
  val DEBUG: Boolean = sys.props.get("sql.macro.debug").contains("true")
}
