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

    c.echo(c.enclosingPosition, "🚀 MACRO IS BEING CALLED!")

    // 1. Extract the SQL query (must be a literal)
    val sqlQuery = extractStringLiteral(c)(query)

    if (sys.props.get("elastic.sql.debug").contains("true")) {
      c.info(c.enclosingPosition, s"Validating SQL: $sqlQuery", force = false)
    }

    // 2. Parse the SQL query
    val parsedQuery = parseSQLQuery(c)(sqlQuery)

    // ============================================================
    // ✅ NEW: Reject SELECT *
    // ============================================================
    rejectSelectStar(c)(parsedQuery, sqlQuery)

    // 3. Extract the selected fields
    val queryFields = extractQueryFields(parsedQuery)

    c.echo(c.enclosingPosition, s"🔍 Parsed fields: ${queryFields.mkString(", ")}")

    // 4. Extract the fields from case class T
    val tpe = c.weakTypeOf[T]
    val caseClassFields = extractCaseClassFields(c)(tpe)
    c.echo(c.enclosingPosition, s"📦 Case class fields: ${caseClassFields.mkString(", ")}")

    // 5. Validate: missing case class fields must have defaults or be Option
    validateMissingFieldsHaveDefaults(c)(queryFields, caseClassFields, tpe)

    // 7. Validate the types
    validateTypes(c)(parsedQuery, caseClassFields)

    // 8. Return the validated request
    sqlQuery
  }

  // ============================================================
  // Helper Methods
  // ============================================================

  private def extractStringLiteral(c: blackbox.Context)(
    query: c.Expr[String]
  ): String = {
    import c.universe._

    query.tree match {
      case Literal(Constant(sql: String)) =>
        c.echo(c.enclosingPosition, s"📝 Query: $sql")
        sql
      case other =>
        c.echo(c.enclosingPosition, s"❌ Not a literal: ${showRaw(other)}")
        c.abort(
          c.enclosingPosition,
          "❌ SQL query must be a string literal for compile-time validation. " +
          "Use the *Unchecked() variant for dynamic queries."
        )
    }
  }

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
  // ✅ Reject SELECT * (incompatible with compile-time validation)
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

    c.echo(c.enclosingPosition, "✅ No SELECT * detected")
  }

  private def extractQueryFields(parsedQuery: SQLSearchRequest): Set[String] = {
    parsedQuery.select.fields.map { field =>
      field.fieldAlias.map(_.alias).getOrElse(field.identifier.name)
    }.toSet
  }

  private def extractCaseClassFields(c: blackbox.Context)(
    tpe: c.universe.Type
  ): Map[String, c.universe.Type] = {
    import c.universe._

    tpe.members.collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        m.name.toString -> m.returnType
    }.toMap
  }

  // ============================================================
  // ✅ VALIDATION 1: Query fields must exist in case class
  // ============================================================
  @deprecated
  private def validateQueryFieldsExist(c: blackbox.Context)(
    queryFields: Set[String],
    caseClassFields: Map[String, c.universe.Type],
    tpe: c.universe.Type
  ): Unit = {
    val unknownFields = queryFields.filterNot(f => caseClassFields.contains(f))

    if (unknownFields.nonEmpty) {
      val availableFields = caseClassFields.keys.toSeq.sorted.mkString(", ")
      val suggestions = unknownFields.flatMap { unknown =>
        findClosestMatch(unknown, caseClassFields.keys.toSeq)
      }

      val suggestionMsg = if (suggestions.nonEmpty) {
        s"\nDid you mean: ${suggestions.mkString(", ")}?"
      } else ""

      c.abort(
        c.enclosingPosition,
        s"❌ SQL query selects fields not present in ${tpe.typeSymbol.name}: " +
        s"${unknownFields.mkString(", ")}\n" +
        s"Available fields: $availableFields$suggestionMsg"
      )
    }

    c.echo(c.enclosingPosition, "✅ All query fields exist in case class")
  }

  // ============================================================
  // ✅ VALIDATION 2: Missing fields must have defaults or be Option
  // ============================================================
  private def validateMissingFieldsHaveDefaults(c: blackbox.Context)(
    queryFields: Set[String],
    caseClassFields: Map[String, c.universe.Type],
    tpe: c.universe.Type
  ): Unit = {
    import c.universe._

    val missingFields = caseClassFields.keySet -- queryFields

    if (missingFields.isEmpty) {
      c.echo(c.enclosingPosition, "✅ No missing fields to validate")
      return
    }

    c.echo(c.enclosingPosition, s"⚠️  Missing fields: ${missingFields.mkString(", ")}")

    // Get constructor parameters with their positions
    val constructor = tpe.decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten

    // Build map: fieldName -> (index, hasDefault, isOption)
    val fieldInfo = params.zipWithIndex.map { case (param, idx) =>
      val fieldName = param.name.toString
      val fieldType = param.typeSignature

      // Check if Option
      val isOption = fieldType.typeConstructor =:= typeOf[Option[_]].typeConstructor

      // Check if has default value
      val companionSymbol = tpe.typeSymbol.companion
      val hasDefault = if (companionSymbol != NoSymbol) {
        val companionType = companionSymbol.typeSignature
        val defaultMethodName = s"apply$$default$$${idx + 1}"
        companionType.member(TermName(defaultMethodName)) != NoSymbol
      } else {
        false
      }

      (fieldName, (idx, hasDefault, isOption))
    }.toMap

    // Check each missing field
    val fieldsWithoutDefaults = missingFields.filterNot { fieldName =>
      fieldInfo.get(fieldName) match {
        case Some((_, hasDefault, isOption)) =>
          if (isOption) {
            c.echo(c.enclosingPosition, s"✅ Field '$fieldName' is Option - OK")
            true
          } else if (hasDefault) {
            c.echo(c.enclosingPosition, s"✅ Field '$fieldName' has default value - OK")
            true
          } else {
            c.echo(c.enclosingPosition, s"❌ Field '$fieldName' has NO default and is NOT Option")
            false
          }
        case None =>
          c.echo(c.enclosingPosition, s"⚠️  Field '$fieldName' not found in constructor")
          false
      }
    }

    if (fieldsWithoutDefaults.nonEmpty) {
      c.abort(
        c.enclosingPosition,
        s"❌ SQL query does not select the following required fields from ${tpe.typeSymbol.name}:\n" +
        s"  ${fieldsWithoutDefaults.mkString(", ")}\n\n" +
        s"These fields are missing from the query:\n" +
        s"  SELECT ${queryFields.mkString(", ")} FROM ...\n\n" +
        s"To fix this, either:\n" +
        s"  1. Add them to the SELECT clause\n" +
        s"  2. Make them Option[T] in the case class\n" +
        s"  3. Provide default values in the case class definition"
      )
    }

    c.echo(c.enclosingPosition, "✅ All missing fields have defaults or are Option")
  }

  // Helper: Get the index of a field in the case class constructor
  private def getFieldIndex(c: blackbox.Context)(
    tpe: c.universe.Type,
    fieldName: String
  ): Int = {
    import c.universe._

    val constructor = tpe.decl(termNames.CONSTRUCTOR).asMethod
    val params = constructor.paramLists.flatten

    params.indexWhere(_.name.toString == fieldName)
  }

  // ============================================================
  // VALIDATION 3: Type compatibility
  // ============================================================
  private def validateTypes(c: blackbox.Context)(
    parsedQuery: SQLSearchRequest,
    caseClassFields: Map[String, c.universe.Type]
  ): Unit = {

    parsedQuery.select.fields.foreach { field =>
      val fieldName = field.fieldAlias.map(_.alias).getOrElse(field.identifier.name)

      (field.out, caseClassFields.get(fieldName)) match {
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

    c.echo(c.enclosingPosition, "✅ Type validation passed")
  }

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
}
