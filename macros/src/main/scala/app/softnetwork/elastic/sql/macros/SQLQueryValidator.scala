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
import app.softnetwork.elastic.sql.function.aggregate.COUNT
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SQLSearchRequest

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

/** SQL Query Validator Trait
  *
  * Provides compile-time validation of SQL queries against Scala case classes. Ensures type safety
  * and prevents runtime deserialization errors.
  */
trait SQLQueryValidator {

  /** Validates an SQL query against a type T. Returns the SQL query if valid, otherwise aborts
    * compilation.
    * @note
    *   query fields that do not exist in case class will be ignored because we are using Jackson to
    *   deserialize the results with the following option
    *   DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES = false
    */
  protected def validateSQLQuery[T: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String]
  ): String = {

    debug(c)("🚀 MACRO IS BEING CALLED!")

    // ✅ Extract the SQL query (must be a literal)
    val sqlQuery = extractSQLString(c)(query)

    val tpe = c.weakTypeOf[T]

    // ✅ Check if already validated
    if (SQLQueryValidator.isCached(c)(tpe, sqlQuery)) {
      debug(c)(s"✅ Query already validated (cached): $sqlQuery")
      return sqlQuery
    }

    debug(c)(s"🔍 Validating SQL query $sqlQuery for type: ${tpe.typeSymbol.name}")

    // ✅ Parse the SQL query
    val parsedQuery = parseSQLQuery(c)(sqlQuery)

    // ✅ Reject SELECT *
    rejectSelectStar(c)(parsedQuery, sqlQuery)

    // ✅ Extract the selected fields from the query
    val queryFields = extractQueryFields(parsedQuery)

    debug(c)(s"🔍 Parsed fields: ${queryFields.mkString(", ")}")

    // ✅ Extract UNNEST information from the query
    val unnestedCollections = extractUnnestedCollections(parsedQuery)

    debug(c)(s"🔍 Unnested collections: ${unnestedCollections.mkString(", ")}")

    // ✅ Recursive validation of unknown fields
    val unknownFields = validateUnknownFieldsRecursively(c)(tpe, queryFields, prefix = "")

    // ✅ Recursive validation of required fields
    validateRequiredFieldsRecursively(c)(
      tpe,
      queryFields,
      unknownFields,
      unnestedCollections,
      prefix = ""
    )

    // ✅ Extract required fields from the case class
    val requiredFields = getRequiredFields(c)(tpe)
    debug(c)(s"📦 Case class fields: ${requiredFields.mkString(", ")}")

    // ✅ Type validation
    validateTypes(c)(parsedQuery, requiredFields.map(values => values._1 -> values._2._1))

    debug(c)("=" * 80)
    debug(c)("✅ SQL Query Validation Complete")
    debug(c)("=" * 80)

    // ✅ Mark as validated
    SQLQueryValidator.markValidated(c)(tpe, sqlQuery)

    // ✅ Return the validated request
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
          c.abort(c.enclosingPosition, "❌ Empty multi-search query")
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
  private def rejectSelectStar[T: c.WeakTypeTag](c: blackbox.Context)(
    parsedQuery: SQLSearchRequest,
    sqlQuery: String
  ): Unit = {
    import c.universe._

    // Check if any field is a wildcard (*)
    val hasWildcard = parsedQuery.select.fields.exists { field =>
      field.identifier.name == "*" && (field.aggregateFunction match {
        case Some(COUNT) =>
          false
        case _ =>
          true
      })
    }

    if (hasWildcard) {
      val tpe = weakTypeOf[T]
      val requiredFields = getRequiredFields(c)(tpe)
      val fieldNames = requiredFields.keys.mkString(", ")

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
           |  1. Explicitly list all required fields for ${tpe.typeSymbol.name}:
           |     SELECT $fieldNames FROM ...
           |
           |  2. Use the *Unchecked() variant for dynamic queries:
           |     searchAsUnchecked[${tpe.typeSymbol.name}](SQLQuery("SELECT * FROM ..."))
           |
           |Best Practice:
           |  Always explicitly select only the fields you need.
           |""".stripMargin
      )
    }

    debug(c)("✅ No SELECT * detected")
  }

  // ============================================================
  // Helper: Check if a type is a case class
  // ============================================================
  private def isCaseClassType(c: blackbox.Context)(tpe: c.universe.Type): Boolean = {
    tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass
  }

  // ============================================================
  // Helper: Check if a type is a Product (case class or tuple)
  // ============================================================
  private def isProductType(c: blackbox.Context)(tpe: c.universe.Type): Boolean = {
    import c.universe._

    // Check if it's a case class
    val isCaseClass = tpe.typeSymbol.isClass && tpe.typeSymbol.asClass.isCaseClass

    // Check if it's a Product type (includes tuples)
    val isProduct = tpe <:< typeOf[Product]

    isCaseClass || isProduct
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
  // Helper: Extract the required fields from a case class
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

      val isOption = paramType <:< typeOf[Option[_]]
      val hasDefault = param.asTerm.isParamWithDefault

      Some((paramName, (paramType, isOption, hasDefault)))
    }.toMap
  }

  // ============================================================
  // Helper: Extract selected fields from parsed SQL query
  // ============================================================
  private def extractQueryFields(parsedQuery: SQLSearchRequest): Set[String] = {
    parsedQuery.select.fields.map { field =>
      field.fieldAlias.map(_.alias).getOrElse(field.identifier.name)
    }.toSet
  }

  // ============================================================
  // Helper: Extract UNNEST collections from the query
  // ============================================================
  private def extractUnnestedCollections(parsedQuery: SQLSearchRequest): Set[String] = {
    // Check if the query has nested elements (UNNEST)
    parsedQuery.select.fields.flatMap { field =>
      field.identifier.nestedElement.map { nested =>
        // Extract the collection name from the nested element
        // Example: "children" from "UNNEST(parent.children) AS children"
        nested.innerHitsName
      }
    }.toSet
  }

  // ============================================================
  // Helper: Recursive Validation of Required Fields
  // ============================================================
  private def validateRequiredFieldsRecursively(
    c: blackbox.Context
  )(
    tpe: c.universe.Type,
    queryFields: Set[String],
    unknownFields: Set[String],
    unnestedCollections: Set[String],
    prefix: String
  ): Unit = {

    val requiredFields = getRequiredFields(c)(tpe)
    debug(c)(
      s"📋 Required fields for ${tpe.typeSymbol.name} (prefix='$prefix'): ${requiredFields.keys.mkString(", ")}"
    )

    requiredFields.foreach { case (fieldName, (fieldType, isOption, hasDefault)) =>
      val fullFieldName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"
      debug(c)(
        s"🔍 Checking field: $fullFieldName (type: $fieldType, optional: $isOption, hasDefault: $hasDefault)"
      )

      // ✅ Check if the field is directly selected (e.g., "address")
      val isDirectlySelected = queryFields.contains(fullFieldName)

      // ✅ Check if nested fields of this field are selected (e.g., "address.street")
      val hasNestedSelection = queryFields.exists(_.startsWith(s"$fullFieldName."))

      // ✅ Determine field characteristics
      val isCollection = isCollectionType(c)(fieldType)
      val isNestedObject = isProductType(c)(fieldType)
      val isNestedCollection = isCollection && {
        getCollectionElementType(c)(fieldType).exists(isProductType(c))
      }

      if (isDirectlySelected) {
        // ✅ Field is selected as a whole (e.g., SELECT address FROM ...)
        debug(c)(s"✅ Field '$fullFieldName' is directly selected")
      } else if (isOption || hasDefault) {
        // ✅ Field is optional or has a default value, can be omitted
        debug(c)(s"✅ Field '$fullFieldName' is optional or has default - OK")
      } else if (hasNestedSelection) {
        // ⚠️ Nested fields are selected (e.g., SELECT address.street FROM ...)

        if (isNestedCollection) {
          // ✅ Collection of case classes
          debug(c)(s"📦 Field '$fullFieldName' is a nested collection")
          validateNestedCollection(c)(
            fullFieldName,
            fieldName,
            fieldType,
            queryFields,
            unknownFields,
            unnestedCollections,
            tpe
          )
        } else if (isNestedObject) {
          // ❌ Nested object (non-collection) with individual field selection
          debug(c)(s"🏗️ Field '$fullFieldName' is a nested object (non-collection)")

          val isUnnested = unnestedCollections.contains(fullFieldName) ||
            unnestedCollections.contains(fieldName)

          if (!isUnnested) {
            c.abort(
              c.enclosingPosition,
              s"""❌ Nested object field '$fullFieldName' cannot be deserialized correctly.
                 |
                 |❌ Problem:
                 |   You are selecting nested fields individually:
                 |   ${queryFields.filter(_.startsWith(s"$fullFieldName.")).mkString(", ")}
                 |
                 |   Elasticsearch will return flat fields like:
                 |   { "$fullFieldName.field1": "value1", "$fullFieldName.field2": "value2" }
                 |
                 |   But Jackson needs a structured object like:
                 |   { "$fullFieldName": {"field1": "value1", "field2": "value2"} }
                 |
                 |✅ Solution 1: Select the entire nested object (recommended)
                 |   SELECT $fullFieldName FROM ...
                 |
                 |✅ Solution 2: Use UNNEST (if you need to filter or join on nested fields)
                 |   SELECT ${queryFields.filter(_.startsWith(s"$fullFieldName.")).mkString(", ")}
                 |   FROM ...
                 |   JOIN UNNEST(....$fullFieldName) AS $fieldName
                 |
                 |📚 Note: This applies to ALL nested objects, not just collections.
                 |""".stripMargin
            )
          }

          // ✅ With UNNEST: validate nested fields recursively
          validateRequiredFieldsRecursively(c)(
            fieldType,
            queryFields,
            unknownFields,
            unnestedCollections,
            fullFieldName
          )
        } else {
          // ✅ Primitive type with nested selection (shouldn't happen)
          debug(c)(s"⚠️ Unexpected nested selection for primitive field: $fullFieldName")
        }
      } else {
        // ❌ Required field is not selected at all
        debug(c)(s"❌ Field '$fullFieldName' is missing")

        val exampleFields = ((queryFields -- unknownFields) + fullFieldName).mkString(", ")
        val suggestions = findClosestMatch(fieldName, unknownFields.map(_.split("\\.").last).toSeq)
        val suggestionMsg = suggestions
          .map(s => s"""\nYou have selected unknown field "$s", did you mean "$fullFieldName"?""")
          .getOrElse("")

        c.abort(
          c.enclosingPosition,
          s"""❌ SQL query does not select the required field: $fullFieldName
             |$suggestionMsg
             |
             |Example query:
             |SELECT $exampleFields FROM ...
             |
             |To fix this, either:
             |  1. Add it to the SELECT clause
             |  2. Make it Option[T] in the case class
             |  3. Provide a default value in the case class definition
             |""".stripMargin
        )
      }
    }
  }

  // ============================================================
  // Helper: Validate Nested Collection Fields
  // ============================================================
  private def validateNestedCollection(
    c: blackbox.Context
  )(
    fullFieldName: String,
    fieldName: String,
    fieldType: c.universe.Type,
    queryFields: Set[String],
    unknownFields: Set[String],
    unnestedCollections: Set[String],
    parentType: c.universe.Type
  ): Unit = {

    // ✅ Check if the collection is unnested (uses UNNEST)
    val isUnnested = unnestedCollections.contains(fullFieldName) ||
      unnestedCollections.contains(fieldName)

    if (!isUnnested) {
      // ❌ Collection with nested field selection but NO UNNEST
      val selectedNestedFields = queryFields.filter(_.startsWith(s"$fullFieldName."))

      getCollectionElementType(c)(fieldType) match {
        case Some(elementType) =>
          c.abort(
            c.enclosingPosition,
            s"""❌ Collection field '$fullFieldName' cannot be deserialized correctly.
               |
               |❌ Problem:
               |   You are selecting nested fields without using UNNEST:
               |   ${selectedNestedFields.mkString(", ")}
               |
               |   Elasticsearch will return flat arrays like:
               |   { "$fullFieldName.field1": ["val1", "val2"], "$fullFieldName.field2": ["val3", "val4"] }
               |
               |   But Jackson needs structured objects like:
               |   { "$fullFieldName": [{"field1": "val1", "field2": "val3"}, {"field1": "val2", "field2": "val4"}] }
               |
               |✅ Solution 1: Select the entire collection (recommended for simple queries)
               |   SELECT $fullFieldName FROM ...
               |
               |✅ Solution 2: Use UNNEST for precise field selection (recommended for complex queries)
               |   SELECT ${selectedNestedFields.mkString(", ")}
               |   FROM ...
               |   JOIN UNNEST(....$fullFieldName) AS $fieldName
               |
               |📚 Documentation:
               |   https://www.elastic.co/guide/en/elasticsearch/reference/current/nested.html
               |""".stripMargin
          )
        case None =>
          debug(c)(s"⚠️ Cannot extract element type from collection: $fullFieldName")
      }
    } else {
      // ✅ Collection is unnested: validate nested fields recursively
      debug(c)(s"✅ Collection field '$fullFieldName' is unnested")

      getCollectionElementType(c)(fieldType).foreach { elementType =>
        if (isProductType(c)(elementType)) {
          validateRequiredFieldsRecursively(c)(
            elementType,
            queryFields,
            unknownFields,
            unnestedCollections,
            fullFieldName
          )
        }
      }
    }
  }

  // ============================================================
  // Helper: Build all valid field paths recursively
  // ============================================================
  private def buildValidFieldPaths(
    c: blackbox.Context
  )(
    tpe: c.universe.Type,
    prefix: String
  ): Set[String] = {

    val requiredFields = getRequiredFields(c)(tpe)

    requiredFields.flatMap { case (fieldName, (fieldType, _, _)) =>
      val fullFieldName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"

      if (isCollectionType(c)(fieldType)) {
        getCollectionElementType(c)(fieldType) match {
          case Some(elementType) if isCaseClassType(c)(elementType) =>
            // ✅ Collection of case classes: recurse
            Set(fullFieldName) ++ buildValidFieldPaths(c)(elementType, fullFieldName)
          case _ =>
            Set(fullFieldName)
        }
      } else if (isCaseClassType(c)(fieldType)) {
        // ✅ Nested case class: recurse
        Set(fullFieldName) ++ buildValidFieldPaths(c)(fieldType, fullFieldName)
      } else {
        Set(fullFieldName)
      }
    }.toSet
  }

  // ============================================================
  // Helper: Validate Unknown Fields Recursively
  // ============================================================
  private def validateUnknownFieldsRecursively(
    c: blackbox.Context
  )(
    tpe: c.universe.Type,
    queryFields: Set[String],
    prefix: String
  ): Set[String] = {

    // ✅ Get all valid field paths at this level and below
    val validFieldPaths = buildValidFieldPaths(c)(tpe, prefix)

    // ✅ Find unknown fields
    val unknownFields = queryFields.filterNot { queryField =>
      validFieldPaths.contains(queryField) ||
      validFieldPaths.exists(vf => queryField.startsWith(s"$vf."))
    }

    if (unknownFields.nonEmpty) {
      val unknownFieldNames = unknownFields.mkString(", ")
      val availableFields = validFieldPaths.toSeq.sorted.mkString(", ")

      c.warning(
        c.enclosingPosition,
        s"""⚠️ SQL query selects fields that don't exist in ${tpe.typeSymbol.name}:
           |$unknownFieldNames
           |
           |Available fields: $availableFields
           |
           |Note: These fields will be ignored during deserialization.
           |""".stripMargin
      )
    }

    unknownFields
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

    val underlyingType = if (scalaType <:< typeOf[Option[_]]) {
      scalaType.typeArgs.headOption.getOrElse(scalaType)
    } else {
      scalaType
    }

    sqlType match {
      case SQLTypes.TinyInt =>
        underlyingType =:= typeOf[Byte] ||
          underlyingType =:= typeOf[Short] ||
          underlyingType =:= typeOf[Int] ||
          underlyingType =:= typeOf[Long]

      case SQLTypes.SmallInt =>
        underlyingType =:= typeOf[Short] ||
          underlyingType =:= typeOf[Int] ||
          underlyingType =:= typeOf[Long]

      case SQLTypes.Int =>
        underlyingType =:= typeOf[Int] ||
          underlyingType =:= typeOf[Long]

      case SQLTypes.BigInt =>
        underlyingType =:= typeOf[Long] ||
          underlyingType =:= typeOf[BigInt]

      case SQLTypes.Double | SQLTypes.Real =>
        underlyingType =:= typeOf[Double] ||
          underlyingType =:= typeOf[Float]

      case SQLTypes.Char =>
        underlyingType =:= typeOf[String] || // CHAR(n) → String
          underlyingType =:= typeOf[Char] // CHAR(1) → Char

      case SQLTypes.Varchar =>
        underlyingType =:= typeOf[String]

      case SQLTypes.Boolean =>
        underlyingType =:= typeOf[Boolean]

      case SQLTypes.Time =>
        underlyingType.toString.contains("Instant") ||
          underlyingType.toString.contains("LocalTime")

      case SQLTypes.Date =>
        underlyingType.toString.contains("Date") ||
          underlyingType.toString.contains("Instant") ||
          underlyingType.toString.contains("LocalDate")

      case SQLTypes.DateTime | SQLTypes.Timestamp =>
        underlyingType.toString.contains("LocalDateTime") ||
          underlyingType.toString.contains("ZonedDateTime") ||
          underlyingType.toString.contains("Instant")

      case SQLTypes.Struct =>
        if (underlyingType.typeSymbol.isClass && underlyingType.typeSymbol.asClass.isCaseClass) {
          // TODO validateStructFields(c)(sqlField, underlyingType)
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

  // ✅ Cache to avoid redundant validations
  private val validationCache = scala.collection.mutable.Map[String, Boolean]()

  private[macros] def isCached(
    c: blackbox.Context
  )(tpe: c.universe.Type, sql: String): Boolean = {
    // ✅ Disable cache in test mode
    if (sys.props.get("sql.macro.test").contains("true")) {
      false
    } else {
      validationCache.contains(s"${tpe.typeSymbol.name}::${sql.trim}")
    }
  }

  private[macros] def markValidated(
    c: blackbox.Context
  )(tpe: c.universe.Type, sql: String): Unit = {
    if (!sys.props.get("sql.macro.test").contains("true")) {
      validationCache(s"${tpe.typeSymbol.name}::${sql.trim}") = true
    }
  }

  // ✅ Method for clearing the cache
  private[macros] def clearCache(): Unit = validationCache.clear()
}
