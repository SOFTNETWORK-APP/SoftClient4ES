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

    // ✅ Extract the SQL query (must be a literal)
    val sqlQuery = extractSQLString(c)(query)

    val tpe = c.weakTypeOf[T]

    // ✅ Check if already validated
    if (SQLQueryValidator.isCached(c)(tpe, sqlQuery)) {
      debug(c)(s"✅ Query already validated (cached): $sqlQuery")
      return sqlQuery
    }

    if (sys.props.get("elastic.sql.debug").contains("true")) {
      c.info(c.enclosingPosition, s"Validating SQL: $sqlQuery", force = false)
    }

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

    // ✅ Recursive validation of required fields
    validateRequiredFieldsRecursive(c)(tpe, queryFields, unnestedCollections, prefix = "")

    // ✅ Recursive validation of unknown fields
    validateUnknownFieldsRecursive(c)(tpe, queryFields, prefix = "")

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
  private def rejectSelectStar[T: c.WeakTypeTag](c: blackbox.Context)(
    parsedQuery: SQLSearchRequest,
    sqlQuery: String
  ): Unit = {
    import c.universe._

    // Check if any field is a wildcard (*)
    val hasWildcard = parsedQuery.select.fields.exists { field =>
      field.identifier.name == "*"
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
  private def validateRequiredFieldsRecursive(
    c: blackbox.Context
  )(
    tpe: c.universe.Type,
    queryFields: Set[String],
    unnestedCollections: Set[String],
    prefix: String
  ): Unit = {

    val requiredFields = getRequiredFields(c)(tpe)

    val missingFields = requiredFields.filterNot {
      case (fieldName, (fieldType, isOption, hasDefault)) =>
        val fullFieldName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"

        // ✅ Check if the field is directly selected (e.g., "address")
        val isDirectlySelected = queryFields.contains(fullFieldName)

        // ✅ Check if nested fields of this field are selected (e.g., "address.street")
        val hasNestedSelection = queryFields.exists(_.startsWith(s"$fullFieldName."))

        if (isDirectlySelected) {
          // ✅ Field is selected as a whole (e.g., SELECT address FROM ...)
          debug(c)(s"✅ Field '$fullFieldName' is directly selected")
          true
        } else if (isOption) {
          // ✅ Field is optional, can be omitted
          debug(c)(s"✅ Field '$fullFieldName' is Option - OK")
          true
        } else if (hasDefault) {
          // ✅ Field has a default value, can be omitted
          debug(c)(s"✅ Field '$fullFieldName' has default value - OK")
          true
        } else if (hasNestedSelection) {
          // ⚠️ Nested fields are selected (e.g., SELECT address.street FROM ...)
          // We must validate that ALL required nested fields are present

          if (isCollectionType(c)(fieldType)) {
            // ✅ Collection: check if it's unnested
            validateCollectionFieldsRecursive(c)(
              fieldName,
              fieldType,
              queryFields,
              unnestedCollections,
              prefix
            )
          } else if (isCaseClassType(c)(fieldType)) {
            // ✅ Nested case class: validate that ALL required nested fields are selected
            debug(c)(s"🔍 Validating nested case class fields: $fullFieldName")

            try {
              validateRequiredFieldsRecursive(c)(
                fieldType,
                queryFields,
                unnestedCollections,
                prefix = fullFieldName
              )
              // ✅ All required nested fields are present
              debug(c)(s"✅ All required nested fields of '$fullFieldName' are present")
              true
            } catch {
              case _: Throwable =>
                // ❌ Some required nested fields are missing
                val nestedFields = getRequiredFields(c)(fieldType)
                val missingNestedFields = nestedFields.filterNot {
                  case (nestedFieldName, (nestedFieldType, nestedIsOption, nestedHasDefault)) =>
                    val fullNestedFieldName = s"$fullFieldName.$nestedFieldName"
                    val isNestedSelected = queryFields.contains(fullNestedFieldName)
                    val hasNestedNestedSelection =
                      queryFields.exists(_.startsWith(s"$fullNestedFieldName."))

                    isNestedSelected || nestedIsOption || nestedHasDefault ||
                    (hasNestedNestedSelection && isCaseClassType(c)(nestedFieldType))
                }

                if (missingNestedFields.nonEmpty) {
                  val missingNames =
                    missingNestedFields.keys.map(n => s"$fullFieldName.$n").mkString(", ")
                  val allRequiredFields = nestedFields
                    .filterNot { case (_, (_, isOpt, hasDef)) => isOpt || hasDef }
                    .keys
                    .map(n => s"$fullFieldName.$n")
                    .mkString(", ")

                  c.abort(
                    c.enclosingPosition,
                    s"""❌ Nested case class field '$fullFieldName' has missing required fields:
                       |$missingNames
                       |
                       |When selecting nested fields individually, ALL required fields must be present.
                       |
                       |Option 1: Select the entire nested object:
                       |  SELECT $fullFieldName FROM ...
                       |
                       |Option 2: Select ALL required nested fields:
                       |  SELECT $allRequiredFields FROM ...
                       |
                       |Option 3: Make missing fields optional or provide default values in the case class
                       |""".stripMargin
                  )
                }

                false
            }
          } else {
            // ✅ Primitive type with nested selection (shouldn't happen)
            debug(c)(s"⚠️ Unexpected nested selection for primitive field: $fullFieldName")
            false
          }
        } else {
          // ❌ Field is not selected at all
          debug(c)(s"❌ Field '$fullFieldName' is missing")
          false
        }
    }

    if (missingFields.nonEmpty) {
      val missingFieldNames = missingFields.keys
        .map { fieldName =>
          if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"
        }
        .mkString(", ")

      val exampleFields = (queryFields ++ missingFields.keys.map { fieldName =>
        if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"
      }).mkString(", ")

      val suggestions = missingFields.keys.flatMap { fieldName =>
        findClosestMatch(fieldName, queryFields.map(_.split("\\.").last).toSeq)
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
           |SELECT $exampleFields FROM ...
           |
           |To fix this, either:
           |  1. Add them to the SELECT clause
           |  2. Make them Option[T] in the case class
           |  3. Provide default values in the case class definition""".stripMargin
      )
    }
  }

  // ============================================================
  // Helper: Validate Collection Fields Recursively
  // ============================================================
  private def validateCollectionFieldsRecursive(
    c: blackbox.Context
  )(
    fieldName: String,
    fieldType: c.universe.Type,
    queryFields: Set[String],
    unnestedCollections: Set[String],
    prefix: String
  ): Boolean = {

    getCollectionElementType(c)(fieldType) match {
      case Some(elementType) =>
        val fullFieldName = if (prefix.isEmpty) fieldName else s"$prefix.$fieldName"

        // ✅ Check if the collection is selected as a whole
        val isDirectlySelected = queryFields.contains(fullFieldName)

        // ✅ Check if the collection is unnested (uses UNNEST)
        val isUnnested = unnestedCollections.contains(fieldName)

        if (isDirectlySelected) {
          debug(c)(s"✅ Collection field '$fullFieldName' is directly selected")
          true
        } else if (isUnnested) {
          debug(c)(s"✅ Collection field '$fullFieldName' is unnested")
          // ✅ For unnested collections, validate nested fields
          if (isCaseClassType(c)(elementType)) {
            val nestedFields = getRequiredFields(c)(elementType)

            val missingNestedFields = nestedFields.filterNot {
              case (nestedFieldName, (nestedFieldType, isOption, hasDefault)) =>
                val fullNestedFieldName = s"$fullFieldName.$nestedFieldName"

                val isSelected = queryFields.contains(fullNestedFieldName)
                val hasNestedSelection = queryFields.exists(_.startsWith(s"$fullNestedFieldName."))

                if (isSelected) {
                  debug(c)(s"✅ Nested field '$fullNestedFieldName' is selected")
                  true
                } else if (isOption) {
                  debug(c)(s"✅ Nested field '$fullNestedFieldName' is Option - OK")
                  true
                } else if (hasDefault) {
                  debug(c)(s"✅ Nested field '$fullNestedFieldName' has default value - OK")
                  true
                } else if (hasNestedSelection && isCaseClassType(c)(nestedFieldType)) {
                  debug(c)(s"🔍 Validating deeply nested case class: $fullNestedFieldName")
                  try {
                    validateRequiredFieldsRecursive(c)(
                      nestedFieldType,
                      queryFields,
                      unnestedCollections,
                      prefix = fullNestedFieldName
                    )
                    true
                  } catch {
                    case _: Throwable => false
                  }
                } else {
                  debug(c)(s"❌ Nested field '$fullNestedFieldName' is missing")
                  false
                }
            }

            if (missingNestedFields.nonEmpty) {
              val missingNames =
                missingNestedFields.keys.map(n => s"$fullFieldName.$n").mkString(", ")
              val allRequiredFields = nestedFields
                .filterNot { case (_, (_, isOpt, hasDef)) => isOpt || hasDef }
                .keys
                .map(n => s"$fullFieldName.$n")
                .mkString(", ")

              c.abort(
                c.enclosingPosition,
                s"""❌ Unnested collection field '$fullFieldName' is missing required nested fields:
                   |$missingNames
                   |
                   |When using UNNEST, ALL required nested fields must be selected:
                   |  SELECT $allRequiredFields FROM parent JOIN UNNEST(parent.$fieldName) AS $fieldName
                   |
                   |Or make missing nested fields optional or provide default values
                   |""".stripMargin
              )
            }

            true
          } else {
            true
          }
        } else if (isCaseClassType(c)(elementType)) {
          // ❌ Collection of case classes with nested field selection but NO UNNEST
          val hasNestedSelection = queryFields.exists(_.startsWith(s"$fullFieldName."))

          if (hasNestedSelection) {
            c.abort(
              c.enclosingPosition,
              s"""❌ Collection field '$fullFieldName' cannot be deserialized correctly.
                 |
                 |You are selecting nested fields of a collection without using UNNEST:
                 |  ${queryFields.filter(_.startsWith(s"$fullFieldName.")).mkString(", ")}
                 |
                 |This will result in flat arrays that cannot be reconstructed into objects.
                 |
                 |Example of the problem:
                 |  Elasticsearch returns: { "children.name": ["Alice", "Bob"], "children.age": [10, 12] }
                 |  But we need: { "children": [{"name": "Alice", "age": 10}, {"name": "Bob", "age": 12}] }
                 |
                 |Solution 1: Select the entire collection:
                 |  SELECT $fullFieldName FROM ...
                 |
                 |Solution 2: Use UNNEST to properly handle nested objects:
                 |  SELECT name, $fieldName.name, $fieldName.age
                 |  FROM ${if (prefix.isEmpty) "table" else prefix}
                 |  JOIN UNNEST(${if (prefix.isEmpty) "" else s"$prefix."}$fieldName) AS $fieldName
                 |
                 |Solution 3: Make the collection optional:
                 |  $fullFieldName: Option[List[${elementType.typeSymbol.name}]] = None
                 |""".stripMargin
            )
          }

          false
        } else {
          // ✅ Collection of primitive types
          true
        }

      case None =>
        debug(c)(s"⚠️ Cannot extract element type from collection: $fieldName")
        false
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
  private def validateUnknownFieldsRecursive(
    c: blackbox.Context
  )(
    tpe: c.universe.Type,
    queryFields: Set[String],
    prefix: String
  ): Unit = {

    val requiredFields = getRequiredFields(c)(tpe)

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
