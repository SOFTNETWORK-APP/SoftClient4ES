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

package app.softnetwork.elastic.client.help

import org.json4s._
import org.json4s.native.JsonMethods._
import scala.io.Source
import scala.util.Try

/** Loader for JSON help files
  */
object HelpJsonLoader {
  implicit val formats: Formats = DefaultFormats

  private val basePath = "help"

  /** Load all help entries from JSON files
    */
  def loadAll(): HelpDatabase = {
    val commands = loadCommands()
    val functions = loadFunctions()

    HelpDatabase(
      commands = commands,
      functions = functions,
      aliases = buildAliases(commands, functions)
    )
  }

  /** Load SQL commands from JSON files
    */
  private def loadCommands(): Map[String, SqlCommandHelp] = {
    val categories = List("ddl", "dml", "dql")

    categories.flatMap { category =>
      loadJsonFilesFromDirectory(s"$basePath/commands/$category").flatMap { json =>
        Try {
          val cmd = parse(json).extract[SqlCommandJson]
          cmd.name.toUpperCase -> convertToCommandHelp(cmd)
        }.toOption
      }
    }.toMap
  }

  /** Load functions from JSON files
    */
  private def loadFunctions(): Map[String, FunctionHelp] = {
    val categories = List(
      "aggregate",
      "conditional",
      "conversion",
      "date",
      "geo",
      "numeric",
      "string"
    )

    categories.flatMap { category =>
      loadJsonFilesFromDirectory(s"$basePath/functions/$category").flatMap { json =>
        Try {
          val fn = parse(json).extract[FunctionJson]
          fn.name.toUpperCase -> convertToFunctionHelp(fn)
        }.toOption
      }
    }.toMap
  }

  /** Load all JSON files from a resource directory
    */
  private def loadJsonFilesFromDirectory(path: String): Seq[String] = {
    Try {
      val resourceUrl = getClass.getClassLoader.getResource(path)
      if (resourceUrl == null) {
        // Fallback: try to list files in the directory
        val dir = new java.io.File(getClass.getClassLoader.getResource(path).toURI)
        if (dir.exists() && dir.isDirectory) {
          dir
            .listFiles()
            .filter(_.getName.endsWith(".json"))
            .map { file =>
              Source.fromFile(file, "UTF-8").mkString
            }
            .toSeq
        } else {
          Seq.empty
        }
      } else {
        // Load from classpath resources
        loadResourceDirectory(path)
      }
    }.getOrElse(Seq.empty)
  }

  /** Load resources from classpath directory
    */
  private def loadResourceDirectory(path: String): Seq[String] = {
    // List of known files (must be maintained or use reflection/scanning)
    val knownFiles = getKnownFiles(path)

    knownFiles.flatMap { fileName =>
      Try {
        val resourcePath = s"$path/$fileName"
        val stream = getClass.getClassLoader.getResourceAsStream(resourcePath)
        if (stream != null) {
          val content = Source.fromInputStream(stream, "UTF-8").mkString
          stream.close()
          Some(content)
        } else {
          None
        }
      }.toOption.flatten
    }
  }

  /** Get known files for a path (for resource loading) This could be generated at build time or
    * loaded from an index file
    */
  private def getKnownFiles(path: String): Seq[String] = {
    // Load from index file if exists
    val indexPath = s"$path/_index.json"
    Try {
      val stream = getClass.getClassLoader.getResourceAsStream(indexPath)
      if (stream != null) {
        val content = Source.fromInputStream(stream, "UTF-8").mkString
        stream.close()
        parse(content).extract[List[String]]
      } else {
        List.empty
      }
    }.getOrElse(List.empty)
  }

  /** Convert JSON model to internal help model
    */
  private def convertToCommandHelp(cmd: SqlCommandJson): SqlCommandHelp = {
    SqlCommandHelp(
      name = cmd.name,
      category = parseCategory(cmd.category),
      shortDescription = cmd.shortDescription,
      syntax = cmd.syntax.mkString("\n"),
      description = cmd.description,
      clauses = cmd.clauses.getOrElse(List.empty).map { c =>
        ClauseHelp(c.name, c.description, c.optional)
      },
      examples = cmd.examples.map { e =>
        HelpExample(e.title, e.sql, e.output)
      },
      seeAlso = cmd.seeAlso,
      notes = cmd.notes,
      limitations = cmd.limitations
    )
  }

  private def convertToFunctionHelp(fn: FunctionJson): FunctionHelp = {
    FunctionHelp(
      name = fn.name,
      category = HelpCategory.Functions,
      shortDescription = fn.shortDescription,
      syntax = fn.syntax.mkString("\n"),
      description = fn.description,
      parameters = fn.parameters.map { p =>
        ParameterHelp(p.name, p.`type`, p.description, p.optional, p.defaultValue)
      },
      returnType = fn.returnType,
      examples = fn.examples.map { e =>
        HelpExample(e.title, e.sql, e.output)
      },
      seeAlso = fn.seeAlso,
      notes = fn.notes
    )
  }

  private def parseCategory(cat: String): HelpCategory = {
    cat.toUpperCase match {
      case "DDL" => HelpCategory.DDL
      case "DML" => HelpCategory.DML
      case "DQL" => HelpCategory.DQL
      case _     => HelpCategory.Functions
    }
  }

  private def buildAliases(
    commands: Map[String, SqlCommandHelp],
    functions: Map[String, FunctionHelp]
  ): Map[String, String] = {
    val cmdAliases = commands.values.flatMap { cmd =>
      // Extract aliases from the original JSON if needed
      Seq.empty[(String, String)]
    }

    val fnAliases = functions.values.flatMap { fn =>
      // Extract aliases from the original JSON if needed
      Seq.empty[(String, String)]
    }

    (cmdAliases ++ fnAliases).toMap
  }
}
