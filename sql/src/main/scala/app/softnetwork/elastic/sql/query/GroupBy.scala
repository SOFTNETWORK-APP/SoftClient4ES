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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.{
  Expr,
  Identifier,
  LongValue,
  PainlessContext,
  PainlessScript,
  TokenRegex,
  Updateable
}

case object GroupBy extends Expr("GROUP BY") with TokenRegex

case class GroupBy(buckets: Seq[Bucket]) extends Updateable {
  override def sql: String = s" $GroupBy ${buckets.mkString(", ")}"
  def update(request: SingleSearch): GroupBy =
    this.copy(buckets = buckets.map(_.update(request)))
  lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
    b.identifier.identifierName -> b
  }.toMap

  override def validate(): Either[String, Unit] = {
    if (buckets.isEmpty) {
      Left("At least one bucket is required in GROUP BY clause")
    } else {
      Right(())
    }
  }

  def nestedElements: Seq[NestedElement] =
    buckets.flatMap(_.nestedElement).distinct
}

case class Bucket(
  identifier: Identifier,
  size: Option[Int] = None
) extends Updateable
    with PainlessScript {
  def tableAlias: Option[String] = identifier.tableAlias
  def table: Option[String] = identifier.table
  override def sql: String = s"$identifier"
  def update(request: SingleSearch): Bucket = {
    identifier.functions.headOption match {
      case Some(func: LongValue) =>
        if (func.value <= 0) {
          throw new IllegalArgumentException(s"Bucket index must be greater than 0: ${func.value}")
        } else if (request.select.fields.size < func.value) {
          throw new IllegalArgumentException(
            s"Bucket index ${func.value} is out of bounds [1, ${request.fields.size}]"
          )
        } else {
          val field = request.select.fields(func.value.toInt - 1)
          this.copy(identifier = field.identifier, size = request.limit.map(_.limit))
        }
      case _ =>
        this.copy(identifier = identifier.update(request), size = request.limit.map(_.limit))
    }
  }

  lazy val sourceBucket: String =
    if (identifier.nested) {
      tableAlias
        .map(a => s"$a.")
        .getOrElse("") + identifier.name.split("\\.").tail.mkString(".")
    } else {
      identifier.name
    }
  lazy val nested: Boolean = nestedElement.isDefined
  lazy val nestedElement: Option[NestedElement] = identifier.nestedElement
  lazy val nestedBucket: Option[String] =
    identifier.nestedElement.map(_.innerHitsName)

  lazy val name: String = identifier.fieldAlias.getOrElse(path)

  lazy val path: String = sourceBucket.replace(".", "_")

  lazy val nestedPath: String = {
    identifier.nestedElement match {
      case Some(ne) => ne.nestedPath
      case None     => "" // Root level
    }
  }

  override def out: SQLType = identifier.out

  override def shouldBeScripted: Boolean = identifier.shouldBeScripted

  override def hasAggregation: Boolean = identifier.hasAggregation

  def isBucketScript: Boolean = !identifier.isAggregation && hasAggregation

  /** Generate painless script for this token
    *
    * @param context
    *   the painless context
    * @return
    *   the painless script
    */
  override def painless(context: Option[PainlessContext]): String =
    identifier.painless(context)
}

case class BucketPath(buckets: Seq[Bucket]) {

  lazy val path: String =
    buckets.foldLeft("")((acc, b) => {
      if (acc.isEmpty) {
        s"${b.path}"
      } else {
        s"$acc>${b.path}"
      }
    })

  override def toString: String = path
}

object MetricSelectorScript {

  def metricSelector(expr: Criteria): String = expr match {
    case Predicate(left, op, right, maybeNot, group) =>
      val leftStr = metricSelector(left)
      val rightStr = metricSelector(right)

      // Filtering all "1 == 1"
      if (leftStr == "1 == 1" && rightStr == "1 == 1") {
        "1 == 1"
      } else if (leftStr == "1 == 1") {
        rightStr
      } else if (rightStr == "1 == 1") {
        leftStr
      } else {
        val opStr = op match {
          case AND | OR => op.painless(None)
          case _        => throw new IllegalArgumentException(s"Unsupported logical operator: $op")
        }
        val not = maybeNot.nonEmpty
        if (group || not)
          s"${maybeNot.map(_ => "!").getOrElse("")}($leftStr) $opStr ($rightStr)"
        else
          s"$leftStr $opStr $rightStr"
      }

    case relation: ElasticRelation => metricSelector(relation.criteria)

    case _: MultiMatchCriteria => "1 == 1"

    case e: Expression if e.isAggregation =>
      // NO FILTERING: the script is generated for all metrics
      val painless = e.painless(None)
      e.maybeValue match {
        case Some(value) if e.operator.isInstanceOf[ComparisonOperator] =>
          value.out match {
            case SQLTypes.Date =>
              s"$painless.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli()"
            case SQLTypes.Time if e.operator.isInstanceOf[ComparisonOperator] =>
              s"$painless.truncatedTo(ChronoUnit.SECONDS).toInstant().toEpochMilli()"
            case SQLTypes.DateTime if e.operator.isInstanceOf[ComparisonOperator] =>
              s"$painless.toInstant().toEpochMilli()"
            case _ => painless
          }
        case _ => painless
      }
    case _ => "1 == 1"
  }
}

case class BucketIncludesExcludes(values: Set[String] = Set.empty, regex: Option[String] = None)

/** Tree structure representing buckets and their hierarchy */
case class BucketNode(
  bucket: Bucket,
  children: Seq[BucketNode] = Seq.empty,
  private val parent: Option[BucketNode] =
    None // to track parent node in order to build bucket path
) {
  def identifier: String = bucket.path

  def findNode(id: String): Option[BucketNode] = {
    if (this.identifier == id) Some(this)
    else children.flatMap(_.findNode(id)).headOption
  }

  def depth: Int = 1 + (if (children.isEmpty) 0 else children.map(_.depth).max)

  def level: Int = {
    parent match {
      case Some(p) => 1 + p.level
      case None    => 0
    }
  }

  // Check if the node is a leaf
  def isLeaf: Boolean = children.isEmpty

  def bucketPath: String = {
    parent match {
      case Some(p) => s"${p.bucketPath}>$identifier"
      case None    => identifier
    }
  }

  def parentBucketPath: Option[String] = {
    parent.map(_.bucketPath)
  }

  def root: BucketNode = {
    parent match {
      case Some(p) => p.root
      case None    => this
    }
  }
}

case class BucketTree(
  roots: Seq[BucketNode] = Seq.empty
) {

  private def filterNode(predicate: BucketNode => Boolean, node: BucketNode): Seq[BucketNode] = {
    if (predicate(node)) {
      node +: node.children.flatMap(child => filterNode(predicate, child))
    } else {
      node.children.flatMap(child => filterNode(predicate, child))
    }
  }

  /** Filter the bucket trees based on a predicate
    * @param predicate
    *   the predicate to filter the buckets
    * @return
    *   trees of buckets that satisfy the predicate
    */
  def filter(predicate: BucketNode => Boolean): Seq[Seq[BucketNode]] = {
    roots.map(root => filterNode(predicate, root))
  }

  /** Filter the bucket trees based on a negated predicate
    * @param predicate
    *   the predicate to filter the buckets
    * @return
    *   trees of buckets that do not satisfy the predicate
    */
  def filterNot(predicate: BucketNode => Boolean): Seq[Seq[BucketNode]] = {
    roots.map(root => filterNode(node => !predicate(node), root))
  }

  /** Find a bucket node by its identifier
    *
    * @param id
    *   the identifier of the bucket
    * @return
    *   an option of the bucket node
    */
  def findNode(id: String): Option[BucketNode] = {
    roots.flatMap(_.findNode(id)).headOption
  }

  /** Find a bucket by its identifier
    *
    * @param id
    *   the identifier of the bucket
    * @return
    *   an option of the bucket
    */
  def find(id: String): Option[Bucket] = {
    findNode(id).map(_.bucket)
  }

  /** Get the total number of nodes in the bucket trees
    *
    * @return
    *   the total number of nodes
    */
  def size: Int = roots.map(countNodes).sum

  private def countNodes(node: BucketNode): Int = {
    1 + node.children.map(countNodes).sum
  }

  def maxDepth: Int = {
    if (roots.isEmpty) 0 else roots.map(_.depth).max
  }

  /** Get all bucket trees as sequences of nodes
    *
    * @return
    *   all node trees
    */
  def allTrees: Seq[Seq[BucketNode]] = roots.flatMap(collectTrees)

  /** Get all bucket trees as sequences of buckets
    *
    * @return
    *   all bucket trees
    */
  def allBuckets: Seq[Seq[Bucket]] = allTrees.map(_.map(_.bucket))

  private def collectTrees(node: BucketNode): Seq[Seq[BucketNode]] = {
    if (node.isLeaf) {
      Seq(Seq(node))
    } else {
      node.children.flatMap { child =>
        collectTrees(child).map(path => node +: path)
      }
    }
  }

  override def toString: String = {
    roots.flatMap(root => printNode(root, "", isLast = true)).mkString("\n")
  }

  private def printNode(
    node: BucketNode,
    prefix: String,
    isLast: Boolean,
    acc: Seq[String] = Seq.empty
  ): Seq[String] = {
    val connector = if (isLast) "└── " else "├── "

    val childPrefix = prefix + (if (isLast) "    " else "│   ")

    (acc :+ s"$prefix$connector${node.identifier} (path: ${node.bucketPath})") ++ node.children.zipWithIndex
      .flatMap { case (child, idx) =>
        printNode(child, childPrefix, idx == node.children.size - 1)
      }
  }
}

object BucketTree {

  def fromBuckets(buckets: Seq[Seq[Bucket]]): BucketTree = {
    if (buckets.isEmpty || buckets.forall(_.isEmpty)) {
      return BucketTree(Seq.empty)
    }

    val validBuckets = buckets.filter(_.nonEmpty)

    // Group by root bucket path
    val groupedByRoot = validBuckets.groupBy(_.head.path)

    val roots = groupedByRoot
      .map { case (_, pathsWithSameRoot) =>
        buildNode(pathsWithSameRoot)
      }
      .toSeq
      .sortBy(_.identifier)

    BucketTree(roots)
  }

  private def buildNode(paths: Seq[Seq[Bucket]], parent: Option[BucketNode] = None): BucketNode = {
    val currentBucket = paths.head.head

    val childPaths = paths
      .filter(_.size > 1)
      .map(_.tail)

    val node = BucketNode(currentBucket, parent = parent)

    val children = if (childPaths.isEmpty) {
      Seq.empty
    } else {
      childPaths
        .groupBy(_.head.path)
        .map { case (_, childPathGroup) => buildNode(childPathGroup, Some(node)) }
        .toSeq
        .sortBy(_.identifier)
    }

    BucketNode(currentBucket, children, parent)
  }
}
