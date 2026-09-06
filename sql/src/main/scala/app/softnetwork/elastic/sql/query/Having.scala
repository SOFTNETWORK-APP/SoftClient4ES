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

import app.softnetwork.elastic.sql.{Expr, Identifier, TokenRegex, Updateable}

case object Having extends Expr("HAVING") with TokenRegex {

  /** `HAVING cnt > 1` where `cnt` aliases a SELECT aggregate (`COUNT(name) AS cnt`). The bare
    * identifier carries no aggregate function of its own, so the selector rendering saw no metric
    * in the condition and the whole HAVING degenerated to `1 == 1`: every group came back — the
    * "workaround" issue #54 itself prescribed, never tested (BIDC-2). Substituting the aliased
    * SELECT item's identifier BEFORE the criteria are updated makes the reference resolve like any
    * other aggregate: the alias comes back through `fieldAliases`, the selector reads `params.cnt`,
    * and no extra aggregation is created since the item is already a SELECT aggregate. Scoped to
    * HAVING on purpose: ORDER BY resolves an alias by name already, and WHERE must keep reading a
    * bare name as a field. Relation predicates (`NESTED(...)`) are left untouched.
    */
  private[query] def resolveAggregateAliases(
    criteria: Criteria,
    request: SingleSearch
  ): Criteria = {
    // Aggregates AND arithmetic over aggregates (`MAX(x) - MIN(x) AS d`): the latter is a
    // `bucket_script`, and a `bucket_selector` may read a sibling pipeline aggregation by name.
    val aliased: Map[String, Identifier] = request.select.fields.collect {
      case f if (f.isAggregation || f.isBucketScript) && f.fieldAlias.isDefined =>
        f.fieldAlias.get.alias -> f.identifier
    }.toMap
    if (aliased.isEmpty) return criteria

    def substitute(id: Identifier): Identifier =
      if (id.functions.isEmpty && !id.nested) aliased.getOrElse(id.name, id) else id

    def rewrite(c: Criteria): Criteria = c match {
      case p: Predicate =>
        p.copy(leftCriteria = rewrite(p.leftCriteria), rightCriteria = rewrite(p.rightCriteria))
      case e: GenericExpression =>
        e.copy(
          identifier = substitute(e.identifier),
          value = e.value match {
            case id: Identifier => substitute(id)
            case v              => v
          }
        )
      case e: Comparison =>
        e.copy(
          identifier = substitute(e.identifier),
          value = e.value match {
            case id: Identifier => substitute(id)
            case v              => v
          }
        )
      case e: BetweenExpr       => e.copy(identifier = substitute(e.identifier))
      case e: InExpr[_, _]      => e.copy(identifier = substitute(e.identifier))
      case e: IsNullCriteria    => e.copy(identifier = substitute(e.identifier))
      case e: IsNotNullCriteria => e.copy(identifier = substitute(e.identifier))
      case other                => other
    }

    rewrite(criteria)
  }
}

case class Having(criteria: Option[Criteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Having $c"
    case _       => ""
  }
  def update(request: SingleSearch): Having =
    this.copy(criteria =
      criteria.map(c => Having.resolveAggregateAliases(c, request).update(request))
    )

  override def validate(): Either[String, Unit] = criteria.map(_.validate()).getOrElse(Right(()))

  def nestedElements: Seq[NestedElement] =
    criteria.map(_.nestedElements).getOrElse(Seq.empty).groupBy(_.path).map(_._2.head).toList

  def script: Option[String] = criteria.flatMap { criteria =>
    val fullScript = MetricSelectorScript
      .metricSelector(criteria)
      .replaceAll("1 == 1 &&", "")
      .replaceAll("&& 1 == 1", "")
      .replaceAll("1 == 1", "")
      .trim
    if (fullScript.nonEmpty) Some(fullScript) else None
  }
}
