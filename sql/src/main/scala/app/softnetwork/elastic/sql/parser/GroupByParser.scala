package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{Bucket, GroupBy}

trait GroupByParser {
  self: Parser with WhereParser =>

  def bucket: PackratParser[Bucket] = (long | identifier) ^^ { i =>
    Bucket(i)
  }

  def groupBy: PackratParser[GroupBy] =
    GroupBy.regex ~ rep1sep(bucket, separator) ^^ { case _ ~ buckets =>
      GroupBy(buckets)
    }

}
