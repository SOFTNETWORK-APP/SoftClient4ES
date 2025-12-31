package app.softnetwork.elastic.sql.schema

object MappingsRules {

  // Options can be modified without reindexing
  private val safeOptions = Set(
    "ignore_above",
    "null_value",
    "store",
    "boost",
    "coerce",
    "copy_to",
    "meta"
  )

  // Options requiring a reindex
  private val unsafeOptions = Set(
    "analyzer",
    "search_analyzer",
    "normalizer",
    "index",
    "doc_values",
    "format",
    "fields",
    "similarity",
    "eager_global_ordinals"
  )

  def isSafe(key: String): Boolean = {

    if (safeOptions.contains(key)) return true

    if (unsafeOptions.contains(key)) return false

    // Default: cautious → UNSAFE
    false
  }
}
