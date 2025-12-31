package app.softnetwork.elastic.sql.schema

object SettingsRules {

  private val dynamicPrefixes = Seq(
    "refresh_interval",
    "number_of_replicas",
    "max_result_window",
    "max_inner_result_window",
    "max_rescore_window",
    "max_docvalue_fields_search",
    "max_script_fields",
    "max_ngram_diff",
    "max_shingle_diff",
    "blocks.read_only",
    "blocks.read_only_allow_delete",
    "blocks.write",
    "routing.allocation"
  )

  private val staticPrefixes = Seq(
    "number_of_shards",
    "codec",
    "routing_partition_size",
    "analysis",
    "similarity",
    "sort",
    "mapping.total_fields.limit",
    "mapping.depth.limit",
    "mapping.nested_fields.limit"
  )

  def isDynamic(key: String): Boolean = {
    // If the setting is explicitly static → false
    if (staticPrefixes.exists(key.contains)) return false

    // If the setting is explicitly dynamic → true
    if (dynamicPrefixes.exists(key.contains)) return true

    // Default: cautious → UNSAFE
    false
  }
}
