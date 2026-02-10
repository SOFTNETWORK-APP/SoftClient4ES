package app.softnetwork.elastic.client.help

/** In-memory database of help entries
  */
case class HelpDatabase(
  commands: Map[String, SqlCommandHelp],
  functions: Map[String, FunctionHelp],
  aliases: Map[String, String]
) {

  def getHelp(topic: String): Option[HelpEntry] = {
    val normalized = topic.toUpperCase.trim
    commands
      .get(normalized)
      .orElse(functions.get(normalized))
      .orElse(aliases.get(normalized).flatMap(t => commands.get(t).orElse(functions.get(t))))
  }

  def search(query: String): Seq[HelpEntry] = {
    val q = query.toLowerCase
    val cmdResults = commands.values.filter { cmd =>
      cmd.name.toLowerCase.contains(q) ||
      cmd.shortDescription.toLowerCase.contains(q) ||
      cmd.description.toLowerCase.contains(q)
    }
    val fnResults = functions.values.filter { fn =>
      fn.name.toLowerCase.contains(q) ||
      fn.shortDescription.toLowerCase.contains(q) ||
      fn.description.toLowerCase.contains(q)
    }
    (cmdResults ++ fnResults).toSeq.sortBy(_.name)
  }

  def allTopics: Seq[String] = {
    (commands.keys ++ functions.keys ++ aliases.keys).toSeq.sorted
  }

  def byCategory(category: HelpCategory): Seq[HelpEntry] = {
    val cmdResults = commands.values.filter(_.category == category)
    val fnResults = if (category == HelpCategory.Function) functions.values else Seq.empty
    (cmdResults ++ fnResults).toSeq.sortBy(_.name)
  }
}
