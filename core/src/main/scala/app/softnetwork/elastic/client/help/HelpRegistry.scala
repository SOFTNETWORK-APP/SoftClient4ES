package app.softnetwork.elastic.client.help

/** Singleton registry that loads from JSON
  */
object HelpRegistry {

  private lazy val database: HelpDatabase = HelpJsonLoader.loadAll()

  def getHelp(topic: String): Option[HelpEntry] = database.getHelp(topic)

  def search(query: String): Seq[HelpEntry] = database.search(query)

  def allTopics: Seq[String] = database.allTopics

  def byCategory(category: HelpCategory): Seq[HelpEntry] = database.byCategory(category)

  def allByCategory: Map[HelpCategory, Seq[HelpEntry]] = {
    HelpCategory.all.map { cat =>
      cat -> byCategory(cat)
    }.toMap
  }

  // For commands map access (used by existing code)
  def commands: Map[String, HelpEntry] = database.commands ++ database.functions
}
