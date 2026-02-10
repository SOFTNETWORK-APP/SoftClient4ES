package app.softnetwork.elastic.client.help

/** CLI for generating documentation
  */
object GenerateDocumentation extends App {
  val outputDir = if (args.nonEmpty) args(0) else "documentation/sql"

  println(s"Generating documentation to: $outputDir")
  MarkdownGenerator.generateAll(outputDir)
  println("Documentation generated successfully!")
}
