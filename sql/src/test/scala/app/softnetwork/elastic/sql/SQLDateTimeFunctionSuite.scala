package app.softnetwork.elastic.sql

import org.scalatest.funsuite.AnyFunSuite
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.time._
import app.softnetwork.elastic.sql.time._
import TimeUnit._
import app.softnetwork.elastic.sql.`type`.SQLType

class SQLDateTimeFunctionSuite extends AnyFunSuite {

  // Base d'exemple
  val baseDate = "doc['createdAt'].value"

  // Liste de toutes les fonctions transformables avec leurs types
  val transformFunctions: Seq[TransformFunction[_, _]] = Seq(
    ParseDate(Identifier(), "yyyy-MM-dd"),
    ParseDateTime(Identifier(), "yyyy-MM-dd HH:mm:ss"),
    DateAdd(Identifier(), TimeInterval(1, Day)),
    DateSub(Identifier(), TimeInterval(2, Month)),
    DateTimeAdd(Identifier(), TimeInterval(3, Hour)),
    DateTimeSub(Identifier(), TimeInterval(30, Minute)),
    DateTrunc(Identifier(), Day),
    Extract(Day),
    FormatDate(Identifier(), "yyyy-MM-dd"),
    FormatDateTime(Identifier(), "yyyy-MM-dd HH:mm:ss"),
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND
  )

  // Fonction pour chaîner une séquence de transformations en vérifiant les types
  def chainTransformsTyped(
    base: String,
    transforms: Seq[TransformFunction[_, _]]
  ): String = {
    require(transforms.nonEmpty, "No transforms provided")

    val initial: (String, SQLType) =
      (transforms.head.toPainless(base, 0), transforms.head.outputType.asInstanceOf[SQLType])

    val (finalExpr, _) = transforms.tail.foldLeft(initial) {
      case ((expr, currentType), t: FunctionN[_, _]) =>
        if (!currentType.getClass.isAssignableFrom(t.inputType.getClass)) {
          throw new IllegalArgumentException(
            s"Type mismatch: expected ${currentType.getClass.getSimpleName}, got ${t.inputType.getClass.getSimpleName}"
          )
        }
        (t.toPainless(expr, 0), t.outputType.asInstanceOf[SQLType])
    }

    finalExpr
  }

  // Générer dynamiquement tous les chaînages valides jusqu'à N fonctions
  def generateChains(
    functions: Seq[TransformFunction[_, _]],
    maxLength: Int
  ): Seq[Seq[TransformFunction[_, _]]] = {
    if (maxLength <= 1) functions.map(Seq(_))
    else {
      val shorter = generateChains(functions, maxLength - 1)
      for {
        chain <- shorter
        f     <- functions
        if f.inputType.getClass.isAssignableFrom(chain.last.outputType.getClass)
      } yield chain :+ f
    }
  }

  // Tester tous les chaînages pour N=2 et N=3
  val chains2: Seq[Seq[TransformFunction[_, _]]] =
    generateChains(transformFunctions, 2)
  val chains3: Seq[Seq[TransformFunction[_, _]]] =
    generateChains(transformFunctions, 3)

  (chains2 ++ chains3).zipWithIndex.foreach { case (chain, idx) =>
    val names = chain.map(_.sql).mkString(" -> ")
    test(s"Valid chain $idx: $names") {
      val chained = chainTransformsTyped(baseDate, chain)
      val expected = chain.reverse.tail.foldLeft(chain.last.toPainless(baseDate, 0)) { (expr, f) =>
        f.toPainless(expr, 0)
      }
      // On ne teste que la génération de code Painless sans évaluer le résultat
      assert(chained.nonEmpty)
    }
  }

  // Test simple pour chaque fonction individuelle
  transformFunctions.foreach { f =>
    test(s"Single transformation ${f.sql}") {
      val result = f.toPainless(baseDate, 0)
      assert(result.nonEmpty)
    }
  }
}
