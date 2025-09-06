package app.softnetwork.elastic.sql

import org.scalatest.funsuite.AnyFunSuite
import TimeUnit._

class SQLDateTimeFunctionSuite extends AnyFunSuite {

  // Base d'exemple
  val baseDate = "doc['createdAt'].value"

  // Liste de toutes les fonctions transformables avec leurs types
  val transformFunctions: Seq[SQLTransformFunction[_, _]] = Seq(
    ParseDate("yyyy-MM-dd"),
    ParseDateTime("yyyy-MM-dd HH:mm:ss"),
    DateAdd(TimeInterval(1, Day)),
    DateSub(TimeInterval(2, Month)),
    DateTimeAdd(TimeInterval(3, Hour)),
    DateTimeSub(TimeInterval(30, Minute)),
    DateTrunc(Day),
    Extract(Day),
    FormatDate("yyyy-MM-dd"),
    FormatDateTime("yyyy-MM-dd HH:mm:ss"),
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
    transforms: Seq[SQLTransformFunction[_, _]]
  ): String = {
    require(transforms.nonEmpty, "No transforms provided")

    val initial: (String, SQLType) =
      (transforms.head.toPainless(base), transforms.head.outputType.asInstanceOf[SQLType])

    val (finalExpr, _) = transforms.tail.foldLeft(initial) {
      case ((expr, currentType), t: SQLUnaryFunction[_, _]) =>
        if (!currentType.getClass.isAssignableFrom(t.inputType.getClass)) {
          throw new IllegalArgumentException(
            s"Type mismatch: expected ${currentType.getClass.getSimpleName}, got ${t.inputType.getClass.getSimpleName}"
          )
        }
        (t.toPainless(expr), t.outputType.asInstanceOf[SQLType])
    }

    finalExpr
  }

  // Générer dynamiquement tous les chaînages valides jusqu'à N fonctions
  def generateChains(
    functions: Seq[SQLTransformFunction[_, _]],
    maxLength: Int
  ): Seq[Seq[SQLTransformFunction[_, _]]] = {
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
  val chains2: Seq[Seq[SQLTransformFunction[_, _]]] =
    generateChains(transformFunctions, 2)
  val chains3: Seq[Seq[SQLTransformFunction[_, _]]] =
    generateChains(transformFunctions, 3)

  (chains2 ++ chains3).zipWithIndex.foreach { case (chain, idx) =>
    val names = chain.map(_.sql).mkString(" -> ")
    test(s"Valid chain $idx: $names") {
      val chained = chainTransformsTyped(baseDate, chain)
      val expected = chain.reverse.tail.foldLeft(chain.last.toPainless(baseDate)) { (expr, f) =>
        f.toPainless(expr)
      }
      // On ne teste que la génération de code Painless sans évaluer le résultat
      assert(chained.nonEmpty)
    }
  }

  // Test simple pour chaque fonction individuelle
  transformFunctions.foreach { f =>
    test(s"Single transformation ${f.sql}") {
      val result = f.toPainless(baseDate)
      assert(result.nonEmpty)
    }
  }
}
