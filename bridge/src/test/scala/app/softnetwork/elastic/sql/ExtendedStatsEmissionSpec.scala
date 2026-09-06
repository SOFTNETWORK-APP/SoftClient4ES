package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query._
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import com.sksamuel.elastic4s.requests.searches.aggs.{AbstractAggregation, Aggregation}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** Issue #222 (story BIDC-3): `STDDEV` / `VARIANCE` (the `extended_stats` family) over a
  * TRANSFORMED expression -- the shared bridge template's half.
  *
  * The template cannot render the shape (that needs the ES 8 / ES 9 client modules' serializer), so
  * what it owns is asserted here: the shape DISCRIMINATOR, both binds (plain and windowed) and both
  * doors; the marker carrying field + script; the Default serializer's LOUD, named refusal instead
  * of the silent raw-field statistic the library used to emit; and the raw-field `extended_stats`
  * emission, pinned byte-for-byte (AC 4 -- these fixtures did not exist before and they must not
  * move).
  *
  * Captured on the base commit (T2, both bridge copies, both doors), before the fix:
  * `STDDEV(YEAR(createdAt))` -> `"extended_stats":{"field":"createdAt"}` (no script: the standard
  * deviation of the raw timestamps), `STDDEV(ABS(salary))` -> `"extended_stats":{}` (neither field
  * nor script), while `MAX(YEAR(createdAt))` emitted `"field":"createdAt","script":{...}`.
  */
class ExtendedStatsEmissionSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  implicit def sqlQueryToRequest(sqlQuery: SelectStatement): ElasticSearchRequest =
    sqlQuery.statement match {
      case Some(value: SingleSearch) => value.copy(score = sqlQuery.score)
      case other => throw new IllegalArgumentException(s"Not a single search: $other")
    }

  private def requestOf(sql: String): ElasticSearchRequest = SelectStatement(sql)

  private def aggregationsOf(sql: String): Seq[ElasticAggregation] = SelectStatement(sql)

  private val family = Seq("STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP")

  private def plain(fn: String, operand: String) =
    s"SELECT id, $fn($operand) AS s FROM t GROUP BY id"

  private def windowed(fn: String, operand: String) =
    s"SELECT id, name, $fn($operand) OVER (PARTITION BY id) AS s FROM t"

  /** Every marker in the aggregation tree of `search`. */
  private def markersOf(
    aggs: Iterable[AbstractAggregation]
  ): Seq[ScriptedExtendedStatsAggregation] =
    aggs.toSeq.flatMap {
      case m: ScriptedExtendedStatsAggregation => Seq(m)
      case a: Aggregation                      => markersOf(a.subaggs)
      case _                                   => Seq.empty
    }

  private def markersOf(search: SearchRequest): Seq[ScriptedExtendedStatsAggregation] =
    markersOf(search.aggs)

  private val yearScript =
    "def param1 = (doc['createdAt'].size() == 0 ? null : " +
    "doc['createdAt'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"

  // ---------------------------------------------------------------------------------------------
  // The discriminator (AC 5): true for the plain bind, true for the windowed bind, false for a raw
  // field -- on the search-body door AND on the sqlQueryToAggregations door.
  // ---------------------------------------------------------------------------------------------

  "hasTransformExtendedStats" should "be true for every family member over a transform (plain bind)" in {
    family.foreach { fn =>
      withClue(s"[$fn] ") {
        requestOf(plain(fn, "YEAR(createdAt)")).hasTransformExtendedStats shouldBe true
        requestOf(plain(fn, "ABS(salary)")).hasTransformExtendedStats shouldBe true
      }
    }
  }

  it should "be true for every family member over a transform (windowed bind)" in {
    family.foreach { fn =>
      withClue(s"[$fn] ") {
        requestOf(windowed(fn, "YEAR(createdAt)")).hasTransformExtendedStats shouldBe true
        requestOf(windowed(fn, "ABS(salary)")).hasTransformExtendedStats shouldBe true
      }
    }
  }

  it should "be false for a raw field, plain and windowed" in {
    family.foreach { fn =>
      withClue(s"[$fn] ") {
        requestOf(plain(fn, "salary")).hasTransformExtendedStats shouldBe false
        requestOf(windowed(fn, "salary")).hasTransformExtendedStats shouldBe false
      }
    }
  }

  it should "be false for a transform inside ANOTHER aggregate (MAX emits its own script)" in {
    requestOf(
      "SELECT id, MAX(YEAR(createdAt)) AS m FROM t GROUP BY id"
    ).hasTransformExtendedStats shouldBe false
  }

  it should "flag the aggregation itself on the sqlQueryToAggregations door" in {
    // The Default serializer refuses the body of a transform-bearing aggregation, so the
    // discriminator is read on the aggregation built with a permissive serializer -- passed
    // explicitly: an implicit declared in a test body is not in scope of a class-level helper.
    def permissiveAggregationsOf(sql: String): Seq[ElasticAggregation] =
      sqlQueryToAggregations(SelectStatement(sql))(
        timestamp,
        PainlessContextType.Query,
        TestSerializers.RawDefault
      )
    val transformed = permissiveAggregationsOf(plain("STDDEV", "YEAR(createdAt)"))
    transformed should have size 1
    transformed.head.hasTransformExtendedStats shouldBe true
    transformed.head.isGlobalMetric shouldBe true
    transformed.head.query shouldBe Some("<unrenderable>")

    val raw = permissiveAggregationsOf(plain("STDDEV", "salary"))
    raw should have size 1
    raw.head.hasTransformExtendedStats shouldBe false
    raw.head.isGlobalMetric shouldBe true
  }

  // ---------------------------------------------------------------------------------------------
  // The marker carries what a rendering serializer needs -- and nothing the raw field emits.
  // ---------------------------------------------------------------------------------------------

  "the marker" should "carry the field and the transform script for a field-derived transform" in {
    val markers = markersOf(requestOf(plain("STDDEV", "YEAR(createdAt)")).search)
    markers should have size 1
    val inner = markers.head.inner
    inner.name shouldBe "s"
    inner.field shouldBe Some("createdAt")
    inner.script.map(_.script) shouldBe Some(yearScript)
  }

  it should "carry no field for a transform with no raw field behind it (ABS(salary))" in {
    // Before the fix this shape emitted `extended_stats: {}` -- which Elasticsearch rejects.
    val markers = markersOf(requestOf(plain("VARIANCE", "ABS(salary)")).search)
    markers should have size 1
    markers.head.inner.field shouldBe None
    markers.head.inner.script.map(_.script) shouldBe Some(
      "def param1 = (doc['salary'].size() == 0 ? null : doc['salary'].value); " +
      "(param1 == null) ? null : Double.valueOf(Math.abs(param1))"
    )
  }

  it should "sit under the partition bucket on the windowed bind" in {
    val search = requestOf(windowed("STDDEV", "YEAR(createdAt)")).search
    // Root level carries the partition terms aggregation, not the marker...
    search.aggs.collect { case m: ScriptedExtendedStatsAggregation => m } shouldBe empty
    // ...and the marker is exactly one level below it.
    val markers = markersOf(search)
    markers should have size 1
    markers.head.inner.field shouldBe Some("createdAt")
    markers.head.inner.script.map(_.script) shouldBe Some(yearScript)
  }

  it should "refuse to wrap an extended_stats without a script" in {
    import com.sksamuel.elastic4s.ElasticApi.extendedStatsAgg
    an[IllegalArgumentException] should be thrownBy
    ScriptedExtendedStatsAggregation(extendedStatsAgg("s", "salary"))
  }

  // ---------------------------------------------------------------------------------------------
  // The Default serializer REFUSES the shape, loudly and by name -- both doors, both binds.
  // ---------------------------------------------------------------------------------------------

  "the Default serializer" should "refuse a transform-bearing extended_stats on the search-body door" in {
    Seq(plain("STDDEV", "YEAR(createdAt)"), windowed("VARIANCE", "ABS(salary)")).foreach { sql =>
      withClue(s"[$sql] ") {
        val refusal = intercept[UnsupportedOperationException](requestOf(sql).query)
        refusal.getMessage shouldBe SearchBodySerializer.TransformExtendedStatsUnsupported
        refusal.getMessage should include("elastic4s#4100")
      }
    }
  }

  it should "refuse it on the sqlQueryToAggregations door too" in {
    Seq(plain("STDDEV", "YEAR(createdAt)"), plain("VAR_POP", "ABS(salary)")).foreach { sql =>
      withClue(s"[$sql] ") {
        val refusal = intercept[UnsupportedOperationException](aggregationsOf(sql))
        refusal.getMessage shouldBe SearchBodySerializer.TransformExtendedStatsUnsupported
      }
    }
  }

  it should "name the major in the client-module wording" in {
    SearchBodySerializer.transformExtendedStatsUnsupportedOn(7) should include("Elasticsearch 7")
    SearchBodySerializer.transformExtendedStatsUnsupportedOn(6) should include("Elasticsearch 6")
    SearchBodySerializer.transformExtendedStatsUnsupportedOn(6) should include("elastic4s#4100")
  }

  // ---------------------------------------------------------------------------------------------
  // Raw-field extended_stats: byte-identical to the base commit on every major (AC 4).
  // ---------------------------------------------------------------------------------------------

  "a raw-field extended_stats" should "emit exactly what it emitted before (plain bind)" in {
    requestOf(plain("STDDEV", "salary")).query shouldBe
    """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""" +
    """"terms":{"field":"id","size":65536,"min_doc_count":1},""" +
    """"aggs":{"s":{"extended_stats":{"field":"salary"}}}}}}"""
  }

  it should "emit exactly what it emitted before (windowed bind)" in {
    requestOf(windowed("STDDEV", "salary")).query shouldBe
    """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""" +
    """"terms":{"field":"id","size":65536,"min_doc_count":1},""" +
    """"aggs":{"s":{"extended_stats":{"field":"salary"}}}}}}"""
  }

  it should "emit exactly what it emitted before (sqlQueryToAggregations door)" in {
    val aggs = aggregationsOf(plain("VAR_POP", "salary"))
    aggs should have size 1
    aggs.head.query shouldBe Some(
      """{"query":{"match_all":{}},"size":0,"aggs":{"s":{"extended_stats":{"field":"salary"}}}}"""
    )
  }

  it should "leave every OTHER scripted metric untouched (MAX(YEAR(x)) still emits its script)" in {
    requestOf("SELECT id, MAX(YEAR(createdAt)) AS m FROM t GROUP BY id").query shouldBe
    """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""" +
    """"terms":{"field":"id","size":65536,"min_doc_count":1},""" +
    """"aggs":{"m":{"max":{"field":"createdAt","script":{"lang":"painless",""" +
    s""""source":"$yearScript"}}}}}}}"""
  }
}

/** Test-side serializers for the shared template tree. */
object TestSerializers {

  /** The library's one-argument builder WITHOUT the Default's refusal -- reaches elastic4s's own
    * behaviour on a marker (a `NotImplementedError`), so a test can build the aggregations of a
    * transform-bearing statement on the sqlQueryToAggregations door and inspect them.
    */
  object RawDefault extends SearchBodySerializer {
    override def serialize(search: SearchRequest): String =
      if (SearchBodySerializer.hasTransformExtendedStats(search)) "<unrenderable>"
      else SearchBodySerializer.Default.serialize(search)
  }
}
