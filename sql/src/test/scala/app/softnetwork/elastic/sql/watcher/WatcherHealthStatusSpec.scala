package app.softnetwork.elastic.sql.watcher

import app.softnetwork.elastic.sql.health.HealthStatus
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeUnit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

class WatcherHealthStatusSpec extends AnyFlatSpec with Matchers {

  "overallHealthStatus" should "combine health statuses correctly" in {
    val watcher = WatcherHealthStatus(
      id = "test",
      active = true,
      lastChecked = Some(ZonedDateTime.now().minusSeconds(10)),
      frequency = Delay(TransformTimeUnit.Minutes, 1),
      executionState = Some(WatcherExecutionState.Failed)
    )

    // healthStatus = Green (checked recently)
    // executionState.health = Red (failed)
    // overall = Red (worst of Green + Red)
    watcher.overallHealthStatus shouldBe HealthStatus.Red
  }

  it should "handle missing executionState gracefully" in {
    val watcher = WatcherHealthStatus(
      id = "new_watcher",
      active = true,
      lastChecked = None,
      frequency = Delay(TransformTimeUnit.Minutes, 5),
      createdAt = Some(ZonedDateTime.now().minusSeconds(60)),
      executionState = None // ✅ Nouveau watcher
    )

    // Should be Green (warm-up period)
    watcher.overallHealthStatus shouldBe HealthStatus.Green
  }

  it should "flag old watchers without execution state" in {
    val watcher = WatcherHealthStatus(
      id = "old_watcher",
      active = true,
      lastChecked = None,
      frequency = Delay(TransformTimeUnit.Minutes, 1),
      createdAt = Some(ZonedDateTime.now().minusHours(24)),
      executionState = None // ❌ Vieux watcher sans état
    )

    // Should be Yellow or Red (suspicious)
    watcher.overallHealthStatus should not be HealthStatus.Green
  }
}
