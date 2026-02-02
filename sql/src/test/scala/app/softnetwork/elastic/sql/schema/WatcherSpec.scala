package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.function.FunctionWithIdentifier
import app.softnetwork.elastic.sql.{
  DateMathScript,
  Identifier,
  IntValue,
  ObjectValue,
  StringValue,
  StringValues
}
import app.softnetwork.elastic.sql.function.time.{CurrentDate, DateSub, DateTimeFunction}
import app.softnetwork.elastic.sql.operator.GT
import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.time.CalendarInterval
import app.softnetwork.elastic.sql.time.TimeUnit.DAYS
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

class WatcherSpec extends AnyFlatSpec with Matchers {

  implicit def criteriaToNode: Criteria => JsonNode = _ => mapper.createObjectNode()

  val intervalTrigger: WatcherTrigger = IntervalWatcherTrigger(Delay(TransformTimeUnit.Minutes, 5))

  val cronTrigger: WatcherTrigger = CronWatcherTrigger("0 */5 * * * ?")

  val compareConditionWithValue: WatcherCondition = CompareWatcherCondition(
    left = "ctx.payload.hits.total",
    operator = GT,
    right = Left(IntValue(10))
  )

  val dateMathScript: DateTimeFunction with FunctionWithIdentifier with DateMathScript =
    DateSub(
      identifier = Identifier(CurrentDate()),
      interval = CalendarInterval(5, DAYS)
    )

  val compareConditionWithDateMathScript: WatcherCondition = CompareWatcherCondition(
    left = "ctx.execution_time",
    operator = GT,
    right = Right(
      dateMathScript.identifier.withFunctions(dateMathScript +: dateMathScript.identifier.functions)
    )
  )

  val scriptCondition: WatcherCondition = ScriptWatcherCondition(
    script = "ctx.payload.hits.total > params.threshold",
    params = Map("threshold" -> IntValue(10))
  )

  // =============================================================
  // Watcher with logging action
  // =============================================================

  behavior of "Watcher with logging action"

  val loggingAction: WatcherAction = LoggingAction(
    LoggingActionConfig(
      text = "Watcher triggered with {{ctx.payload.hits.total}} hits"
    ),
    foreach = Some("ctx.payload.hits.hits"),
    maxIterations = Some(500)
  )

  val searchInput: WatcherInput = SearchWatcherInput(
    index = Seq("my_index"),
    query = None
  )

  it should "supports never condition with interval trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = NeverWatcherCondition,
      trigger = intervalTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
      | NEVER
      | TRIGGER EVERY 5 MINUTES
      | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
      | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"never":{}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  it should "supports always condition with interval trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = AlwaysWatcherCondition,
      trigger = intervalTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | ALWAYS
        | TRIGGER EVERY 5 MINUTES
        | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
        | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"always":{}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  it should "supports compare condition with value, interval trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = compareConditionWithValue,
      trigger = intervalTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN ctx.payload.hits.total > 10
        | TRIGGER EVERY 5 MINUTES
        | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
        | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"compare":{"ctx.payload.hits.total":{"gt":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  it should "supports compare condition with date math script, interval trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = compareConditionWithDateMathScript,
      trigger = intervalTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN ctx.execution_time > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
        | TRIGGER EVERY 5 MINUTES
        | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
        | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"compare":{"ctx.execution_time":{"gt":"now-5d/d"}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  it should "supports script condition with interval trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = scriptCondition,
      trigger = intervalTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | TRIGGER EVERY 5 MINUTES
        | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
        | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"script":{"source":"ctx.payload.hits.total > params.threshold","lang":"painless","params":{"threshold":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  it should "supports script condition with cron trigger and search input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = scriptCondition,
      trigger = cronTrigger,
      input = searchInput,
      actions = ListMap(
        "log_action" -> loggingAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | TRIGGER AT SCHEDULE '0 */5 * * * ?'
        | log_action AS (logging = (text = "Watcher triggered with {{ctx.payload.hits.total}} hits", level = "info"), foreach = "ctx.payload.hits.hits", max_iterations = 500)
        | WITH INPUT AS SELECT * FROM my_index""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"cron":"0 */5 * * * ?"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}}}},"condition":{"script":{"source":"ctx.payload.hits.total > params.threshold","lang":"painless","params":{"threshold":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  // =============================================================
  // Watcher with webhook action
  // =============================================================

  behavior of "Watcher with webhook action"

  val webhookScriptCondition: WatcherCondition = ScriptWatcherCondition(
    script = "ctx.payload.keys.size > params.threshold",
    params = Map("threshold" -> IntValue(1))
  )

  val webhookAction: WatcherAction = WebhookAction(
    WebhookActionConfig(
      scheme = "https",
      host = "example.com",
      port = 443,
      method = "POST",
      path = "/webhook",
      headers = Some(Map("Content-Type" -> "application/json")),
      body = Some("""{"message": "Watcher triggered with {{ctx.payload._value}}"}"""),
      params = Some(Map("watch_id" -> "{{ctx.watch_id}}")),
      connectionTimeout = Some(Delay(TransformTimeUnit.Seconds, 10)),
      readTimeout = Some(Delay(TransformTimeUnit.Seconds, 30))
    ),
    foreach = Some("ctx.payload.keys"),
    maxIterations = Some(2)
  )

  val simpleInput: WatcherInput = SimpleWatcherInput(
    payload =
      ObjectValue(Map("keys" -> StringValues(Seq(StringValue("value1"), StringValue("value2")))))
  )

  it should "supports never condition with interval trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = NeverWatcherCondition,
      trigger = intervalTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | NEVER
        | TRIGGER EVERY 5 MINUTES
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"never":{}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

  it should "supports always condition with interval trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = AlwaysWatcherCondition,
      trigger = intervalTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | ALWAYS
        | TRIGGER EVERY 5 MINUTES
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"always":{}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

  it should "supports compare condition with value, interval trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = compareConditionWithValue,
      trigger = intervalTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN ctx.payload.hits.total > 10
        | TRIGGER EVERY 5 MINUTES
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"compare":{"ctx.payload.hits.total":{"gt":10}}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

  it should "supports compare condition with date math script, interval trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = compareConditionWithDateMathScript,
      trigger = intervalTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN ctx.execution_time > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
        | TRIGGER EVERY 5 MINUTES
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"compare":{"ctx.execution_time":{"gt":"now-5d/d"}}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

  it should "supports script condition with interval trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = webhookScriptCondition,
      trigger = intervalTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN SCRIPT 'ctx.payload.keys.size > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 1) RETURNS TRUE
        | TRIGGER EVERY 5 MINUTES
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"script":{"source":"ctx.payload.keys.size > params.threshold","lang":"painless","params":{"threshold":1}}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

  it should "supports script condition with cron trigger and simple input" in {
    val watcher = Watcher(
      id = "my_watcher",
      condition = webhookScriptCondition,
      trigger = cronTrigger,
      input = simpleInput,
      actions = ListMap(
        "webhook_action" -> webhookAction
      )
    )

    watcher.sql.replaceAll("\n", " ").replaceAll("\\s+", " ") shouldBe
    """CREATE OR REPLACE WATCHER my_watcher AS
        | WHEN SCRIPT 'ctx.payload.keys.size > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 1) RETURNS TRUE
        | TRIGGER AT SCHEDULE '0 */5 * * * ?'
        | webhook_action AS (webhook = (path = "/webhook", params = (watch_id = "{{ctx.watch_id}}"), port = 443, scheme = "https", headers = (Content-Type = "application/json"), connection_timeout = "10s", method = "POST", body = "{"message": "Watcher triggered with {{ctx.payload._value}}"}", host = "example.com", read_timeout = "30s"), foreach = "ctx.payload.keys", max_iterations = 2)
        | WITH INPUT (keys = ["value1","value2"])""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"cron":"0 */5 * * * ?"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"script":{"source":"ctx.payload.keys.size > params.threshold","lang":"painless","params":{"threshold":1}}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

}
