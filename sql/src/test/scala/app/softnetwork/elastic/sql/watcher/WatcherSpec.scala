package app.softnetwork.elastic.sql.watcher

import app.softnetwork.elastic.sql.function.FunctionWithIdentifier
import app.softnetwork.elastic.sql.function.time.{CurrentDate, DateSub, DateTimeFunction}
import app.softnetwork.elastic.sql.http._
import app.softnetwork.elastic.sql.operator.GT
import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.time.CalendarInterval
import app.softnetwork.elastic.sql.time.TimeUnit.DAYS
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeUnit}
import app.softnetwork.elastic.sql.{
  DateMathScript,
  Identifier,
  IntValue,
  Null,
  ObjectValue,
  StringValue,
  StringValues
}
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
    params = ListMap("threshold" -> IntValue(10))
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
    limit = Some(500)
  )

  val searchInput: WatcherInput = SearchWatcherInput(
    index = Seq("my_index"),
    query = None,
    timeout = Some(Delay(TransformTimeUnit.Minutes, 2))
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
      | EVERY 5 MINUTES
      | FROM my_index WITHIN 2 MINUTES
      | NEVER DO
      | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
      | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"never":{}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
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
        | EVERY 5 MINUTES
        | FROM my_index WITHIN 2 MINUTES
        | ALWAYS DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"always":{}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
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
        | EVERY 5 MINUTES
        | FROM my_index WITHIN 2 MINUTES
        | WHEN ctx.payload.hits.total > 10
        | DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"compare":{"ctx.payload.hits.total":{"gt":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
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
        | EVERY 5 MINUTES
        | FROM my_index WITHIN 2 MINUTES
        | WHEN ctx.execution_time > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
        | DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"compare":{"ctx.execution_time":{"gt":"now-5d/d"}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
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
        | EVERY 5 MINUTES
        | FROM my_index WITHIN 2 MINUTES
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"interval":"5m"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"script":{"source":"ctx.payload.hits.total > params.threshold","lang":"painless","params":{"threshold":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
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
        | AT SCHEDULE '0 */5 * * * ?'
        | FROM my_index WITHIN 2 MINUTES
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"cron":"0 */5 * * * ?"}},"input":{"search":{"request":{"indices":["my_index"],"body":{"query":{"match_all":{}}}},"timeout":"2m"}},"condition":{"script":{"source":"ctx.payload.hits.total > params.threshold","lang":"painless","params":{"threshold":10}}},"actions":{"log_action":{"foreach":"ctx.payload.hits.hits","max_iterations":500,"logging":{"text":"Watcher triggered with {{ctx.payload.hits.total}} hits","level":"info"}}}}"""
  }

  // =============================================================
  // Watcher with webhook action
  // =============================================================

  behavior of "Watcher with webhook action"

  val webhookScriptCondition: WatcherCondition = ScriptWatcherCondition(
    script = "ctx.payload.keys.size > params.threshold",
    params = ListMap("threshold" -> IntValue(1))
  )

  val url: Url =
    Url("https://example.com/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D")

  val timeout: Timeout =
    Timeout(
      connection = Some(Delay(TransformTimeUnit.Seconds, 10)),
      read = Some(Delay(TransformTimeUnit.Seconds, 30))
    )

  val webhookAction: WatcherAction = WebhookAction(
    HttpRequest(
      url = url,
      method = Method.Post,
      headers = Some(Headers(ListMap("Content-Type" -> StringValue("application/json")))),
      body =
        Some(Body(StringValue("""{"message": "Watcher triggered with {{ctx.payload._value}}"}"""))),
      timeout = Some(timeout)
    ),
    foreach = Some("ctx.payload.keys"),
    limit = Some(2)
  )

  val simpleInput: WatcherInput = SimpleWatcherInput(
    payload = ObjectValue(
      ListMap("keys" -> StringValues(Seq(StringValue("value1"), StringValue("value2"))))
    )
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
        | EVERY 5 MINUTES
        | WITH INPUT (keys = ["value1","value2"])
        | NEVER DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val webhook = watcher.actions
      .getOrElse("webhook_action", fail("webhook_action not found"))
      .asInstanceOf[WebhookAction]

    webhook.webhook.url.query
      .flatMap(_.params.get("watch_id"))
      .getOrElse(Null) shouldBe StringValue("{{ctx.watch_id}}")

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
        | EVERY 5 MINUTES
        | WITH INPUT (keys = ["value1","value2"])
        | ALWAYS DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
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
        | EVERY 5 MINUTES
        | WITH INPUT (keys = ["value1","value2"])
        | WHEN ctx.payload.hits.total > 10
        | DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
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
        | EVERY 5 MINUTES
        | WITH INPUT (keys = ["value1","value2"])
        | WHEN ctx.execution_time > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY)
        | DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
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
        | EVERY 5 MINUTES
        | WITH INPUT (keys = ["value1","value2"])
        | WHEN SCRIPT 'ctx.payload.keys.size > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 1) RETURNS TRUE
        | DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
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
        | AT SCHEDULE '0 */5 * * * ?'
        | WITH INPUT (keys = ["value1","value2"])
        | WHEN SCRIPT 'ctx.payload.keys.size > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 1) RETURNS TRUE
        | DO
        | webhook_action AS WEBHOOK POST "https://example.com:443/webhook?watch_id=%7B%7Bctx.watch_id%7D%7D" HEADERS (Content-Type = "application/json") BODY "{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}" TIMEOUT (connection = "10s", read = "30s") FOREACH "ctx.payload.keys" LIMIT 2
        | END""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\s+", " ")

    val json: String = watcher.node
    json shouldBe """{"trigger":{"schedule":{"cron":"0 */5 * * * ?"}},"input":{"simple":{"keys":["value1","value2"]}},"condition":{"script":{"source":"ctx.payload.keys.size > params.threshold","lang":"painless","params":{"threshold":1}}},"actions":{"webhook_action":{"foreach":"ctx.payload.keys","max_iterations":2,"webhook":{"scheme":"https","host":"example.com","port":443,"method":"post","path":"/webhook","headers":{"Content-Type":"application/json"},"params":{"watch_id":"{{ctx.watch_id}}"},"body":"{\"message\": \"Watcher triggered with {{ctx.payload._value}}\"}","connection_timeout":"10s","read_timeout":"30s"}}}}"""
  }

}
