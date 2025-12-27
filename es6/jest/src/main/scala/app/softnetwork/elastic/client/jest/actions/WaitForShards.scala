package app.softnetwork.elastic.client.jest.actions

import io.searchbox.action.{AbstractAction, GenericResultAbstractAction}
import io.searchbox.client.config.ElasticsearchVersion

object WaitForShards {
  class Builder(var index: String, var status: String = "yellow", var timeout: Int = 30)
      extends AbstractAction.Builder[WaitForShards, WaitForShards.Builder] {
    override def build = new WaitForShards(this)
  }
}

class WaitForShards protected (builder: WaitForShards.Builder)
    extends GenericResultAbstractAction(builder) {
  override def getRestMethodName = "GET"

  override def buildURI(elasticsearchVersion: ElasticsearchVersion): String =
    s"/_cluster/health/${builder.index}?wait_for_status=${builder.status}&timeout=${builder.timeout}s"

}
