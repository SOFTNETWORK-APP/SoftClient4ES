package app.softnetwork.elastic.client.jest.actions

import io.searchbox.action.AbstractAction
import io.searchbox.action.GenericResultAbstractAction
import io.searchbox.client.config.ElasticsearchVersion

object GetIndex {
  class Builder(var index: String) extends AbstractAction.Builder[GetIndex, GetIndex.Builder] {
    override def build = new GetIndex(this)
  }
}

class GetIndex protected (builder: GetIndex.Builder) extends GenericResultAbstractAction(builder) {
  override def getRestMethodName = "GET"

  override def buildURI(elasticsearchVersion: ElasticsearchVersion): String = s"/${builder.index}"

}
