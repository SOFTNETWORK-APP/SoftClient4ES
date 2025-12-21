package app.softnetwork.elastic.client.jest.actions

import io.searchbox.client.config.ElasticsearchVersion

object Pipeline {

  import io.searchbox.action.AbstractAction
  import io.searchbox.client.JestResult
  import com.google.gson.Gson

  case class Create(pipelineId: String, json: String) extends AbstractAction[JestResult] {

    payload = json

    override def getRestMethodName: String = "PUT"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_ingest/pipeline/$pipelineId"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201)
      Option(json).foreach(result.setJsonString)
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }

  case class Get(pipelineId: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "GET"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_ingest/pipeline/$pipelineId"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201 || statusCode == 404)
      if (statusCode != 404) {
        Option(json).foreach(result.setJsonString)
      }
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }

  case class Delete(pipelineName: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "DELETE"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_ingest/pipeline/$pipelineName"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201)
      Option(json).foreach(result.setJsonString)
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }
}
