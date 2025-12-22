package app.softnetwork.elastic.client.jest.actions

import io.searchbox.client.config.ElasticsearchVersion

object Template {

  import io.searchbox.action.AbstractAction
  import io.searchbox.client.JestResult
  import com.google.gson.Gson

  case class Create(templateName: String, json: String) extends AbstractAction[JestResult] {

    payload = json

    override def getRestMethodName: String = "PUT"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_template/$templateName"

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

  case class Get(templateName: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "GET"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_template/$templateName"

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

  case class GetAll() extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "GET"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      "/_template"

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

  case class Delete(templateName: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "DELETE"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_template/$templateName"

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

  case class Exists(templateName: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "HEAD"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_template/$templateName"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 404)
      Option(json).foreach(result.setJsonString)
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }
}
