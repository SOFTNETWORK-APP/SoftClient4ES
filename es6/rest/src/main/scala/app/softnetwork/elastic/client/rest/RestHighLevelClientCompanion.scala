package app.softnetwork.elastic.client.rest

import app.softnetwork.elastic.client.ElasticConfig
import com.sksamuel.exts.Logging
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.search.SearchModule

import scala.collection.JavaConverters._

trait RestHighLevelClientCompanion extends Logging {

  def elasticConfig: ElasticConfig

  private var client: Option[RestHighLevelClient] = None

  lazy val namedXContentRegistry: NamedXContentRegistry = {
    // import scala.jdk.CollectionConverters._
    val searchModule = new SearchModule(Settings.EMPTY, false, List.empty[SearchPlugin].asJava)
    new NamedXContentRegistry(searchModule.getNamedXContents)
  }

  def apply(): RestHighLevelClient = {
    client match {
      case Some(c) => c
      case _ =>
        val credentialsProvider = new BasicCredentialsProvider()
        if (elasticConfig.credentials.username.nonEmpty) {
          credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(
              elasticConfig.credentials.username,
              elasticConfig.credentials.password
            )
          )
        }
        val restClientBuilder: RestClientBuilder = RestClient
          .builder(
            HttpHost.create(elasticConfig.credentials.url)
          )
          .setHttpClientConfigCallback((httpAsyncClientBuilder: HttpAsyncClientBuilder) =>
            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
          )
        val c = new RestHighLevelClient(restClientBuilder)
        client = Some(c)
        c
    }
  }
}
