package app.softnetwork.elastic.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ElasticCredentialsSpec extends AnyFlatSpec with Matchers {

  "ElasticCredentials" should "auto-detect Basic Auth" in {
    val creds = ElasticCredentials(
      username = "elastic",
      password = "changeme"
    )
    creds.authMethod shouldBe Some(BasicAuth)
    creds.isBasicAuth shouldBe true
  }

  it should "auto-detect API Key Auth" in {
    val creds = ElasticCredentials(
      apiKey = Some("VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw==")
    )
    creds.authMethod shouldBe Some(ApiKeyAuth)
    creds.isApiKeyAuth shouldBe true
  }

  it should "auto-detect Bearer Token Auth" in {
    val creds = ElasticCredentials(
      bearerToken = Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
    )
    creds.authMethod shouldBe Some(BearerTokenAuth)
    creds.isBearerTokenAuth shouldBe true
  }

  it should "encode API Key when in id:api_key format" in {
    val creds = ElasticCredentials(
      apiKey = Some("VuaCfGcBCdbkQm-e5aOx:ui2lp2axTNmsyakw9tvNnw")
    )
    val encoded = creds.encodedApiKey.get

    // Should be Base64 encoded
    encoded should not contain ":"

    // Should decode back to original
    import java.util.Base64
    import java.nio.charset.StandardCharsets
    val decoded = new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)
    decoded shouldBe creds.apiKey.get
  }

  it should "not re-encode already encoded API Key" in {
    val alreadyEncoded = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
    val creds = ElasticCredentials(
      apiKey = Some(alreadyEncoded)
    )
    creds.encodedApiKey.get shouldBe alreadyEncoded
  }

  it should "validate credentials correctly" in {
    // Valid Basic Auth
    ElasticCredentials(
      username = "user",
      password = "pass"
    ).validate() shouldBe Right(())

    // Invalid Basic Auth (missing password)
    ElasticCredentials(
      username = "user",
      method = Some("basic")
    ).validate().isLeft shouldBe true

    // Valid API Key
    ElasticCredentials(
      apiKey = Some("key123")
    ).validate() shouldBe Right(())

    // No auth is valid
    ElasticCredentials().validate() shouldBe Right(())
  }
}
