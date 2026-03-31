/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.licensing

import com.nimbusds.jose.{JWSAlgorithm, JWSHeader}
import com.nimbusds.jose.crypto.Ed25519Signer
import com.nimbusds.jose.jwk.OctetKeyPair
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}

import java.util.Date

object JwtTestHelper {

  /** Static test key pair — the private key is used to sign test JWTs. The corresponding public key
    * is in testkit/src/main/resources/keys/softclient4es-test.jwk
    */
  private val keyPairJson: String =
    """{"kty":"OKP","d":"AanRaois6uVjNOdq46JyJ57LJdrVX3Q-r4KIGwkm37Y","crv":"Ed25519","kid":"softclient4es-test","x":"EGBuSwTrahLvXcMhjr042wzUc4Wm0FTrTALpb56PLNg"}"""

  val keyPair: OctetKeyPair = OctetKeyPair.parse(keyPairJson)

  val publicKey: OctetKeyPair = keyPair.toPublicJWK.asInstanceOf[OctetKeyPair]

  def signJwt(
    claims: JWTClaimsSet,
    kid: String = "softclient4es-test",
    key: OctetKeyPair = keyPair
  ): String = {
    val header = new JWSHeader.Builder(JWSAlgorithm.EdDSA).keyID(kid).build()
    val signed = new SignedJWT(header, claims)
    signed.sign(new Ed25519Signer(key))
    signed.serialize()
  }

  def proClaimsBuilder(
    expiresAt: Date = new Date(System.currentTimeMillis() + 3600000L)
  ): JWTClaimsSet.Builder =
    new JWTClaimsSet.Builder()
      .issuer("https://license.softclient4es.com")
      .subject("org-acme-123")
      .claim("tier", "pro")
      .claim(
        "features",
        java.util.Arrays.asList(
          "materialized_views",
          "jdbc_driver",
          "unlimited_results",
          "flight_sql"
        )
      )
      .claim(
        "quotas", {
          val m = new java.util.LinkedHashMap[String, AnyRef]()
          m.put("max_materialized_views", Integer.valueOf(50))
          m.put("max_result_rows", Integer.valueOf(1000000))
          m.put("max_concurrent_queries", Integer.valueOf(50))
          m.put("max_clusters", Integer.valueOf(5))
          m
        }
      )
      .claim("org_name", "Acme Corp")
      .jwtID("lic-001")
      .claim("trial", false)
      .expirationTime(expiresAt)

  def enterpriseClaimsBuilder(
    expiresAt: Date = new Date(System.currentTimeMillis() + 3600000L)
  ): JWTClaimsSet.Builder =
    new JWTClaimsSet.Builder()
      .issuer("https://license.softclient4es.com")
      .subject("org-bigcorp-456")
      .claim("tier", "enterprise")
      .claim(
        "features",
        java.util.Arrays.asList(
          "materialized_views",
          "jdbc_driver",
          "odbc_driver",
          "unlimited_results",
          "advanced_aggregations",
          "flight_sql",
          "federation"
        )
      )
      .claim(
        "quotas", {
          val m = new java.util.LinkedHashMap[String, AnyRef]()
          m
        }
      )
      .claim("org_name", "BigCorp Inc")
      .jwtID("lic-002")
      .expirationTime(expiresAt)

  def communityClaimsBuilder(
    expiresAt: Date = new Date(System.currentTimeMillis() + 3600000L)
  ): JWTClaimsSet.Builder =
    new JWTClaimsSet.Builder()
      .issuer("https://license.softclient4es.com")
      .subject("org-free-789")
      .claim("tier", "community")
      .claim(
        "features",
        java.util.Arrays.asList("materialized_views", "jdbc_driver")
      )
      .claim(
        "quotas", {
          val m = new java.util.LinkedHashMap[String, AnyRef]()
          m.put("max_materialized_views", Integer.valueOf(3))
          m.put("max_result_rows", Integer.valueOf(10000))
          m.put("max_concurrent_queries", Integer.valueOf(5))
          m.put("max_clusters", Integer.valueOf(2))
          m
        }
      )
      .claim("org_name", "Free User")
      .jwtID("lic-003")
      .expirationTime(expiresAt)
}
