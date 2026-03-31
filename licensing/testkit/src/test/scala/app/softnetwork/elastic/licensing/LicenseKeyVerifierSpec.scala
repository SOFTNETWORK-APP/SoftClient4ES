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

import com.nimbusds.jose.jwk.OctetKeyPair
import com.nimbusds.jose.jwk.gen.OctetKeyPairGenerator
import com.nimbusds.jose.jwk.Curve
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LicenseKeyVerifierSpec extends AnyFlatSpec with Matchers {

  "LicenseKeyVerifier.verify" should "return true for a valid signature" in {
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val signed = SignedJWT.parse(jwt)
    LicenseKeyVerifier.verify(signed, JwtTestHelper.publicKey) shouldBe true
  }

  it should "return false for a tampered payload" in {
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val signed = SignedJWT.parse(jwt)
    // Tamper: create a new JWT with a different subject but the same signature
    val tampered = new SignedJWT(
      signed.getHeader.toBase64URL,
      new JWTClaimsSet.Builder(signed.getJWTClaimsSet).subject("tampered").build().toPayload.toBase64URL,
      signed.getSignature
    )
    LicenseKeyVerifier.verify(tampered, JwtTestHelper.publicKey) shouldBe false
  }

  it should "return false when verified against a different key" in {
    val otherKeyPair = new OctetKeyPairGenerator(Curve.Ed25519)
      .keyID("other-key")
      .generate()
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val signed = SignedJWT.parse(jwt)
    LicenseKeyVerifier.verify(
      signed,
      otherKeyPair.toPublicJWK.asInstanceOf[OctetKeyPair]
    ) shouldBe false
  }

  "LicenseKeyVerifier.loadPublicKey" should "load the test key by kid from classpath" in {
    val result = LicenseKeyVerifier.loadPublicKey("softclient4es-test")
    result shouldBe a[Right[_, _]]
    val key = result.toOption.get
    key.getKeyID shouldBe "softclient4es-test"
    key.getCurve.getName shouldBe "Ed25519"
  }

  it should "return Left for an unknown kid" in {
    val result = LicenseKeyVerifier.loadPublicKey("unknown-key-id")
    result shouldBe a[Left[_, _]]
    result.left.toOption.get shouldBe a[InvalidLicense]
    result.left.toOption.get.asInstanceOf[InvalidLicense].reason should include("Unknown key ID")
  }
}
