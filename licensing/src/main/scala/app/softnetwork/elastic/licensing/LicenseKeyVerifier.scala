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

import com.nimbusds.jose.crypto.Ed25519Verifier
import com.nimbusds.jose.jwk.OctetKeyPair
import com.nimbusds.jwt.SignedJWT

import scala.io.Source
import scala.util.{Failure, Success, Try}

object LicenseKeyVerifier {

  /** Verify a signed JWT against an Ed25519 public key.
    *
    * @param jws
    *   the signed JWT to verify
    * @param publicKey
    *   the Ed25519 public key (JWK format)
    * @return
    *   true if the signature is valid
    */
  def verify(jws: SignedJWT, publicKey: OctetKeyPair): Boolean =
    Try(jws.verify(new Ed25519Verifier(publicKey.toPublicJWK))).getOrElse(false)

  /** Load an Ed25519 public key from the classpath by key ID.
    *
    * Looks for `keys/{kid}.jwk` on the classpath and parses it as JWK JSON.
    *
    * @param kid
    *   the key ID (matches the JWT `kid` header)
    * @return
    *   the public key or a LicenseError
    */
  def loadPublicKey(kid: String): Either[LicenseError, OctetKeyPair] = {
    val resourcePath = s"keys/$kid.jwk"
    Option(getClass.getClassLoader.getResourceAsStream(resourcePath)) match {
      case None =>
        Left(InvalidLicense(s"Unknown key ID: $kid"))
      case Some(is) =>
        val result = Try {
          val json = Source.fromInputStream(is, "UTF-8").mkString
          OctetKeyPair.parse(json).toPublicJWK.asInstanceOf[OctetKeyPair]
        }
        is.close()
        result match {
          case Success(key) => Right(key)
          case Failure(ex) =>
            Left(InvalidLicense(s"Failed to parse key '$kid': ${ex.getMessage}"))
        }
    }
  }
}
