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

package app.softnetwork.elastic.client.spi

import app.softnetwork.elastic.client.ElasticConfig

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.duration.{Duration => ScalaDuration, FiniteDuration}

/** The key under which [[ElasticClientFactory]] caches a client: two callers share a client only
  * when their effective `elastic.*` configurations are equal.
  *
  * It keeps no secret. Equality is the URL plus a fingerprint, an HMAC-SHA256 of the whole
  * [[ElasticConfig]] under a random key drawn once per class loader, so a fingerprint that leaked
  * anyway cannot be used to guess a password offline. `toString` names the URL only.
  */
private[spi] final class ClientKey private (val url: String, private val fingerprint: String) {

  override def equals(other: Any): Boolean = other match {
    case that: ClientKey => url == that.url && fingerprint == that.fingerprint
    case _               => false
  }

  override def hashCode: Int = 31 * url.hashCode + fingerprint.hashCode

  override def toString: String = s"ClientKey($url)"
}

private[spi] object ClientKey {

  private[this] val Algorithm = "HmacSHA256"

  /** Drawn once per class loader; never leaves this object. */
  private[this] val secret: SecretKeySpec = {
    val bytes = new Array[Byte](32)
    new SecureRandom().nextBytes(bytes)
    new SecretKeySpec(bytes, Algorithm)
  }

  /** The ONE key function of the client cache: every cache and every path reads its key here.
    *
    * The whole configuration is the key, not the identity alone: the credentials become the auth
    * header, the timeouts the request configuration, the scroll settings the connection pool, and
    * the client reads `include-document-id` and the schema-cache TTL. The case-class tree is walked
    * generically, so a field added to [[ElasticConfig]] joins the key without anyone editing a
    * list.
    */
  def of(config: ElasticConfig): ClientKey = {
    val mac = Mac.getInstance(Algorithm)
    mac.init(secret)
    val fingerprint = Base64.getEncoder.encodeToString(mac.doFinal(canonical(config)))
    new ClientKey(config.credentials.url, fingerprint)
  }

  /** An unambiguous encoding of a configuration tree: a type tag before every value and a length
    * before every string, so that no two different trees encode to the same bytes (comma-joined
    * `toString`s would make ("a,b", "c") and ("a", "b,c") collide).
    *
    * Leaves are strings, booleans, numbers, durations and case objects. Any other leaf type fails
    * loudly: a value whose encoding is not its value would otherwise give every call a new client
    * or, worse, two identities one client. The message names the type, never the value.
    */
  private[spi] def canonical(value: Any): Array[Byte] = {
    val bytes = new ByteArrayOutputStream(512)
    val out = new DataOutputStream(bytes)
    write(out, value)
    out.flush()
    bytes.toByteArray
  }

  private[this] def write(out: DataOutputStream, value: Any): Unit = value match {
    case null       => out.writeByte('0')
    case s: String  => out.writeByte('S'); writeString(out, s)
    case b: Boolean => out.writeByte('Z'); out.writeBoolean(b)
    case i: Int     => out.writeByte('I'); out.writeInt(i)
    case l: Long    => out.writeByte('J'); out.writeLong(l)
    case d: Double =>
      out.writeByte('D')
      // 0.0 == -0.0 for case-class equality, so they must encode alike
      out.writeLong(java.lang.Double.doubleToLongBits(if (d == 0.0d) 0.0d else d))
    case d: java.time.Duration =>
      out.writeByte('T'); out.writeLong(d.getSeconds); out.writeInt(d.getNano)
    case d: FiniteDuration =>
      out.writeByte('F'); out.writeLong(d.toNanos)
    case d: ScalaDuration => // Inf, MinusInf, Undefined
      out.writeByte('V'); writeString(out, d.toString)
    case None => out.writeByte('N')
    case Some(v) =>
      out.writeByte('O'); write(out, v)
    case p: Product =>
      out.writeByte('P')
      writeString(out, p.productPrefix)
      out.writeInt(p.productArity)
      p.productIterator.foreach(write(out, _))
    case other =>
      throw new IllegalArgumentException(
        s"Unsupported configuration value type in a client cache key: ${other.getClass.getName}"
      )
  }

  private[this] def writeString(out: DataOutputStream, s: String): Unit = {
    val utf8 = s.getBytes(StandardCharsets.UTF_8)
    out.writeInt(utf8.length)
    out.write(utf8)
  }
}
