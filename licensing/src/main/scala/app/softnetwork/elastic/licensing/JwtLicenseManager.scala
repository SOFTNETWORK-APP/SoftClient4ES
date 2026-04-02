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
import com.nimbusds.jwt.SignedJWT
import com.typesafe.scalalogging.LazyLogging

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import scala.util.Try

class JwtLicenseManager(
  publicKeyOverride: Option[OctetKeyPair] = None,
  expectedIssuer: String = "https://license.softclient4es.com"
) extends LicenseManager
    with LazyLogging {

  private case class LicenseState(
    licenseKey: LicenseKey,
    quota: Quota,
    graceStatus: GraceStatus = GraceStatus.NotInGrace,
    gracePeriodDays: Option[Long] = None,
    degraded: Boolean = false
  )

  private val state: AtomicReference[LicenseState] = new AtomicReference(
    LicenseState(LicenseKey.Community, Quota.Community)
  )

  override def validate(jwt: String): Either[LicenseError, LicenseKey] =
    doValidate(jwt, gracePeriod = None)

  def validateWithGracePeriod(
    jwt: String,
    gracePeriod: Duration
  ): Either[LicenseError, LicenseKey] =
    doValidate(jwt, gracePeriod = Some(gracePeriod))

  def resetToCommunity(): Unit =
    state.set(
      LicenseState(
        LicenseKey.Community,
        Quota.Community,
        GraceStatus.NotInGrace,
        None,
        degraded = true
      )
    )

  override def hasFeature(feature: Feature): Boolean =
    state.get().licenseKey.features.contains(feature)

  override def quotas: Quota = state.get().quota

  override def licenseType: LicenseType = state.get().licenseKey.licenseType

  override def graceStatus: GraceStatus = state.get().graceStatus

  override def wasDegraded: Boolean = state.get().degraded

  // Per-request warning — only fires during MidGrace (second half of grace period).
  // EarlyGrace startup warnings are handled by LicenseResolver.logGraceWarning().
  // Re-computes days from Instant.now() so warnings stay accurate between validate() calls.
  override def warnIfInGrace(): Unit = {
    val s = state.get()
    s.licenseKey.expiresAt.foreach { exp =>
      if (exp.isBefore(Instant.now())) {
        s.gracePeriodDays.foreach { gpDays =>
          val daysSinceExpiry = Duration.between(exp, Instant.now()).toDays
          val earlyThreshold = gpDays / 2
          if (daysSinceExpiry >= earlyThreshold && daysSinceExpiry <= gpDays) {
            val daysRemaining = math.max(0L, gpDays - daysSinceExpiry)
            logger.warn(
              s"License expired $daysSinceExpiry days ago. Service will degrade to Community in $daysRemaining days."
            )
          }
        }
      }
    }
  }

  private def doValidate(
    jwt: String,
    gracePeriod: Option[Duration]
  ): Either[LicenseError, LicenseKey] =
    for {
      signed    <- parseJwt(jwt)
      publicKey <- resolvePublicKey(signed)
      _         <- verifySignature(signed, publicKey)
      _         <- validateIssuer(signed)
      key       <- extractLicenseKey(signed, gracePeriod)
    } yield {
      val quota = extractQuota(signed)
      val gs = computeGraceStatus(key.expiresAt, gracePeriod)
      val gpDays = gracePeriod.map(_.toDays)
      state.set(LicenseState(key, quota, gs, gpDays, degraded = false))
      key
    }

  private def computeGraceStatus(
    expiresAt: Option[Instant],
    gracePeriod: Option[Duration]
  ): GraceStatus =
    expiresAt match {
      case Some(exp) if exp.isBefore(Instant.now()) =>
        gracePeriod match {
          case Some(grace) =>
            val daysSinceExpiry = Duration.between(exp, Instant.now()).toDays
            val graceDays = grace.toDays
            val earlyThreshold = graceDays / 2
            if (daysSinceExpiry < earlyThreshold)
              GraceStatus.EarlyGrace(daysSinceExpiry)
            else
              GraceStatus.MidGrace(daysSinceExpiry, math.max(0L, graceDays - daysSinceExpiry))
          case None =>
            GraceStatus.NotInGrace
        }
      case _ => GraceStatus.NotInGrace
    }

  private def parseJwt(jwt: String): Either[LicenseError, SignedJWT] =
    Try(SignedJWT.parse(jwt)).toEither.left.map(_ => InvalidLicense("Malformed JWT"))

  private def resolvePublicKey(signed: SignedJWT): Either[LicenseError, OctetKeyPair] =
    publicKeyOverride match {
      case Some(key) => Right(key)
      case None =>
        Option(signed.getHeader.getKeyID) match {
          case Some(kid) => LicenseKeyVerifier.loadPublicKey(kid)
          case None      => Left(InvalidLicense("Missing key ID (kid) in JWT header"))
        }
    }

  private def verifySignature(
    signed: SignedJWT,
    publicKey: OctetKeyPair
  ): Either[LicenseError, Unit] =
    if (LicenseKeyVerifier.verify(signed, publicKey)) Right(())
    else Left(InvalidLicense("Invalid signature"))

  private def validateIssuer(signed: SignedJWT): Either[LicenseError, Unit] = {
    val iss = Option(signed.getJWTClaimsSet.getIssuer).getOrElse("")
    if (iss == expectedIssuer) Right(())
    else Left(InvalidLicense(s"Invalid issuer: $iss"))
  }

  private def extractLicenseKey(
    signed: SignedJWT,
    gracePeriod: Option[Duration]
  ): Either[LicenseError, LicenseKey] = {
    val claims = signed.getJWTClaimsSet

    val tierStr = Option(claims.getStringClaim("tier"))
    val tier = tierStr.map(LicenseType.fromString).getOrElse(LicenseType.Community)

    val features: Set[Feature] = Option(claims.getStringListClaim("features"))
      .map(_.asScala.flatMap(Feature.fromString).toSet)
      .getOrElse(Set.empty)

    val expiresAt = Option(claims.getExpirationTime).map(_.toInstant)

    // Check expiry with optional grace period
    expiresAt match {
      case Some(exp: Instant) if exp.isBefore(Instant.now()) =>
        gracePeriod match {
          case Some(grace) if exp.plus(grace).isAfter(Instant.now()) =>
            // Within grace period — allow
            buildLicenseKey(claims, tier, features, expiresAt)
          case _ =>
            Left(ExpiredLicense(exp))
        }
      case _ =>
        buildLicenseKey(claims, tier, features, expiresAt)
    }
  }

  private def buildLicenseKey(
    claims: com.nimbusds.jwt.JWTClaimsSet,
    tier: LicenseType,
    features: Set[Feature],
    expiresAt: Option[Instant]
  ): Either[LicenseError, LicenseKey] = {
    val sub = Option(claims.getSubject).getOrElse("unknown")

    val metadata = Map.newBuilder[String, String]
    Option(claims.getStringClaim("org_name")).foreach(v => metadata += ("org_name" -> v))
    Option(claims.getJWTID).foreach(v => metadata += ("jti" -> v))
    Try(Option(claims.getBooleanClaim("trial")))
      .getOrElse(None)
      .foreach(v => metadata += ("trial" -> v.toString))

    Right(
      LicenseKey(
        id = sub,
        licenseType = tier,
        features = features,
        expiresAt = expiresAt,
        metadata = metadata.result()
      )
    )
  }

  private def extractQuota(signed: SignedJWT): Quota = {
    val claims = signed.getJWTClaimsSet
    val quotaObj = Option(claims.getJSONObjectClaim("quotas"))

    def intClaim(key: String): Option[Int] =
      quotaObj.flatMap(q => Option(q.get(key))).flatMap {
        case n: java.lang.Number => Some(n.intValue())
        case _                   => None
      }

    Quota(
      maxMaterializedViews = intClaim("max_materialized_views"),
      maxQueryResults = intClaim("max_result_rows"),
      maxConcurrentQueries = intClaim("max_concurrent_queries"),
      maxClusters = intClaim("max_clusters")
    )
  }
}
