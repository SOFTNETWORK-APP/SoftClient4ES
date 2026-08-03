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

package app.softnetwork.elastic.client.file

import java.net.URI
import java.nio.file.{Path, Paths}

import scala.util.{Success, Try}
import scala.util.matching.Regex

/** Classifies a `COPY INTO … FROM '<path>'` source string as "on this machine's filesystem" or
  * "somewhere Hadoop has to reach".
  *
  * Why this exists (issue #183): Hadoop's `FileSystem.get` reaches
  * `UserGroupInformation.getCurrentUser()`, which calls
  * `javax.security.auth.Subject.getSubject(AccessControlContext)`. JDK 23 re-specified that method
  * to throw `UnsupportedOperationException` whenever a Security Manager is not allowed (the
  * default), and JEP 486 (JDK 24) made it throw `("getSubject is not supported")` unconditionally
  * while removing the `-Djava.security.manager=allow` escape hatch (the VM refuses to start with
  * it). Verified against hadoop-common 3.4.2 bytecode — upgrading Hadoop does not help.
  *
  * Anything that only needs bytes off a local file therefore bypasses Hadoop entirely. Remote
  * schemes (`s3a`, `s3`, `gs`, `abfs`, `abfss`, `wasb`, `wasbs`, `hdfs`, …) keep going through
  * Hadoop unchanged, and so do Parquet and Delta reads, which genuinely need it.
  *
  * This object intentionally has NO Hadoop dependency so it can be unit-tested in isolation.
  */
object LocalPath {

  /** A URI scheme is at least TWO characters before the colon.
    *
    * The lower bound is not cosmetic: `new URI("C:/data/x.jsonl")` parses `C` as a scheme, so a
    * one-character prefix must never be treated as one — otherwise every Windows drive-letter path
    * would be misrouted to Hadoop.
    */
  private val SchemePrefix: Regex = """^([A-Za-z][A-Za-z0-9+.-]+):""".r

  private val FileScheme = "file"

  /** Only these authorities denote "this machine". */
  private val LocalAuthorities = Set("", "localhost")

  /** Extracted so `LocalPathSpec` can exercise BOTH platforms deterministically on either OS. */
  private[file] val onWindows: Boolean = java.io.File.separatorChar == '\\'

  /** The URI scheme of `filePath`, lowercased, or `None` when it has none.
    *
    * This is the ONLY scheme parser in the file package. `HadoopConfigurationFactory.forPath` used
    * to run its own (`Try(new URI(path).getScheme)`), which disagreed with this one in two ways
    * that silently cost a user their credentials — see AD-10.
    *
    * A Windows drive letter is not a scheme: [[SchemePrefix]] requires at least two characters
    * before the colon, so `C:/data/x.jsonl` yields `None`.
    */
  def scheme(filePath: String): Option[String] =
    Option(filePath)
      .filter(_.trim.nonEmpty)
      .flatMap(raw => SchemePrefix.findFirstMatchIn(raw).map(_.group(1).toLowerCase))

  /** Returns the local [[java.nio.file.Path]] denoted by `filePath`, or `None` when the path is not
    * on this machine's filesystem and must be handled by Hadoop.
    *
    * Local means: no scheme at all (absolute, relative, or a Windows drive path), or the `file:`
    * scheme with an empty or `localhost` authority.
    *
    * `~` is NOT expanded, and surrounding whitespace is NOT trimmed — both match Hadoop's
    * `Path(String)` exactly. Pass an absolute path.
    *
    * Throws [[java.nio.file.InvalidPathException]] (a subclass of `IllegalArgumentException`) for a
    * string this platform cannot represent as a path at all — see AD-8.
    */
  def resolve(filePath: String): Option[Path] =
    // `filter(_.trim.nonEmpty)` rejects blank input WITHOUT rewriting the string: Hadoop preserves
    // leading/trailing whitespace in a file name and so must we, or `COPY INTO … FROM '/tmp/x '`
    // silently reads `/tmp/x`.
    Option(filePath).filter(_.trim.nonEmpty).flatMap { raw =>
      scheme(raw) match {
        case None             => Some(Paths.get(raw))
        case Some(FileScheme) => fromFileUri(raw) // `scheme` already lowercased it
        case Some(_)          => None // remote scheme → Hadoop
      }
    }

  private def fromFileUri(raw: String): Option[Path] = {
    // `new URI` percent-decodes correctly (UTF-8, and unlike URLDecoder it does not turn '+' into a
    // space) but rejects unencoded characters such as a literal space. When it rejects the input,
    // the user did not percent-encode, so the remainder is already the literal path.
    //
    // A query or fragment disqualifies the URI reading. Hadoop's `Path(String)` takes "the rest of
    // the string" as the path — "query & fragment not supported" — so `?` and `#` are ordinary file
    // name characters to it (verified: `new Path("file:///a/report#1.jsonl").toUri.getPath` is
    // `/a/report#1.jsonl`). Using `u.getPath` there would truncate to `/a/report` and silently read
    // a DIFFERENT file. Fall through to the literal split, which keeps them.
    val (authority, path) = Try(new URI(raw)) match {
      case Success(u)
          if u.getPath != null && u.getPath.nonEmpty &&
            u.getQuery == null && u.getFragment == null =>
        (Option(u.getAuthority).getOrElse(""), u.getPath)
      case _ =>
        // Also the branch for an OPAQUE `file:` URI (`file:relative/x.jsonl`), where `getPath` is
        // null. Opaque URIs are taken literally — they are not percent-decoded.
        splitLiteral(raw.substring(FileScheme.length + 1))
    }

    // Only an empty authority or `localhost` denotes this machine. Any other host keeps the
    // pre-existing Hadoop behaviour rather than silently reinterpreting it as a local path.
    if (!LocalAuthorities.contains(authority.toLowerCase)) None
    else Option(path).filter(_.nonEmpty).map(p => Paths.get(stripDriveSlash(p, onWindows)))
  }

  /** Splits `//authority/path`, `///path` or `/path` (the part after `file:`) without URI parsing.
    */
  private def splitLiteral(rest: String): (String, String) =
    if (rest.startsWith("//")) {
      val afterSlashes = rest.substring(2)
      afterSlashes.indexOf('/') match {
        case -1  => (afterSlashes, "")
        case idx => (afterSlashes.substring(0, idx), afterSlashes.substring(idx))
      }
    } else ("", rest)

  /** `file:///C:/data/x.jsonl` yields the URI path `/C:/data/x.jsonl`; Windows needs the leading
    * slash removed before `Paths.get` will accept it.
    *
    * Gated on the platform on purpose: on POSIX `/C:` is a perfectly legal directory name, and
    * stripping the slash there would turn an absolute path into a CWD-relative one.
    *
    * `windows` is a parameter rather than a direct read of [[onWindows]] so both branches are
    * unit-testable on either OS.
    */
  private[file] def stripDriveSlash(p: String, windows: Boolean): String =
    if (
      windows && p.length >= 3 && p.charAt(0) == '/' && p.charAt(2) == ':' &&
      Character.isLetter(p.charAt(1))
    ) p.substring(1)
    else p
}
