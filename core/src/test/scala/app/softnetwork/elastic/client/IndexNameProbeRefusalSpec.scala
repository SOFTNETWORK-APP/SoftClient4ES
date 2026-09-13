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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result.ElasticError
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

/** `validateIndexName` is the layer that owns the engine's answer to a BI tool's temp-table
  * capability probe, and these are the two things that answer must keep doing.
  *
  *   1. **It must keep refusing both probe families.** The SQL-92 family
  *      (`XT__..._Connect_CheckCreateTempTableCap`) carries NO forbidden character — its only
  *      disqualifier is its uppercase letters — so anything that lowercases a name on the `CREATE
  *      TABLE` / `DROP TABLE` routing path flips it from a deliberate `400` to a real index created
  *      in the customer's cluster on every Tableau connect. `Table.indexName` is one `toLowerCase`
  *      away from exactly that and nothing else in the repo would notice, which is why the guard is
  *      a behavioural assertion about the probe NAMES rather than a comment. 2. **It must name the
  *      disqualifier that is actually specific.** The MySQL family (`#Tableau_..._1_Connect_Chec`)
  *      violates two rules at once, and before 0.23.0 the rule order made it answer *"Index name
  *      must be lowercase"* — a case complaint about a name whose `#` keeps it illegal however it
  *      is cased.
  *
  * The probe names are the verbatim statements Tableau Desktop 2026.2.2 emitted through the JDBC
  * driver, captured in the Epic 19 BI corpus.
  */
class IndexNameProbeRefusalSpec extends AnyFlatSpec with Matchers {

  /** `validateIndexName` is `protected`, so the subject is reached the way every production caller
    * reaches it: through a concrete implementor.
    */
  private object Subject extends ElasticClientHelpers {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    def validate(index: String, pattern: Boolean = false): Option[ElasticError] =
      validateIndexName(index, pattern)
  }

  /** Tableau MySQL dialect — `CREATE TABLE` / `DROP TABLE IF EXISTS` on a `#`-prefixed name.
    * Uppercase AND `#`: illegal twice over, and illegal in a way no case change repairs.
    */
  private val MySqlProbeNames = Seq(
    "#Tableau___A___A______F_______B__B____D__C___F__1_Connect_Chec",
    "#Tableau_______C____C_FE___A__A_BB__EF___F_FFDF_1_Connect_Chec",
    "#Tableau____F_AE__A___F_____F__FBC____E_BD___E__1_Connect_Chec",
    "#Tableau_____FAC_D__ADE________________DA_B_A_A_1_Connect_Chec",
    "#Tableau____C__F_______F__F____A_F_C__E_E___DBE_1_Connect_Chec",
    "#Tableau___B_A____D___FC__BF___E____F__CED_CCF__1_Connect_Chec",
    "#Tableau____D________EA___BE___A___FF___A___E___1_Connect_Chec",
    "#Tableau________D_____A________________B_____D__1_Connect_Chec",
    "#Tableau____B___E_D_CFE___B_D_BB_D_D____C_B_D___1_Connect_Chec"
  )

  /** Tableau SQL-92 dialect — the `DROP TABLE "XT..."` half of the probe (its `CREATE` half is
    * refused earlier, by the grammar). Uppercase ONLY: legal the moment anything lowercases it.
    */
  private val Sql92ProbeNames = Seq(
    "XT__EEE____D_E_DA___CC____C__B_B__BFB____1_Connect_CheckCreateTempTableCap",
    "XT___A_E__B__B_C_______B_F_____AA___C_D__1_Connect_CheckCreateTempTableCap",
    "XT___DE_EA_A_________A_B_D___A__F_CBA__C_1_Connect_CheckCreateTempTableCap"
  )

  "A Tableau MySQL temp-table probe name" should "be refused" in {
    MySqlProbeNames.foreach { name =>
      withClue(s"[$name] ") { Subject.validate(name) should not be empty }
    }
  }

  it should "name the '#' rather than only the casing" in {
    MySqlProbeNames.foreach { name =>
      val message = Subject.validate(name).map(_.message).getOrElse("")
      withClue(s"[$name] -> $message ") {
        message should include("invalid characters")
        message should include("#")
        // Both rules are violated and both must be reported - the defect this pins was that the
        // casing rule SPOKE FOR the '#'.
        message should include("must be lowercase")
      }
    }
  }

  "A Tableau SQL-92 temp-table probe name" should "be refused" in {
    Sql92ProbeNames.foreach { name =>
      withClue(s"[$name] ") { Subject.validate(name) should not be empty }
    }
  }

  /** The falsifier for the test above: it passes for the RIGHT reason only while casing is the
    * whole of the disqualification. If a future change adds another rule these names also break,
    * "refused" would stay green for a reason that has nothing to do with the hazard.
    */
  it should "be refused on its CASING and nothing else, so a lowercase() anywhere would create it" in {
    Sql92ProbeNames.foreach { name =>
      val message = Subject.validate(name).map(_.message).getOrElse("")
      withClue(s"[$name] -> $message ") {
        message shouldBe "Index name must be lowercase"
        Subject.validate(name.toLowerCase) shouldBe None
      }
    }
  }

  "validateIndexName" should "report every violated rule in one message" in {
    val message = Subject.validate("#My Index").map(_.message).getOrElse("")
    message should include("invalid characters")
    message should include("must be lowercase")
    // Reported in specificity order: the named characters first.
    message.indexOf("invalid characters") should be < message.indexOf("must be lowercase")
  }

  it should "keep a single-rule message unchanged" in {
    Subject.validate("MyIndex").map(_.message) shouldBe Some("Index name must be lowercase")
    Subject.validate("-my-index").map(_.message) shouldBe Some(
      "Index name cannot start with '-', '_', or '+'"
    )
    Subject.validate(".").map(_.message) shouldBe Some("Index name cannot be '.' or '..'")
    Subject.validate("").map(_.message) shouldBe Some("Index name cannot be empty")
    Subject.validate("a" * 256).map(_.message) shouldBe Some(
      "Index name is too long (max 255 characters): 256"
    )
  }

  it should "name the characters it actually found, including the backslash" in {
    val message = Subject.validate("my\\index").map(_.message).getOrElse("")
    message should startWith("Index name contains invalid characters: \\")
    // The pre-0.23.0 list omitted the backslash the regex it guarded rejected.
    message should include("not allowed: \\")
  }

  it should "accept a legal name" in {
    Subject.validate("my-index") shouldBe None
    Subject.validate("my_index.2026") shouldBe None
    Subject.validate("a" * 255) shouldBe None
  }

  it should "treat '*' as legal in a pattern and illegal in a name" in {
    Subject.validate("my*index", pattern = true) shouldBe None
    Subject.validate("my*index").map(_.message).getOrElse("") should include("*")
    // ... and the forbidden list it prints must agree with the mode it was called in.
    Subject.validate("my#index", pattern = true).map(_.message).getOrElse("") should not include "*"
  }

  it should "derive the pattern set from the name set, minus the wildcard" in {
    ElasticClientHelpers.forbiddenIndexNameChars
      .diff(ElasticClientHelpers.forbiddenIndexPatternChars) shouldBe Seq('*')
  }

  "validateAliasName and validateTemplateName" should "relabel a multi-rule message throughout" in {
    // They rewrite "Index" -> "Alias"/"Template" in the message; a joined message carries the word
    // once per violated rule, so a single-replacement implementation would leak "Index".
    val alias = new ElasticClientHelpers {
      override protected def logger: Logger = LoggerFactory.getLogger(getClass)
      def validateAlias(name: String): Option[ElasticError] = validateAliasName(name)
      def validateTemplate(name: String): Option[ElasticError] = validateTemplateName(name)
    }
    val aliasMessage = alias.validateAlias("#My Alias").map(_.message).getOrElse("")
    aliasMessage should not include "Index"
    aliasMessage should include("Alias name contains invalid characters")
    aliasMessage should include("Alias name must be lowercase")

    val templateMessage = alias.validateTemplate("#My Template").map(_.message).getOrElse("")
    templateMessage should not include "Index"
    templateMessage should include("Template name contains invalid characters")
    templateMessage should include("Template name must be lowercase")
  }
}
