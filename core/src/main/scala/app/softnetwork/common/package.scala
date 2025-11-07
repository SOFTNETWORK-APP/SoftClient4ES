package app.softnetwork

import org.slf4j.Logger

import java.io.Closeable

package object common {

  trait ClientCompanion extends Closeable { _: { def logger: Logger } =>

    /** Check if client is initialized and connected
      */
    def isInitialized: Boolean

    /** Test connection
      * @return
      *   true if connection is successful
      */
    def testConnection(): Boolean
  }

}
