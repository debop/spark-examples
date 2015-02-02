package learningsparkexamples

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.slf4j.LoggerFactory

/**
 * AbstractSparkFunSuite
 * @author sunghyouk.bae@gmail.com
 */
abstract class AbstractSparkFunSuite extends FunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  protected val log = LoggerFactory.getLogger(getClass)


  protected def deleteDirectory(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      log.debug(s"delete directory. path=$path")
      FileUtils.deleteDirectory(dir)
    }
  }

}
