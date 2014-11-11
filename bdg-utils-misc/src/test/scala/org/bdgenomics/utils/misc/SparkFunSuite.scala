/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.utils.misc

import org.scalatest.{ Tag, BeforeAndAfter, FunSuite }
import org.apache.spark.{ SparkConf, SparkContext }
import java.net.ServerSocket
import org.apache.log4j.Level

trait SparkFunSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _
  val appName: String = "bdg-utils"
  val master: String = "local[4]"
  val properties: Map[String, String] = Map()

  def setupSparkContext(sparkName: String) {
    // Silence the Spark logs if requested
    synchronized {
      // Find an unused port
      val s = new ServerSocket(0)
      val driverPort = s.getLocalPort
      s.close()
      val conf = new SparkConf(false)
        .setAppName(appName + ": " + sparkName)
        .setMaster(master)
        .set("spark.driver.port", driverPort.toString)

      properties.foreach(kv => conf.set(kv._1, kv._2))

      sc = new SparkContext(conf)
    }
  }

  def teardownSparkContext() {
    // Stop the context
    sc.stop()
    sc = null
  }

  def sparkBefore(beforeName: String)(body: => Unit) {
    before {
      setupSparkContext(beforeName)
      try {
        // Run the before block
        body
      } finally {
        teardownSparkContext()
      }
    }
  }

  def sparkAfter(beforeName: String)(body: => Unit) {
    after {
      setupSparkContext(beforeName)
      try {
        // Run the after block
        body
      } finally {
        teardownSparkContext()
      }
    }
  }

  def sparkTest(name: String, tags: Tag*)(body: => Unit) {
    test(name, SparkTest +: tags: _*) {
      setupSparkContext(name)
      try {
        // Run the test
        body
      } finally {
        teardownSparkContext()
      }
    }
  }

  /**
   * Finds the full path of a "test file," usually in the src/test/resources directory.
   * @param name The path of the file w/r/t src/test/resources
   * @return The absolute path of the file
   * @throws IllegalArgumentException if the file doesn't exist
   */
  def testFile(name: String): String = {
    val url = ClassLoader.getSystemClassLoader.getResource(name)
    if (url == null) { throw new IllegalArgumentException("Couldn't find resource \"%s\"".format(name)) }
    url.getFile
  }
}

