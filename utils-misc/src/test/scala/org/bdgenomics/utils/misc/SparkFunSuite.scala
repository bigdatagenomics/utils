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

import com.google.common.io.Resources
import java.io.File
import java.net.{ URI, URL }
import java.nio.file.{ Files, Path }
import org.scalatest.{ Tag, BeforeAndAfter, FunSuite }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SparkSession, SQLContext }
import org.apache.log4j.Level
import scala.io.Source

trait SparkFunSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _
  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  val appName: String = "bdg-utils"
  val master: String = "local[4]"
  val properties: Map[String, String] = Map()

  final val testPortRange: Range = 50000 until 60000
  final val rnd: scala.util.Random = new scala.util.Random

  private def nextTestPort = {
    testPortRange.start + rnd.nextInt(testPortRange.length)
  }

  def setupSparkContext(sparkName: String) {
    var maybeSpark: Option[SparkSession] = None

    while (maybeSpark.isEmpty) {
      val conf = new SparkConf(false)
        .setAppName(appName + ": " + sparkName)
        .setMaster(master)
        .set("spark.driver.port", nextTestPort.toString)
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")

      properties.foreach(kv => conf.set(kv._1, kv._2))
      try {
        maybeSpark = Some(SparkSession.builder()
          .config(conf)
          .getOrCreate())
      } catch {
        // Retry in the case of a bind exception
        case bindException: java.net.BindException =>
        // Throw all other exceptions
        case e: Exception                          => throw e
      }
    }
    spark = maybeSpark.get
    sqlContext = spark.sqlContext
    sc = spark.sparkContext
  }

  def teardownSparkContext() {
    // Stop the context
    spark.stop()
    spark = null
    sqlContext = null
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
   *
   * @param name The path of the file w/r/t src/test/resources
   * @return The absolute path of the file
   * @throws IllegalArgumentException if the file doesn't exist
   */
  def testFile(name: String): String = {
    val url = resourceUrl(name)
    if (url == null) {
      throw new IllegalArgumentException("Couldn't find resource \"%s\"".format(name))
    }
    url.getFile
  }

  /**
   * Finds the URL of a "test file," usually in the src/test/resources directory.
   *
   * @param path The path of the file inside src/test/resources
   * @return The URL of the file
   */
  def resourceUrl(path: String): URL = {
    ClassLoader.getSystemClassLoader.getResource(path)
  }

  /**
   * Makes a temporary directory, and returns a path to a file in that directory.
   *
   * @param path The name of the file in a directory.
   * @return Returns the absolute path to a temp file.
   */
  def tmpFile(path: String): String = {
    Files.createTempDirectory("").toAbsolutePath.toString + "/" + path
  }

  /**
   * Creates temporary file for saveAsParquet().
   * This helper function is required because createTempFile creates an actual file, not
   * just a name.  Whereas, this returns the name of something that could be mkdir'ed, in the
   * same location as createTempFile() uses, so therefore the returned path from this method
   * should be suitable for saveAsParquet().
   *
   * @param suffix Suffix of file being created
   * @return Absolute path to temporary file location
   */
  def tmpLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("TempSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  /**
   * Compares whether two files contain exactly the same text.
   *
   * Will fire an assert if the files differ.
   *
   * @param expectedPath The expected data to compare against.
   * @param actualPath The data to compare.
   */
  def checkFiles(expectedPath: String, actualPath: String) {
    val actualFile = Source.fromFile(actualPath)
    val actualLines = actualFile.getLines.toList

    val expectedFile = Source.fromFile(expectedPath)
    val expectedLines = expectedFile.getLines.toList

    assert(expectedLines.size === actualLines.size)
    expectedLines.zip(actualLines).zipWithIndex.foreach {
      case ((expected, actual), idx) =>
        assert(
          expected == actual,
          s"Line ${idx + 1} differs.\nExpected:\n${expectedLines.mkString("\n")}\n\nActual:\n${actualLines.mkString("\n")}")
    }
  }

  /**
   * Copies a resource file to a temp path.
   *
   * @param name The name of the resource to copy.
   * @return Returns the path this resource has been copied to.
   */
  def copyResourcePath(name: String): Path = {
    val tempFile = Files.createTempFile(name, "." + name.split('.').tail.mkString("."))
    Files.write(tempFile, Resources.toByteArray(getClass().getResource("/" + name)))
  }

  /**
   * Copies a resource file to a temp path.
   *
   * @param name The name of the resource to copy.
   * @return Returns the string path this resource has been copied to.
   */
  def copyResource(name: String): String = {
    copyResourcePath(name).toString
  }

  /**
   * Copies a resource file to a temp path.
   *
   * @param name The name of the resource to copy.
   * @return Returns the URI of the location this resource has been copied to.
   */
  def copyResourceUri(name: String): URI = {
    copyResourcePath(name).toUri
  }
}

