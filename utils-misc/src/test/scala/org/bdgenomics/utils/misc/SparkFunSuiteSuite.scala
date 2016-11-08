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

import scala.io.Source

class SparkFunSuiteSuite extends SparkFunSuite {

  sparkTest("a simple test using spark") {
    val rdd = sc.parallelize(Seq(0, 1, 2, 3, 4))

    assert(rdd.count === 5)
    assert(rdd.reduce(_ + _) === 10)
  }

  test("copying a resource should pass equivalence testing") {
    val originalFile = testFile("test.txt")
    val copiedFile = copyResource("test.txt")

    checkFiles(originalFile, copiedFile)
  }

  test("load a resource file") {
    assert(Source.fromFile(resourceUrl("test.txt").toURI)
      .getLines
      .mkString(" ") === "This is a test of the emergency resource copying system.")
  }
}
