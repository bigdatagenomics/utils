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
package org.bdgenomics.utils.instrumentation

import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.utils.misc.SparkFunSuite

import scala.collection.JavaConversions._

class InstrumentedOrderedRDDFunctionsSuite extends SparkFunSuite {

  sparkBefore("Before") {
    Metrics.initialize(sc)
  }

  sparkAfter("After") {
    Metrics.stopRecording()
  }

  sparkTest("Nested timings are not recorded for sortByKey operation") {

    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2).instrument().keyBy(e => e).sortByKey()
    assert(rdd.count() === 5)

    Metrics.Recorder.value.foreach(recorder => {
      val timerMap = recorder.accumulable.value.timerMap
      // We should have timings for 4 timers: keyBy, keyBy's function call, sortByKey, and count,
      // but not for any nested operations within sortByKey
      val timerNames = timerMap.keys().map(_.timerName).toSet
      assert(timerNames.size === 4)
      assertContains(timerNames, "keyBy")
      assertContains(timerNames, "sortByKey")
      assertContains(timerNames, "function call")
      assertContains(timerNames, "count")
    })

  }

  private def assertContains(names: Set[String], nameStartsWith: String) = {
    assert(names.count(_.startsWith(nameStartsWith)) === 1,
      "Timer names [" + names + "] did not contain [" + nameStartsWith + "]")
  }

}
