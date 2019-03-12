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
package org.bdgenomics.utils.cli

import java.io.{ StringWriter, PrintWriter }
import grizzled.slf4j.Logging
import org.apache.spark.{ SparkConf, SparkContext }
import org.bdgenomics.utils.instrumentation._

trait BDGCommandCompanion {
  val commandName: String
  val commandDescription: String

  def apply(cmdLine: Array[String]): BDGCommand

  // Make running an BDG command easier from an IDE
  def main(cmdLine: Array[String]) {
    apply(cmdLine).run()
  }
}

trait BDGCommand extends Runnable {
  val companion: BDGCommandCompanion
}

trait BDGSparkCommand[A <: Args4jBase] extends BDGCommand with Logging {
  protected val args: A

  def run(sc: SparkContext)

  def run() {
    val start = System.nanoTime()
    val conf = new SparkConf().setAppName(companion.commandName)
    if (conf.getOption("spark.master").isEmpty) {
      conf.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }
    val sc = new SparkContext(conf)
    val metricsListener = initializeMetrics(sc)
    val e = try {
      run(sc)
      None
    } catch {
      case e: Throwable => {
        System.err.println("Command body threw exception:\n%s".format(e))
        Some(e)
      }
    }
    val totalTime = System.nanoTime() - start
    printMetrics(totalTime, metricsListener)
    e.foreach(throw _)
  }

  def initializeMetrics(sc: SparkContext): Option[MetricsListener] = {
    if (args.printMetrics) {
      val metricsListener = new MetricsListener(new RecordedMetrics())
      sc.addSparkListener(metricsListener)
      Metrics.initialize(sc)
      Some(metricsListener)
    } else {
      // This avoids recording metrics if we have a recorder left over from previous use of this thread
      Metrics.stopRecording()
      None
    }
  }

  def printMetrics(totalTime: Long, metricsListener: Option[MetricsListener]) {
    info("Overall Duration: " + DurationFormatting.formatNanosecondDuration(totalTime))
    if (args.printMetrics && metricsListener.isDefined) {
      // Set the output buffer size to 4KB by default
      val stringWriter = new StringWriter()
      val out = new PrintWriter(stringWriter)
      out.println("Metrics:")
      out.println()
      Metrics.print(out, metricsListener.map(_.metrics.sparkMetrics.stageTimes))
      out.println()
      metricsListener.foreach(_.metrics.sparkMetrics.print(out))
      out.flush()
      info(stringWriter.getBuffer.toString)
    }
  }
}
