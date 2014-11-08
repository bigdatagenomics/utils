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
package org.bdgenomics.utils.statistics.mixtures

import breeze.stats.distributions.Poisson
import org.bdgenomics.utils.misc.{ MathUtils, SparkFunSuite }
import scala.math.max
import scala.util.Random

class PoissonMixtureModelSuite extends SparkFunSuite {

  override val properties = Map(("spark.kryo.referenceTracking" -> "false"))

  sparkTest("run the m step on a trivial example") {
    val rdd = sc.parallelize(Seq((Array(1.0, 0.0, 0.0), 1),
      (Array(0.0, 1.0, 0.0), 2),
      (Array(0.0, 0.0, 1.0), 3)))

    val (newDistributions, weights) = PoissonMixtureModel.mStep(rdd,
      Array(Poisson(1.0),
        Poisson(2.0),
        Poisson(3.0)))

    assert(newDistributions.length === 3)
    assert(weights.length === 3)
    assert(MathUtils.fpEquals(weights.sum, 1.0))
    assert(weights.forall(MathUtils.fpEquals(_, 0.3333333333333333)))
    assert(MathUtils.fpEquals(newDistributions(0).mean, 1.0))
    assert(MathUtils.fpEquals(newDistributions(1).mean, 2.0))
    assert(MathUtils.fpEquals(newDistributions(2).mean, 3.0))
  }

  sparkTest("fit a poisson to several thousand points") {
    // give a random seed to avoid non-determinism between different runs                                                                                                                                     
    val rv = new Random(4321L)

    // build values for an rdd
    val randomValues5 = (0 until 5000).map(i => max(0.0, 5.0 + rv.nextGaussian).toInt)
    val randomValues50 = (0 until 5000).map(i => max(0.0, 50.0 + rv.nextGaussian).toInt)
    val rdd = sc.parallelize(randomValues5 ++ randomValues50)

    // fit two components
    val models = PoissonMixtureModel.train(rdd, 2, 25)

    assert(MathUtils.fpEquals(models(0).mean, 5.0, 1.0))
    assert(MathUtils.fpEquals(models(1).mean, 50.0, 1.0))
  }
}
