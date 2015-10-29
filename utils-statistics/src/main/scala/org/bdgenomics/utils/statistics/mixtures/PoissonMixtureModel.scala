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
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.MathUtils

object PoissonMixtureModel extends DiscreteKMeansMixtureModel[Poisson] {

  /**
   * Initializes the distributions, given a mean and a sigma.
   *
   * @param mean Mean for an initial distribution.
   * @param sigma Standard deviation for an initial distribution.
   * @return Returns a distribution.
   */
  protected def initializeDistribution(mean: Double, sigma: Double): Poisson = {
    new Poisson(mean)(null)
  }

  /**
   * The maximization stage for the EM algorithm. Must be implemented by user.
   *
   * @param rdd An RDD containing an array of weights and the value for the point.
   * @param distributions The distributions fit by the last iteration of the EM algorithm.
   * @return Returns a tuple containing an array of updated distributions as well as
   *         an array of distribution weights (should sum to one).
   */
  private[mixtures] def mStep(rdd: RDD[(Array[Double], Int)],
                              distributions: Array[Poisson]): (Array[Poisson], Array[Double]) = {
    rdd.cache()

    // aggregate the distribution weights
    val weights = rdd.map(p => {
      val a = p._1

      if (a.forall(v => !v.isNaN && !v.isInfinite)) {
        a
      } else {
        Array.fill(a.length) { 0.0 }
      }
    }).aggregate(Array.fill(distributions.length) { 0.0 })(MathUtils.aggregateArray, MathUtils.aggregateArray)

    // map to acquire the distribution means and aggregate
    val means = rdd.map(p => {
      val (a, v) = p

      // calculate mean contributions for this element
      MathUtils.scalarArrayMultiply(v, a)

      if (a.forall(v => !v.isNaN && !v.isInfinite)) {
        a
      } else {
        Array.fill(a.length) { 0.0 }
      }
    }).aggregate(Array.fill(distributions.length) { 0.0 })(MathUtils.aggregateArray, MathUtils.aggregateArray)

    // unpersist the rdd, as we're done using it
    rdd.unpersist()

    // normalize means
    (0 until means.length).foreach(i => means(i) /= weights(i))

    // softmax weights
    MathUtils.softmax(weights)

    (means.map(lambda => new Poisson(lambda)(null)), weights)
  }
}
