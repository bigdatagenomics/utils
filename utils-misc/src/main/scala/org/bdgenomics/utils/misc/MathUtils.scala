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

import scala.annotation.tailrec
import scala.math.{ abs, log, max }

object MathUtils extends Serializable {

  /**
   * Recursively computes the factorial of an integer.
   *
   * @param i Value to compute factorial of.
   * @param v Current running value.
   * @return Returns the factorial.
   */
  @tailrec private def factorial(i: Int, v: Int): Int = {
    if (i <= 1) {
      max(1, v)
    } else {
      factorial(i - 1, i * v)
    }
  }

  /**
   * Recursively computes the factorial of an integer.
   *
   * @param i Value to compute factorial of.
   * @return Returns the factorial.
   */
  def factorial(i: Int): Int = {
    factorial(i - 1, i)
  }

  /**
   * Compute equality of two floating point numbers, given a tolerance.
   *
   * @param a Floating point number to compare.
   * @param b Floating point number to compare.
   * @param tol Tolerance to apply for checking equality.
   * @return Returns true if the two numbers are within `tol` of each other.
   *         Default value is 1e-6.
   */
  def fpEquals(a: Double, b: Double, tol: Double = 1e-6): Boolean = {
    abs(a - b) <= tol
  }

  /**
   * Aggregation function for arrays which does not allocate a new array.
   *
   * @param a1 Array to aggregate.
   * @param a2 Array to aggregate.
   * @return Overwrites a1 with the aggregated value, and returns a1.
   */
  def aggregateArray(a1: Array[Double], a2: Array[Double]): Array[Double] = {
    a1.indices.foreach(i => a1(i) += a2(i))
    a1
  }

  /**
   * Log function which never returns -infinity or NaN
   *
   * @param v Value to take log of.
   * @param floor Floor for return value to take on. Default is -1e5.
   * @return Returns either the log of the input value, or the floor, whichever is higher.
   */
  def safeLog(v: Double, floor: Double = -100000.0): Double = {
    val l = log(v)
    if (l.isNaN || l.isInfinite) {
      floor
    } else {
      l
    }
  }

  /**
   * Computes the additive softmax of an array in place.
   *
   * @param a Array to normalize. Normalizes array in place.
   */
  def softmax(a: Array[Double]) {
    val norm = a.sum

    a.indices.foreach(i => a(i) /= norm)
  }

  /**
   * Multiplies an array by a scalar.
   *
   * @param s Scalar to multiply against array.
   * @param a Array to multiply. Multiplies array in place.
   */
  def scalarArrayMultiply(s: Double, a: Array[Double]) {
    a.indices.foreach(i => a(i) *= s)
  }
}
