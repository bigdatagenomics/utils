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

package org.bdgenomics.utils.interval.rdd

import org.bdgenomics.utils.interval.array._
import org.bdgenomics.utils.misc.Logging
import scala.math.max
import scala.reflect.ClassTag

protected class IntervalPartition[K <: Interval[K]: ClassTag, V: ClassTag](protected val array: IntervalArray[K, V])
    extends Serializable with Logging {

  def getIntervalArray(): IntervalArray[K, V] = {
    array
  }

  protected def withMap(map: IntervalArray[K, V]): IntervalPartition[K, V] = {
    new IntervalPartition(map)
  }

  /**
   * Gets all (k,v) data from partition within the specificed interval
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def get(r: K): Iterable[(K, V)] = {
    array.get(r)
  }

  /**
   * Gets all (k,v) data from partition
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def get(): Iterable[(K, V)] = {
    array.collect.toIterable
  }

  private[utils] def filterByInterval(r: K): IntervalPartition[K, V] = {
    IntervalPartition(array.get(r))
  }

  /**
   * Return a new IntervalPartition filtered by some predicate
   */
  def filter(pred: ((K, V)) => Boolean): IntervalPartition[K, V] = {
    this.withMap(array.filter(pred))
  }

  /**
   * Applies a map function over the interval tree
   */
  def mapValues[V2: ClassTag](f: V => V2): IntervalPartition[K, V2] = {
    val retTree: IntervalArray[K, V2] = array.mapValues(f)
    new IntervalPartition(retTree)
  }

  /**
   * Puts all (k,v) data from partition within the specificed interval
   *
   * @return IntervalPartition with new data
   */
  def multiput(kvs: Iterator[(K, V)]): IntervalPartition[K, V] = {
    this.withMap(array.insert(kvs))
  }

  /**
   * Puts all (k,v) data from partition within the specificed interval
   *
   * @return IntervalPartition with new data
   */
  def put(kv: (K, V)): IntervalPartition[K, V] = {
    multiput(Iterator(kv))
  }

  /**
   * Merges trees of this partition with a specified partition
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def mergePartitions(p: IntervalPartition[K, V]): IntervalPartition[K, V] = {
    this.withMap(array.insert(p.getIntervalArray().collect.toIterator))
  }

}

private[rdd] object IntervalPartition {

  def apply[K <: Interval[K]: ClassTag, K2 <: Interval[K2]: ClassTag, V: ClassTag](iter: Iterable[(K, V)]): IntervalPartition[K, V] = {
    val array = iter.toArray
    val map = IntervalArray(array, array.map(_._1.width).fold(0L)(max(_, _)), sorted = false)
    new IntervalPartition(map)
  }
}
