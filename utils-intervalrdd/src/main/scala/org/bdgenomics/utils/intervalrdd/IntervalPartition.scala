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

package org.bdgenomics.utils.intervalrdd

import org.bdgenomics.utils.intervaltree._
import org.bdgenomics.utils.misc.Logging
import scala.reflect.ClassTag

protected class IntervalPartition[K <: Interval, V: ClassTag](protected val iTree: IntervalTree[K, V]) extends Serializable with Logging {

  def this() {
    this(new IntervalTree[K, V]())
  }

  def getTree(): IntervalTree[K, V] = {
    iTree
  }

  protected def withMap(map: IntervalTree[K, V]): IntervalPartition[K, V] = {
    new IntervalPartition(map)
  }

  /**
   * Gets all (k,v) data from partition within the specificed interval
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def get(r: K): Iterator[(K, V)] = {
    iTree.search(r).filter(kv => intervalOverlap(r, kv._1))
  }

  /*
   * Helper function for overlap of intervals
   */
  def intervalOverlap(r1: K, r2: K): Boolean = {
    r1.start < r2.end && r1.end > r2.start
  }

  /**
   * Gets all (k,v) data from partition
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def get(): Iterator[(K, V)] = {
    iTree.get.toIterator
  }

  def filterByInterval(r: K): IntervalPartition[K, V] = {
    val i: Iterator[(K, V)] = iTree.search(r)
    IntervalPartition(i)
  }

  /**
   * Return a new IntervalPartition filtered by some predicate
   */
  def filter(pred: (K, V) => Boolean): IntervalPartition[K, V] = {
    this.withMap(iTree.filter(pred))
  }

  /**
   * Applies a map function over the interval tree
   */
  def mapValues[V2: ClassTag](f: V => V2): IntervalPartition[K, V2] = {
    val retTree: IntervalTree[K, V2] = iTree.mapValues(f)
    new IntervalPartition(retTree)
  }

  /**
   * Puts all (k,v) data from partition within the specificed interval
   *
   * @return IntervalPartition with new data
   */
  def multiput(r: K, vs: Iterator[V]): IntervalPartition[K, V] = {
    val newTree = iTree.snapshot()
    newTree.insert(r, vs)
    this.withMap(newTree)
  }

  /**
   * Puts all (k,v) data from partition within the specificed interval
   *
   * @return IntervalPartition with new data
   */
  def put(r: K, v: V): IntervalPartition[K, V] = {
    multiput(r, Iterator(v))
  }

  /**
   * Merges trees of this partition with a specified partition
   *
   * @return Iterator of searched interval and the corresponding (K,V) pairs
   */
  def mergePartitions(p: IntervalPartition[K, V]): IntervalPartition[K, V] = {
    val newTree = iTree.merge(p.getTree)
    this.withMap(newTree)
  }

}

private[intervalrdd] object IntervalPartition {

  def apply[K <: Interval, K2 <: Interval, V: ClassTag](iter: Iterator[(K, V)]): IntervalPartition[K, V] = {
    val map = new IntervalTree[K, V]()
    map.insert(iter)
    new IntervalPartition(map)
  }

}
