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

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.bdgenomics.utils.interval.array._
import scala.reflect.ClassTag

class IntervalRDD[K <: Interval[K]: ClassTag, V: ClassTag](
  /** The underlying representation of the IndexedRDD as an RDD of partitions. */
  private val partitionsRDD: RDD[IntervalPartition[K, V]])
    extends RDD[(K, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined, "Partitioner for underlying RDD must be defined")

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Provides the `RDD[(K, V)]` equivalent output.
   *
   * @param part Partition
   * @param context TaskContext
   * @return Iterator of (K, V) pairs
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    // This needs to be present to compile
    null
  }

  /**
   * Converts to normal RDD of (K, V) pairs
   *
   * @return RDD of (K, V) pairs
   */
  def toRDD: RDD[(K, V)] = {
    partitionsRDD.flatMap(_.get)
  }

  /**
   * Repartitions underlying data in IntervalRDD.
   *
   * @param partitioner new Partitioner to partition data by
   * @return repartitioned IntervalRDD
   */
  def partitionBy(partitioner: Partitioner): IntervalRDD[K, V] = {
    IntervalRDD(partitionsRDD.flatMap(_.get()).partitionBy(partitioner))
  }

  /**
   * Resets RDD name
   * @param _name RDD name
   * @return
   */
  override def setName(_name: String): this.type = {
    partitionsRDD.setName(_name)
    this
  }

  /** Persists the interval partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }
  /** Unpersists the interval partitions using */
  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** The number of elements in the RDD. */
  override def count: Long = {
    partitionsRDD.map(_.getIntervalArray.length).reduce(_ + _)
  }

  /**
   * Gets first element in the RDD.
   *
   * @return first (K,V) pair
   */
  override def first: (K, V) = {
    partitionsRDD.filter(_.getIntervalArray.length > 0).first.getIntervalArray.collect()(0)
  }

  /**
   * Collects all elements in RDD into an array of (K, V) pairs.
   *
   * @return Array of (K, V) pairs
   */
  override def collect: Array[(K, V)] = partitionsRDD.flatMap(_.get).collect

  /**
   * Gets the values corresponding to the specified key, if any
   * Assume that we're only getting data that exists (if it doesn't exist,
   * would have been handled by upper LazyMaterialization layer
   *
   * @param interval Interval to collect overlapping data from
   * @return List of V records overlapping interval
   */
  def collect(interval: K): List[V] = {

    val results: Array[Array[V]] = {
      context.runJob(partitionsRDD, (context: TaskContext, partIter: Iterator[IntervalPartition[K, V]]) => {
        if (partIter.hasNext) {
          val intPart = partIter.next()
          intPart.filterByInterval(interval).getIntervalArray.collect.map(_._2)
        } else {
          Array[V]()
        }
      })
    }
    results.flatten.toList
  }

  /**
   * Filters IntervalRDD by interval and returns new IntervalRDD.
   *
   * @param r Interval to filter by
   * @return filtered IntervalRDD
   */
  def filterByInterval(r: K): IntervalRDD[K, V] = {
    mapIntervalPartitions(_.filterByInterval(r))
  }

  /**
   * Performs filtering given a predicate.
   *
   * @param pred Predicate to filter (K,V) pairs by
   * @return new IntervalRDD with filtered predicate
   */
  override def filter(pred: ((K, V)) => Boolean): IntervalRDD[K, V] = {
    this.mapIntervalPartitions(_.filter(pred))
  }

  /**
   * Maps each value, preserving the index.
   *
   * @param f: Function that maps values V to new type V2
   * @return new IntervalRDD with values V2
   */
  def mapValues[V2: ClassTag](f: V => V2): IntervalRDD[K, V2] = {
    this.mapIntervalPartitions(_.mapValues(f))
  }

  /**
   * Maps interval partitions in partitionsRDD.
   *
   * @param f mapping function for interval partitions
   * @tparam K2 key type extending Interval
   * @tparam V2 data type
   * @return new IntervalRDD of type [K2, V2]
   */
  def mapIntervalPartitions[K2 <: Interval[K2]: ClassTag, V2: ClassTag](
    f: (IntervalPartition[K, V]) => IntervalPartition[K2, V2]): IntervalRDD[K2, V2] = {

    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    new IntervalRDD(newPartitionsRDD)
  }

  /**
   * Generates new IntervalRDD from an RDD of IntervalPartitions.
   *
   * @param partitionsRDD RDD of IntervalPartitions
   * @tparam K2 Interval type for IntervalPartitions
   * @tparam V2 Value type for IntervalPartitions
   * @return new IntervalRDD
   */
  private def withPartitionsRDD[K2 <: Interval[K2]: ClassTag, V2: ClassTag](
    partitionsRDD: RDD[IntervalPartition[K2, V2]]): IntervalRDD[K2, V2] = {
    new IntervalRDD(partitionsRDD)
  }

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD.
   *
   * @param elems Array of (K,V) pairs
   * @return IntervalRDD with inserted elems
   */
  def multiput(elems: Array[(K, V)], isSorted: Boolean = false): IntervalRDD[K, V] = {
    multiputRDD(context.parallelize(elems.toSeq), isSorted)
  }

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD.
   *
   * @param elem (K,V) pair
   * @return IntervalRDD with inserted elem
   */
  def put(elem: (K, V)): IntervalRDD[K, V] = multiput(Array(elem), true)

  /**
   * Unconditionally updates the specified keys to have the specified value. Returns a new IntervalRDD.
   *
   * @param elems RDD of (K,V) pairs
   * @param sorted Boolean that determines whether elems is sorted
   * @return IntervalRDD with inserted elems
   */
  def multiputRDD(elems: RDD[(K, V)], sorted: Boolean = false): IntervalRDD[K, V] = {
    val partitioned = elems.partitionBy(partitioner.get)

    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter.toIterable, sorted)),
      preservesPartitioning = true)

    // merge the new partitions with existing partitions
    val merger = new PartitionMerger[K, V]()
    val newPartitionsRDD = partitionsRDD.zipPartitions(convertedPartitions, true)((aiter, biter) => merger(aiter, biter))
    new IntervalRDD(newPartitionsRDD)
  }
}

class PartitionMerger[K <: Interval[K], V: ClassTag]() extends Serializable {

  /**
   * Merges two Iterators of IntervalPartitions together.
   *
   * @param thisIter First iterator of IntervalPartitions
   * @param otherIter Iterator of IntervalPartitions to merge
   * @return Merged iterator of IntervalPartitions
   */
  def apply(thisIter: Iterator[IntervalPartition[K, V]], otherIter: Iterator[IntervalPartition[K, V]]): Iterator[IntervalPartition[K, V]] = {
    val thisPart = thisIter.next
    val otherPart = otherIter.next
    Iterator(thisPart.mergePartitions(otherPart))
  }
}

object IntervalRDD {

  /**
   *  Constructs an IntervalRDD from a set of interval, V tuples.
   *
   * @param elems RDD of (K,V) pairs
   * @param sorted Determines whether RDD has been sorted
   * @tparam K
   * @tparam V
   * @return new IntervalRDD
   */
  def apply[K <: Interval[K]: ClassTag, V: ClassTag](elems: RDD[(K, V)], sorted: Boolean = false): IntervalRDD[K, V] = {
    val partitioned =
      if (elems.partitioner.isDefined) elems
      else {
        elems.partitionBy(new HashPartitioner(elems.partitions.size))
      }
    val convertedPartitions: RDD[IntervalPartition[K, V]] = partitioned.mapPartitions[IntervalPartition[K, V]](
      iter => Iterator(IntervalPartition(iter.toIterable, sorted)),
      preservesPartitioning = true)

    new IntervalRDD(convertedPartitions)
  }

}
