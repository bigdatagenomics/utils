package org.bdgenomics.intervaltree

/**
 * An interval is a region on a coordinate space that has a defined width. This
 * can be used to express a region of a genome, a transcript, a gene, etc.
 */
trait Interval {

  /**
   * @return The start of this interval.
   */
  def start: Long

  /**
   * @return The end of this interval.
   */
  def end: Long

  /**
   * A width is the key property of an interval, which can represent a genomic
   * region, a transcript, a gene, etc.
   *
   * @return The width of this interval.
   */
  def width: Long = end - start

}

/*
 * Basic implementation of Interval
 */
case class Region(start: Long, end: Long) extends Interval

