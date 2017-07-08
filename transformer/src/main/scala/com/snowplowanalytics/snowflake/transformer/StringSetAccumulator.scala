/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

import StringSetAccumulator._

/**
 * Spark accumulator. Safe to use in transformations as add is idempotent
 */
class StringSetAccumulator extends AccumulatorV2[KeyAccum, KeyAccum] {

  private val accum = mutable.Set.empty[String]

  def merge(other: AccumulatorV2[KeyAccum, KeyAccum]): Unit = other match {
    case o: StringSetAccumulator => accum ++= o.accum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def isZero: Boolean = accum.isEmpty

  def copy(): AccumulatorV2[KeyAccum, KeyAccum] = {
    val newAcc = new StringSetAccumulator
    accum.synchronized {
      newAcc.accum ++= accum
    }
    newAcc
  }

  def value = accum

  def add(keys: KeyAccum): Unit = {
    accum ++= keys
  }

  def add(keys: Set[String]): Unit = {
    val mutableSet = mutable.Set(keys.toList: _*)
    add(mutableSet)
  }

  def reset(): Unit = {
    accum.clear()
  }
}


object StringSetAccumulator {
  type KeyAccum = mutable.Set[String]
}
