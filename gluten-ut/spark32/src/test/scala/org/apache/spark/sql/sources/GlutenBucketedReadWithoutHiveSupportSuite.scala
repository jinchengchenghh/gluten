/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources

import scala.util.Random

import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.internal.SQLConf

class GlutenBucketedReadWithoutHiveSupportSuite extends BucketedReadWithoutHiveSupportSuite
  with GlutenSQLTestsTrait {
  import testImplicits._

  private val maxI = 5
  private val maxJ = 13
  private lazy val df = (0 until 50).map(i => (i % maxI, i % maxJ, i.toString)).toDF("i", "j", "k")

  test("gluten read bucketed data") {
    withTable("bucketed_table") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j", "k")
        .saveAsTable("bucketed_table")

      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {
        val bucketValue = Random.nextInt(maxI)
        val table = spark.table("bucketed_table").filter($"i" === bucketValue)
        val query = table.queryExecution
        val output = query.analyzed.output
        val rdd = query.toRdd

        assert(rdd.partitions.length == 8)

        val attrs = table.select("j", "k").queryExecution.analyzed.output
        val checkBucketId = rdd.mapPartitionsWithIndex((index, rows) => {
          val getBucketId = UnsafeProjection.create(
            HashPartitioning(attrs, 8).partitionIdExpression :: Nil,
            output)
          rows.map(row => getBucketId(row).getInt(0) -> index)
        })
        checkBucketId.collect().foreach(r => assert(r._1 == r._2))
      }
    }
  }
  }
