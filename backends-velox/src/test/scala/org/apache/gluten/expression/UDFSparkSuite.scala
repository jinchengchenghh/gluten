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
package org.apache.gluten.expression

import org.apache.gluten.execution.{SparkPartialProjectColumnarExec, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.functions.udf

import java.io.File

class UDFSparkSuite extends WholeStageTransformerSuite {
  disableFallbackCheck
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
      .set("spark.gluten.sql.debug", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val table = "lineitem"
    val tableDir = getClass.getResource(resourcePath).getFile
    val tablePath = new File(tableDir, table).getAbsolutePath
    val tableDF = spark.read.format(fileFormat).load(tablePath)
    tableDF.createOrReplaceTempView(table)

    val plusOne = udf((x: Int) => x + 1)
    spark.udf.register("plus_one", plusOne)
    val plusOne2 = udf((x: Long) => x + 1)
    spark.udf.register("plus_one", plusOne2)
    val noArgument = udf(() => 15)
    spark.udf.register("no_argument", noArgument)

  }

  test("test plus_one") {
    runQueryAndCompare("SELECT sum(plus_one(cast(l_orderkey as long))) from lineitem") {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  test("test plus_one with column used twice") {
    runQueryAndCompare(
      "SELECT sum(plus_one(cast(l_orderkey as long)) + hash(l_orderkey)) from lineitem") {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  test("test plus_one without cast") {
    runQueryAndCompare("SELECT sum(plus_one(l_orderkey) + hash(l_orderkey)) from lineitem") {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  test("test plus_one with many columns") {
    runQueryAndCompare(
      "SELECT sum(plus_one(cast(l_orderkey as long)) + hash(l_partkey)) from lineitem") {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  test("test plus_one with many columns in project") {
    runQueryAndCompare("SELECT floor(cast(l_orderkey as long)), hash(l_partkey) from lineitem") {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  test("test function no argument") {
    runQueryAndCompare("""SELECT no_argument(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }

  // Wait to fix, may don't get file name in Spark
  ignore("test function input_file_name") {
    runQueryAndCompare("""SELECT input_file_name(), l_orderkey
                         | from lineitem limit 100""".stripMargin) {
      checkGlutenOperatorMatch[SparkPartialProjectColumnarExec]
    }
  }
}