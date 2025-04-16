package org.apache.iceberg.spark.source

import org.apache.iceberg.spark.source.SparkWrite.TaskCommit
import org.apache.iceberg.{DataFile, FileFormat, Schema}
import org.apache.spark.sql.connector.write.{Write, WriterCommitMessage}

object IcebergWriteUtil {
  def isSparkWrite(write: Write): Boolean = {
    write.isInstanceOf[SparkWrite]
  }

  def supportedDataType(write: Write): Boolean = {
    write.asInstanceOf[SparkWrite]
    // get the private field writeSchema
    true
  }

  def getWriteSchema(write: Write): Schema = {
    write.asInstanceOf[SparkWrite]
    null
  }

  def getFileFormat(write: Write): FileFormat = {
    FileFormat.PARQUET
  }

  // Similar to the UnpartitionedDataWriter#commit
  def commitDataFiles(
                      dataFiles: Array[DataFile]): WriterCommitMessage = {
    val commit = new TaskCommit(dataFiles)
    commit.reportOutputMetrics()
    commit
  }

}
