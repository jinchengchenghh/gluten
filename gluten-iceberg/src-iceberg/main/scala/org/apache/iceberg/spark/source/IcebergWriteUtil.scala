package org.apache.iceberg.spark.source

import org.apache.iceberg.spark.source.SparkWrite.TaskCommit
import org.apache.iceberg.{DataFile, FileFormat, Schema, Table}
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}

object IcebergWriteUtil {
  def isBatchAppend(write: BatchWrite): Boolean = {
    write.getClass.getSimpleName.equals("BatchAppend")
  }

  def hasUnsupportedDataType(write: Write): Boolean = {
    getWriteSchema(write).columns()
    // get the private field writeSchema
    false
  }

  private def getWriteSchema(write: Write): Schema = {
    val field = classOf[SparkWrite].getDeclaredField("writeSchema")
    field.setAccessible(true)
    field.get(write).asInstanceOf[Schema]
  }

  def getTable(write: Write): Table = {
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    field.get(write).asInstanceOf[Table]
  }

  def getSparkWrite(write: BatchWrite): SparkWrite = {
    // Access the enclosing SparkWrite instance from BatchAppend
    val outerInstanceField = write.getClass.getDeclaredField("this$0")
    outerInstanceField.setAccessible(true)
    outerInstanceField.get(write).asInstanceOf[SparkWrite]
  }

  def getFileFormat(write: BatchWrite): FileFormat = {
    val sparkWrite = getSparkWrite(write)
    val field = classOf[SparkWrite].getDeclaredField("format")
    field.setAccessible(true)
    field.get(sparkWrite).asInstanceOf[FileFormat]
  }

  def getDirectory(write: BatchWrite): String = {
    val sparkWrite = getSparkWrite(write)
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    getTable(sparkWrite).locationProvider().newDataLocation("")
  }

  // Similar to the UnpartitionedDataWriter#commit
  def commitDataFiles(
                      dataFiles: Array[DataFile]): WriterCommitMessage = {
    val commit = new TaskCommit(dataFiles)
    commit.reportOutputMetrics()
    commit
  }

}
