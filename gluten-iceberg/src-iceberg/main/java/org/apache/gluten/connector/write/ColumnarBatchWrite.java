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
package org.apache.gluten.connector.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

abstract public class ColumnarBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        throw new UnsupportedOperationException();
    }

    public ColumnarDataWriterFactory createColumnarBatchWriterFactory(PhysicalWriteInfo info) {
        throw new UnsupportedOperationException();
    }

//    private abstract class BaseBatchWrite implements ColumnarBatchWrite {
//        private BaseBatchWrite() {
//        }
//
//        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
//            return SparkWrite.this.createWriterFactory();
//        }
//
//        public boolean useCommitCoordinator() {
//            return false;
//        }
//
//        public void abort(WriterCommitMessage[] messages) {
//            SparkWrite.this.abort(messages);
//        }
//
//        public String toString() {
//            return String.format("IcebergBatchWrite(table=%s, format=%s)", SparkWrite.this.table, SparkWrite.this.format);
//        }
//    }
}
