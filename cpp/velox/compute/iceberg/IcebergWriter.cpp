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

#include "IcebergWriter.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"

using namespace facebook::velox;
namespace {
std::shared_ptr<connector::hive::iceberg::IcebergInsertTableHandle>
  createIcebergInsertTableHandle(
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat,
      const std::vector<std::string>& partitionedBy = {}) {
    std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
        columnHandles;
    int32_t numPartitionColumns{0};

    std::vector<std::string> columnNames = outputRowType->names();
    std::vector<TypePtr> columnTypes = outputRowType->children();

    for (int i = 0; i < columnNames.size(); ++i) {
      if (std::find(
              partitionedBy.cbegin(),
              partitionedBy.cend(),
              columnNames.at(i)) != partitionedBy.cend()) {
        ++numPartitionColumns;
        columnHandles.push_back(
            std::make_shared<connector::hive::HiveColumnHandle>(
                columnNames.at(i),
                connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
                columnTypes.at(i),
                columnTypes.at(i)));
      } else {
        columnHandles.push_back(
            std::make_shared<connector::hive::HiveColumnHandle>(
                columnNames.at(i),
                connector::hive::HiveColumnHandle::ColumnType::kRegular,
                columnTypes.at(i),
                columnTypes.at(i)));
      }
    }

    VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());

    std::shared_ptr<const connector::hive::LocationHandle> locationHandle =
        makeLocationHandle(
            outputDirectoryPath,
            std::nullopt,
            connector::hive::LocationHandle::TableType::kNew);

    std::vector<std::shared_ptr<const VeloxIcebergNestedField>> columns;
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 0, "c0", BIGINT(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 1, "c1", INTEGER(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 2, "c2", SMALLINT(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 3, "c3", REAL(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 4, "c4", DOUBLE(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 5, "c5", VARCHAR(), nullptr));
    columns.emplace_back(
        new VeloxIcebergNestedField(false, 6, "c6", BOOLEAN(), nullptr));

    std::shared_ptr<const VeloxIcebergSchema> schema =
        std::make_shared<VeloxIcebergSchema>(
            0,
            columns,
            std::unordered_map<std::string, std::int32_t>(),
            std::unordered_map<std::string, std::int32_t>(),
            std::vector<int32_t>());

    std::vector<std::string> fields;
    fields.reserve(partitionedBy.size());
    for (const auto& partition : partitionedBy) {
      fields.push_back(partition);
    }

    std::shared_ptr<const VeloxIcebergPartitionSpec> partitionSpec =
        std::make_shared<VeloxIcebergPartitionSpec>(0, schema, fields);

    return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
        columnHandles,
        locationHandle,
        schema,
        partitionSpec,
        fileFormat,
        nullptr,
        CompressionKind::CompressionKind_ZSTD);
  }

  std::shared_ptr<IcebergDataSink> createIcebergDataSink(
      const RowTypePtr& rowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {}) {
    return std::make_shared<IcebergDataSink>(
        rowType,
        createIcebergInsertTableHandle(
            rowType, outputDirectoryPath, fileFormat, partitionedBy),
        connectorQueryCtx_.get(),
        CommitStrategy::kNoCommit,
        connectorConfig_);
  }

  std::vector<std::string> listFiles(const std::string& dirPath) {
    std::vector<std::string> files;
    for (auto& dirEntry : fs::recursive_directory_iterator(dirPath)) {
      if (dirEntry.is_regular_file()) {
        files.push_back(dirEntry.path().string());
      }
    }
    return files;
  }

  std::shared_ptr<HiveIcebergSplit> makeIcebergConnectorSplit(
      const std::string& filePath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    std::unordered_map<std::string, std::string> customSplitInfo;
    customSplitInfo["table_format"] = "hive-iceberg";

    auto file = filesystems::getFileSystem(filePath, nullptr)
                    ->openFileForRead(filePath);
    const int64_t fileSize = file->size();

    return std::make_shared<HiveIcebergSplit>(
        kHiveConnectorId,
        filePath,
        fileFormat,
        0,
        fileSize,
        partitionKeys,
        std::nullopt,
        customSplitInfo,
        nullptr,
        /*cacheable=*/true,
        std::vector<IcebergDeleteFile>());
  }

}

namespace gluten {
std::string IcebergWriter::write() {

}
}

