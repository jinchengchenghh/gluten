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
package org.apache.spark.sql.execution.ui

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, TaskResourceRequest}
import org.apache.spark.sql.execution.{GlutenAutoAdjustStageResourceProfile => GlutenResourceProfile}

import scala.collection.mutable

object SingleTaskUtil {

  def applySingleTask(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    val rpManager = spark.sparkContext.resourceProfileManager
    val defaultRP = rpManager.defaultResourceProfile

    // initial resource profile config as default resource profile
    val taskResource = mutable.Map.empty[String, TaskResourceRequest] ++= defaultRP.taskResources
    val executorResource =
      mutable.Map.empty[String, ExecutorResourceRequest] ++= defaultRP.executorResources
    executorResource.put(ResourceProfile.CORES, new ExecutorResourceRequest(ResourceProfile.CORES, 1))
    taskResource.put(ResourceProfile.CORES, new TaskResourceRequest(ResourceProfile.CORES, 1))
    val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
    GlutenResourceProfile.applyNewResourceProfileIfPossible(
      plan,
      newRP,
      rpManager,
      spark.sparkContext.getConf)
  }
}
