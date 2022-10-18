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

package io.glutenproject.expression

import scala.collection.mutable.ArrayBuffer

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.{Expression, Murmur3Hash, XxHash64}
import org.apache.spark.sql.types.DataType

trait HashTransformer {
  def transformHash(funcName: String, args: java.lang.Object, exprs: Seq[Expression],
                  dataType: DataType, nullable: Boolean): ExpressionNode = {
    val nodes = new java.util.ArrayList[ExpressionNode]()
    val arrayBuffer = new ArrayBuffer[DataType]()
    exprs.foreach(expression => {
      val expressionNode = expression.asInstanceOf[ExpressionTransformer].doTransform(args)
      if (!expressionNode.isInstanceOf[ExpressionNode]) {
        throw new UnsupportedOperationException(s"Not supported yet.")
      }
      arrayBuffer.append(expression.dataType)
      nodes.add(expressionNode)
    })
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(funcName, arrayBuffer, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}

class Murmur3HashTransformer(exprs: Seq[Expression], original: Expression)
  extends Murmur3Hash(exprs: Seq[Expression])
    with ExpressionTransformer
    with HashTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    transformHash(ConverterUtils.MURMUR3HASH, args, exprs, original.dataType, original.nullable)
  }
}

class XxHash64Transformer(exprs: Seq[Expression], original: XxHash64)
  extends XxHash64(exprs: Seq[Expression], original.seed: Long)
    with ExpressionTransformer
    with HashTransformer  {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    transformHash(ConverterUtils.XXHASH64, args, exprs, original.dataType, original.nullable)
  }
}

object HashTransformer {

  def create(children: Seq[Expression], original: Expression): Expression =
    original match {
      case _: XxHash64 =>
        new XxHash64Transformer(children, original.asInstanceOf[XxHash64])
      case _: Murmur3Hash =>
        new Murmur3HashTransformer(children, original.asInstanceOf[Murmur3Hash])
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}


