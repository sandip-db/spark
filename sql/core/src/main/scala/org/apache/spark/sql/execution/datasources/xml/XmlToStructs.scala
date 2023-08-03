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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExprUtils, NullIntolerant, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{FailFastMode, GenericArrayData, PermissiveMode}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.execution.datasources.xml.parsers.StaxXmlParser
import org.apache.spark.sql.execution.datasources.xml.util.InferSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class XmlToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression
    with TimeZoneAwareExpression
    with CodegenFallback
    with ExpectsInputTypes
    with NullIntolerant
    with QueryErrorsBase {

  def this(child: Expression, schema: Expression, options: Map[String, String]) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = options,
      child = child,
      timeZoneId = None)

  def this(child: Expression, schema: Expression) = this(child, schema, Map.empty[String, String])

  def this(child: Expression, schema: Expression, options: Expression) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  override lazy val dataType: DataType = schema

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  @transient
  lazy val rowSchema: StructType = schema match {
    case st: StructType => st
    case ArrayType(st: StructType, _) => st
  }

  val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
  @transient lazy val parser = {
    val parsedOptions = new XmlOptions(options, timeZoneId.get, nameOfCorruptRecord)
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("from_xml", mode)
    }

    new StaxXmlParser(rowSchema, parsedOptions)
  }
  override def nullSafeEval(xml: Any): Any = xml match {
    case string: UTF8String =>
      CatalystTypeConverters.convertToCatalyst(
        parser.parseColumn(string.toString, rowSchema))
    case string: String =>
      parser.parseColumn(string, rowSchema)
    case arr: GenericArrayData =>
      CatalystTypeConverters.convertToCatalyst(
        arr.array.map(s => parser.parseColumn(s.toString, rowSchema)))
    case arr: Array[_] =>
      arr.map(s => parser.parseColumn(s.toString, rowSchema))
    case _ => null
  }

  override def inputTypes: Seq[DataType] = schema match {
    case _: StructType => Seq(StringType)
    case ArrayType(_: StructType, _) => Seq(ArrayType(StringType))
  }

  // Overrides, in Spark 3.2.0+
  protected def withNewChildInternal(newChild: Expression): XmlToStructs =
    copy(child = newChild)
}

/**
 * A function infers schema of XML string.
 */
@ExpressionDescription(
  usage = "_FUNC_(csv[, options]) - Returns schema in the DDL format of XML string.",
  examples = """
    Examples:
      > SELECT _FUNC_('1,abc');
       STRUCT<_c0: INT, _c1: STRING>
  """,
  since = "3.0.0",
  group = "csv_funcs")
case class SchemaOfCsv(
                        child: Expression,
                        options: Map[String, String])
  extends UnaryExpression with CodegenFallback with QueryErrorsBase {

  def this(child: Expression) = this(child, Map.empty[String, String])

  def this(child: Expression, options: Expression) = this(
    child = child,
    options = ExprUtils.convertToMapData(options))

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  @transient
  private lazy val xmlOptions = new XmlOptions(options, "UTC")

  @transient
  private lazy val xmlFactory = xmlOptions.buildXmlFactory()

  @transient
  private lazy val xml = child.eval().asInstanceOf[UTF8String]

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.foldable && xml != null) {
      super.checkInputDataTypes()
    } else if (!child.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "xml",
          "inputType" -> toSQLType(child.dataType),
          "inputExpr" -> toSQLExpr(child)))
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "xml"))
    }
  }

  override def eval(v: InternalRow): Any = {
    val dataType = InferSchema.infer(xml.toString, xmlOptions) match {
      case st: StructType =>
        InferSchema.canonicalizeType(st).getOrElse(StructType(Nil))
      case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
        InferSchema
          .canonicalizeType(at.elementType)
          .map(ArrayType(_, containsNull = at.containsNull))
          .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
      case other: DataType =>
        InferSchema.canonicalizeType(other).getOrElse(StringType)
    }

    UTF8String.fromString(dataType.sql)
  }

  override def prettyName: String = "schema_of_xml"

  override protected def withNewChildInternal(newChild: Expression): SchemaOfCsv =
    copy(child = newChild)
}
