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

import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Locale
import javax.xml.stream.XMLInputFactory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs, DateFormatter, DateTimeUtils, ParseMode, PermissiveMode, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.internal.SQLConf

/**
 * Options for the XML data source.
 */
private[sql] class XmlOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging {

  import XmlOptions._

  def this(
    parameters: Map[String, String] = Map.empty,
    defaultTimeZoneId: String = SQLConf.get.sessionLocalTimeZone,
    defaultColumnNameOfCorruptRecord: String = "") = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      defaultColumnNameOfCorruptRecord)
  }

  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
  val compressionCodec = parameters.get(COMPRESSION).map(CompressionCodecs.getCodecClassName)
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  require(!rowTag.startsWith("<") && !rowTag.endsWith(">"),
          "'rowTag' should not include angle brackets")
  val rootTag = parameters.getOrElse("rootTag", XmlOptions.DEFAULT_ROOT_TAG)
  require(!rootTag.startsWith("<") && !rootTag.endsWith(">"),
          "'rootTag' should not include angle brackets")
  val declaration = parameters.getOrElse("declaration", XmlOptions.DEFAULT_DECLARATION)
  require(!declaration.startsWith("<") && !declaration.endsWith(">"),
          "'declaration' should not include angle brackets")
  val arrayElementName = parameters.getOrElse("arrayElementName",
    XmlOptions.DEFAULT_ARRAY_ELEMENT_NAME)
  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
  val excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false)
  val treatEmptyValuesAsNulls =
    parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false)
  val attributePrefix =
    parameters.getOrElse("attributePrefix", XmlOptions.DEFAULT_ATTRIBUTE_PREFIX)
  val valueTag = parameters.getOrElse("valueTag", XmlOptions.DEFAULT_VALUE_TAG)
  require(valueTag.nonEmpty, "'valueTag' option should not be empty string.")
  require(valueTag != attributePrefix,
    "'valueTag' and 'attributePrefix' options should not be the same.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", "_corrupt_record")
  val ignoreSurroundingSpaces =
    parameters.get("ignoreSurroundingSpaces").map(_.toBoolean).getOrElse(false)
  val parseMode = ParseMode.fromString(parameters.getOrElse("mode", PermissiveMode.name))
  val inferSchema = parameters.get("inferSchema").map(_.toBoolean).getOrElse(true)
  val rowValidationXSDPath = parameters.get("rowValidationXSDPath").orNull
  val wildcardColName =
    parameters.getOrElse("wildcardColName", XmlOptions.DEFAULT_WILDCARD_COL_NAME)
  val ignoreNamespace = parameters.get("ignoreNamespace").map(_.toBoolean).getOrElse(false)
  val timestampFormat = parameters.get("timestampFormat")
  val timezone = parameters.get("timezone")

  val zoneId: ZoneId = DateTimeUtils.getZoneId(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION,
      parameters.getOrElse(TIME_ZONE, defaultTimeZoneId)))

  // A language tag in IETF BCP 47 format
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val dateFormat = parameters.get("dateFormat")

  // sandip : change true to false
  val multiLine = parameters.get(MULTI_LINE).map(_.toBoolean).getOrElse(true)
  val charset = parameters.getOrElse(ENCODING,
    parameters.getOrElse(CHARSET, XmlOptions.DEFAULT_CHARSET))

  def buildXmlFactory(): XMLInputFactory = {
    XMLInputFactory.newInstance()
  }

  val timestampFormatter = TimestampFormatter(
    timestampFormat,
    zoneId,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  val dateFormatter = DateFormatter(
    dateFormat,
    locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
}

private[sql] object XmlOptions extends DataSourceOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_DECLARATION = "version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\""
  val DEFAULT_ARRAY_ELEMENT_NAME = "item"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null
  val DEFAULT_WILDCARD_COL_NAME = "xs_any"
  val LOCALE = newOption("locale")
  val COMPRESSION = newOption("compression")
  val MULTI_LINE = newOption("multiLine")
  // Options with alternative
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)
  val TIME_ZONE = "timezone"
  newOption(DateTimeUtils.TIMEZONE_OPTION, TIME_ZONE)

  def apply(parameters: Map[String, String]): XmlOptions =
    new XmlOptions(parameters, SQLConf.get.sessionLocalTimeZone)

  def apply(): XmlOptions =
    new XmlOptions(Map.empty, SQLConf.get.sessionLocalTimeZone)
}