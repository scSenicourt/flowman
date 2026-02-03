/*
 * Copyright (C) 2021 The Flowman Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim.expressionEncoderFor
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.catalyst.PlanUtils


object DataFrameBuilder {
    private val rowParserOptions = RowParser.Options()

    /**
     * Creates a DataFrame from a sequence of string array records. The string values will be converted to appropriate
     * data types as specified in the schema
     *
     * @param sparkSession
     * @param lines
     * @param schema
     * @return
     */
    def ofStringValues(sparkSession: SparkSession, lines:Seq[Array[String]], schema:StructType) : DataFrame = {
        val reader = new RowParser(schema, rowParserOptions)
        val rows = lines.map(reader.parse)
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Create an empty [[DataFrame]] from a schema
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def ofSchema(sparkSession: SparkSession, schema:StructType) : DataFrame = {
        val rdd = sparkSession.sparkContext.emptyRDD[Row]
        sparkSession.createDataFrame(rdd, schema)
    }

    /**
     * Creates a DataFrame from a sequence of Spark [[Row]] objects and a Spark schema.
     *
     * @param sparkSession
     * @param rows
     * @param schema
     * @return
     */
    def ofRows(sparkSession: SparkSession, rows:Seq[Row], schema:StructType): DataFrame = {
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Creates a DataFrame from a Spark [[LogicalPlan]]
     *
     * @param sparkSession
     * @param logicalPlan
     * @return
     */
    def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
        val qe = sparkSession.sessionState.executePlan(logicalPlan)
        qe.assertAnalyzed()
        // In Spark 4.0, Dataset is abstract - use internal API or reflection
        try {
            // Try to use Dataset.apply if available
            Dataset.ofRows(sparkSession, logicalPlan)
        } catch {
            case _: NoSuchMethodError =>
                // Fallback: use reflection to create Dataset
                try {
                    val datasetClass = classOf[Dataset[Row]]
                    val constructor = datasetClass.getDeclaredConstructor(
                        classOf[SparkSession],
                        classOf[LogicalPlan],
                        classOf[org.apache.spark.sql.Encoder[Row]]
                    )
                    constructor.setAccessible(true)
                    constructor.newInstance(sparkSession, logicalPlan, expressionEncoderFor(qe.analyzed.schema))
                } catch {
                    case _: Exception =>
                        // Ultimate fallback: use createDataFrame with QueryExecution
                        sparkSession.internalCreateDataFrame(qe.analyzed, expressionEncoderFor(qe.analyzed.schema))
                }
        }
    }

    /**
     * Creates a DataFrame containing a single Row for a given Schema, either with NULL values or with default values.
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def singleRow(sparkSession: SparkSession, schema: StructType): DataFrame = {
        val logicalPlan = PlanUtils.singleRowPlan(schema)
        // In Spark 4.0, Dataset is abstract - use internal API or reflection
        try {
            // Try to use Dataset.apply if available
            Dataset.ofRows(sparkSession, logicalPlan)
        } catch {
            case _: NoSuchMethodError =>
                // Fallback: use reflection to create Dataset
                try {
                    val datasetClass = classOf[Dataset[Row]]
                    val constructor = datasetClass.getDeclaredConstructor(
                        classOf[SparkSession],
                        classOf[LogicalPlan],
                        classOf[org.apache.spark.sql.Encoder[Row]]
                    )
                    constructor.setAccessible(true)
                    constructor.newInstance(sparkSession, logicalPlan, expressionEncoderFor(schema))
                } catch {
                    case _: Exception =>
                        // Ultimate fallback: create DataFrame directly
                        sparkSession.internalCreateDataFrame(logicalPlan, expressionEncoderFor(schema))
                }
        }
    }

    /**
     * Creates a DataFrame containing a single Row for a given Schema, either with NULL values or with default values.
     *
     * @param sparkSession
     * @param schema
     * @return
     */
    def namedAttributes(sparkSession: SparkSession, schema: StructType): DataFrame = {
        val logicalPlan = PlanUtils.namedAttributePlan(schema)
        // In Spark 4.0, Dataset is abstract - use internal API or reflection
        try {
            // Try to use Dataset.apply if available
            Dataset.ofRows(sparkSession, logicalPlan)
        } catch {
            case _: NoSuchMethodError =>
                // Fallback: use reflection to create Dataset
                try {
                    val datasetClass = classOf[Dataset[Row]]
                    val constructor = datasetClass.getDeclaredConstructor(
                        classOf[SparkSession],
                        classOf[LogicalPlan],
                        classOf[org.apache.spark.sql.Encoder[Row]]
                    )
                    constructor.setAccessible(true)
                    constructor.newInstance(sparkSession, logicalPlan, expressionEncoderFor(schema))
                } catch {
                    case _: Exception =>
                        // Ultimate fallback: create DataFrame directly
                        sparkSession.internalCreateDataFrame(logicalPlan, expressionEncoderFor(schema))
                }
        }
    }
}
