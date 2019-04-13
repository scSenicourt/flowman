/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.util

import java.util.Locale

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType


object SchemaUtils {
    def mapType(typeName:String) : DataType  = {
        typeName.toLowerCase() match {
            case "string" => StringType
            case "float" => FloatType
            case "double" => DoubleType
            case "short" => ShortType
            case "int" => IntegerType
            case "integer" => IntegerType
            case "long" => LongType
            case "date" => DateType
            case "timestamp" => TimestampType
            case _ => throw new RuntimeException(s"Unknown type $typeName")
        }
    }
    def createSchema(fields:Seq[(String,String)]) : StructType = {
        // When reading data from MCSV files, we need to specify our desired schema. Here
        // we create a Spark SQL schema definition from the fields list in the specification
        def mapField(fieldName:String, typeName:String) : StructField = {
            StructField(fieldName, mapType(typeName), true)
        }

        StructType(fields.map { case (name:String,typ:String) => mapField(name, typ) } )
    }

    /**
      * Helper method for applying an optional schema, as specified by the SourceTable
      *
      * @param df
      * @param schema
      * @return
      */
    def applySchema(df:DataFrame, schema:StructType) : DataFrame = {
        // Apply requested Schema
        if (schema != null)
            df.select(schema.map(field => col(field.name).cast(field.dataType)):_*)
        else
            df
    }


    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param inputSchema - Denotes the input schema to be conformed
      * @param requiredSchema
      * @return
      */
    def conformSchema(inputSchema:StructType, requiredSchema:StructType) : Seq[Column] = {
        def conformField(requiredField:StructField, inputType:DataType, prefix:String) : Column = {
            requiredField.dataType match {
                case st:StructType => struct(conformStruct(st, inputType.asInstanceOf[StructType], prefix + requiredField.name + "."):_*)
                case _:ArrayType => col(prefix + requiredField.name)
                case _:DataType => col(prefix + requiredField.name).cast(requiredField.dataType)
            }
        }

        def conformStruct(requiredSchema:StructType, inputSchema:StructType, prefix:String) : Seq[Column] = {
            val inputFields = inputSchema.fields.map(field => (field.name.toLowerCase(Locale.ROOT), field)).toMap
            requiredSchema.fields.map { field =>
                inputFields.get(field.name.toLowerCase(Locale.ROOT))
                    .map(f => conformField(field, f.dataType, prefix))
                    .getOrElse(lit(null).cast(field.dataType))
                    .as(field.name)
            }.toSeq
        }

        conformStruct(requiredSchema, inputSchema, "")
    }

    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param df - Denotes the input DataFrame
      * @param requiredSchema
      * @return
      */
    def conformSchema(df:DataFrame, requiredSchema:StructType) : DataFrame = {
        val unifiedColumns = conformSchema(df.schema, requiredSchema)
        df.select(unifiedColumns:_*)
    }

    /**
      * Conforms the DataFrame to the given set of columns and data types
      * @param df
      * @param columns
      * @return
      */
    def conformColumns(df:DataFrame, columns:Seq[(String,String)]) : DataFrame = {
        val inputCols = df.columns.map(col => (col.toUpperCase(Locale.ROOT), df(col))).toMap
        val cols = columns.map(nv =>
            inputCols.getOrElse(nv._1.toUpperCase(Locale.ROOT), lit(null).as(nv._1))
                .cast(SchemaUtils.mapType(nv._2))
        )
        df.select(cols: _*)
    }

    /**
      * Removes all meta data from a Spark schema. Useful for comparing results in unit tests
      * @param schema
      * @return
      */
    def dropMetadata(schema:StructType) : StructType = {
        def processType(dataType:DataType) : DataType = {
            dataType match {
                case st:StructType => dropMetadata(st)
                case ar:ArrayType => ArrayType(processType(ar.elementType), ar.containsNull)
                case mt:MapType => MapType(processType(mt.keyType), processType(mt.valueType), mt.valueContainsNull)
                case dt:DataType => dt
            }
        }
        val fields = schema.fields.map { field =>
            StructField(field.name, processType(field.dataType), field.nullable)
        }
        StructType(fields)
    }

    /**
      * Converts the given Spark schema to a lower case schema
      * @param schema
      * @return
      */
    def toLowerCase(schema:StructType) : StructType = {
        StructType(schema.fields.map(toLowerCase))
    }
    private def toLowerCase(field:StructField) : StructField = {
        StructField(field.name.toLowerCase(Locale.ROOT), toLowerCase(field.dataType), field.nullable, field.metadata)
    }
    private def toLowerCase(dtype:DataType) : DataType = {
        dtype match {
            case struct:StructType => toLowerCase(struct)
            case array:ArrayType => ArrayType(toLowerCase(array.elementType),array.containsNull)
            case map:MapType => MapType(toLowerCase(map.keyType), toLowerCase(map.valueType), map.valueContainsNull)
            case dt:DataType => dt
        }
    }
}
