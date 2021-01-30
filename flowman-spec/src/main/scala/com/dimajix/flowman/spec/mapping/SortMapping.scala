/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class SortMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[(String,SortOrder)]
) extends BaseMapping {
    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     * @return
     */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(tables != null)

        val df = tables(input)
        val cols = columns.map(nv =>
            nv._2 match {
                case SortOrder(Ascending, NullsFirst) => col(nv._1).asc_nulls_first
                case SortOrder(Ascending, NullsLast) => col(nv._1).asc_nulls_last
                case SortOrder(Descending, NullsFirst) => col(nv._1).desc_nulls_first
                case SortOrder(Descending, NullsLast) => col(nv._1).desc_nulls_last
            }
        )
        val result = df.sort(cols:_*)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(input != null)

        val result = input(this.input)

        Map("main" -> result)
    }
}



class SortMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns:Seq[Map[String,String]] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SortMapping = {
        SortMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.flatMap(context.evaluate).map { case(col,order) => col -> SortOrder.of(order) }
        )
    }
}
