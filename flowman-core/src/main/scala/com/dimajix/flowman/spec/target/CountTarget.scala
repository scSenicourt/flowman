/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


case class CountTarget(
    instanceProperties:Target.Properties
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[CountTarget])

    /**
      * Build the "count" target by printing the number of records onto the console
      *
      * @param executor
      * @param input
      */
    override def build(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit = {
        val count = input(instanceProperties.input).count()
        System.out.println(s"Table $input contains $count records")
    }

    /**
      * Cleaning a count target is a no-op
      * @param executor
      */
    override def clean(executor: Executor): Unit = {

    }
}


class CountTargetSpec extends TargetSpec {
    override def instantiate(context: Context): CountTarget = {
        CountTarget(
            Target.Properties(context)
        )
    }
}