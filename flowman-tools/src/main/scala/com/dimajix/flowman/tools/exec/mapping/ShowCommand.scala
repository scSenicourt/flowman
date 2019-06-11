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

package com.dimajix.flowman.tools.exec.mapping

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.task.ShowMappingTask
import com.dimajix.flowman.tools.exec.ActionCommand


class ShowCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ShowCommand])

    @Option(name="-n", aliases=Array("--limit"), usage="Specifies maximimum number of rows to print", metaVar="<limit>", required = false)
    var limit: Int = 10
    @Argument(index=0, usage="specifies the mapping to show", metaVar="<mapping>", required=true)
    var mapping: String = ""
    @Argument(index=1, usage="specifies the columns to show as a comma separated list", metaVar="<columns>", required=false)
    var columns: String = ""


    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        val columns = this.columns.split(",").filter(_.nonEmpty)
        val task = ShowMappingTask(context, MappingOutputIdentifier(mapping), columns, limit)

        Try {
            task.execute(executor)
        } match {
            case Success(_) =>
                logger.info("Successfully finished dumping mapping")
                true
            case Failure(e) =>
                logger.error(s"Caught exception while dumping mapping '$mapping", e)
                false
        }
    }
}
