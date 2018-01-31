package com.dimajix.dataflow.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.OutputIdentifier


class OutputTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[OutputTask])

    @JsonProperty(value="outputs", required=true) private var _outputs:Seq[String] = Seq()

    def outputs(implicit context: Context) : Seq[OutputIdentifier] = _outputs.map(i => OutputIdentifier.parse(context.evaluate(i)))
    def outputs_=(outputs:Seq[String]) : Unit = _outputs = outputs

    /**
      * Executes all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        outputs.foreach(o => executeOutput(executor, o))
        true
    }

    private def executeOutput(executor: Executor, identifier:OutputIdentifier) : Boolean = {
        implicit val context = executor.context
        val output = context.getOutput(identifier)

        logger.info("Resolving dependencies for output {}", identifier.toString)
        val dependencies = output.dependencies
            .map(d => (d, executor.instantiate(d)))
            .toMap

        logger.info("Executing output {}", identifier.toString)
        output.execute(executor, dependencies)
        true
    }
}
