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

package com.dimajix.flowman.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper


class SwaggerSchemaTest extends FlatSpec with Matchers  {
    "A Swagger Schema" should "be deserializable" in {
        val spec =
            """
              |kind: swagger
              |entity: Pet
              |spec: |
              |    swagger: "2.0"
              |    info:
              |      version: 1.0.0
              |      title: Swagger Petstore
              |      description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |      termsOfService: http://swagger.io/terms/
              |      contact:
              |        name: Swagger API Team
              |        email: apiteam@swagger.io
              |        url: http://swagger.io
              |      license:
              |        name: Apache 2.0
              |    url: https://www.apache.org/licenses/LICENSE-2.0.html
              |    definitions:
              |      Pet:
              |        required:
              |          - name
              |          - id
              |        properties:
              |          name:
              |            type: string
              |            description: The Pets name
              |          tag:
              |            type: string
              |          id:
              |            type: integer
              |            format: int64
              |            description: The Pets ID
              |""".stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
        result shouldBe an[SwaggerSchema]
        result.description should be ("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification")

        val fields = result.fields
        fields.size should be (3)
        fields(0).nullable should be (false)
        fields(0).name should be ("id")
        fields(0).description should be ("The Pets ID")
        fields(0).ftype should be (LongType)

        fields(1).nullable should be (false)
        fields(1).name should be ("name")
        fields(1).description should be ("The Pets name")
        fields(1).ftype should be (StringType)

        fields(2).nullable should be (true)
        fields(2).name should be ("tag")
        fields(2).ftype should be (StringType)
    }
}
