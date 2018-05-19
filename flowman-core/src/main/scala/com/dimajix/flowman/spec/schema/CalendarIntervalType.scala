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

import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.CalendarInterval


case object CalendarIntervalType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.CalendarIntervalType

    override def parse(value:String, granularity:String) : CalendarInterval = {
        CalendarInterval.fromString(value)
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[CalendarInterval] = {
        value match {
            case SingleValue(v) => Seq(CalendarInterval.fromString(v))
            case ArrayValue(values) => values.map(CalendarInterval.fromString)
            case RangeValue(start,end,step) => ???
        }
    }
}
