/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.types

import org.apache.spark.sql.SparkShim
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.util.ObjectMapper
import com.dimajix.spark.SPARK_VERSION_MAJOR
import com.dimajix.util.DateTimeUtils


class CalendarIntervalTypeTest extends FlatSpec with Matchers {
    "A CalendarIntervalType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("calendarinterval") should be(CalendarIntervalType)
    }

    it should "parse strings" in {
        CalendarIntervalType.parse("interval 12 minute") should be (SparkShim.calendarInterval(0, 0, 12*DateTimeUtils.MICROS_PER_MINUTE))
    }

    it should "provide the correct Spark type" in {
        CalendarIntervalType.sparkType should be (org.apache.spark.sql.types.CalendarIntervalType)
    }

    it should "provide the correct SQL type" in {
        CalendarIntervalType.sqlType should be ("calendarinterval")
        CalendarIntervalType.typeName should be ("calendarinterval")
        if (SPARK_VERSION_MAJOR <= 2)
            CalendarIntervalType.sparkType.sql should be ("CALENDARINTERVAL")
        else
            CalendarIntervalType.sparkType.sql should be ("INTERVAL")
    }
}
