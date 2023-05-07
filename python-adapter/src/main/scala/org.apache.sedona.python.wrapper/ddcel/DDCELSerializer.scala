/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.python.wrapper.ddcel

import org.apache.sedona.core.ddcel.entries.{DDCELEntry, Face, HalfEdge, Vertex}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBWriter
import org.apache.sedona.python.wrapper.utils.implicits._

import java.nio.charset.StandardCharsets

case class DDCELSerializer(entry: DDCELEntry) {

  def serialize: Array[Byte] = {

    val entryType = entry match {
      case _: Vertex => 1.toByteArray()
      case _: HalfEdge => 2.toByteArray()
      case _: Face => 3.toByteArray()
    }

    val wkbWriter = new WKBWriter(2, 2)
    val serializedGeom = wkbWriter.write(entry.getGeometry)
    val serializedGeomLength = serializedGeom.length.toByteArray()

    val params = entry.getParams.toArray
    val serializedParams = params.map {
      case boolean: java.lang.Boolean => if(boolean) 1.toByteArray() else 0.toByteArray()
      case int: java.lang.Integer => int.intValue().toByteArray()
      case double: java.lang.Double => double.doubleValue().toByteArray()
      case string: String => string.getBytes(StandardCharsets.UTF_8)
      case geometry: Geometry => wkbWriter.write(geometry)
    }
    val serializedParamsWithLen = serializedParams.flatMap(param => param.length.toByteArray() ++ param)

    entryType ++ serializedGeomLength ++ serializedGeom ++ serializedParamsWithLen
  }

}
