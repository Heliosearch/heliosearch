package org.apache.solr.schema;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.solr.request.SolrRequestInfo;

public class SchemaUtil {
  public static FieldType GENERIC_STRTYPE = new StrField();


  /** Returns a FieldType for the given field name (or defaults to a generic StrField type if the schema can't be found, or if the field doesn't exist in the schema).
   * This should *only* be used when there is no normal solr request object or other context.
   */
  public static FieldType getFieldTypeNoContext(String fieldName) {
    SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();
    if (reqInfo == null) return GENERIC_STRTYPE;

    FieldType ft = reqInfo.getReq().getSchema().getFieldTypeNoEx(fieldName);
    return ft == null ? GENERIC_STRTYPE : ft;
  }


}
