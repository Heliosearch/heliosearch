/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sindicetech.siren.solr.handler.mapper;

import com.sindicetech.siren.solr.handler.FieldEntry;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import java.util.HashMap;
import java.util.Map;

/**
 * The base class for field mappers.
 */
public abstract class FieldMapper {

  protected boolean required = false;

  protected String fieldType;

  public static final String NULL_FIELDTYPE = "null";

  FieldMapper(final String fieldType) {
    this.fieldType = fieldType;
  }

  public void setRequired(final boolean isRequired) {
    this.required = isRequired;
  }

  boolean isRequired() {
    return required;
  }

  abstract String getKey();

  SolrInputField map(final FieldEntry entry) {
    SolrInputField field = new SolrInputField(this.getTargetFieldname(entry));
    field.setValue(entry.getValue(), 1.0f);
    return field;
  }

  boolean isNull() {
    if (fieldType.equals(NULL_FIELDTYPE)) {
      return true;
    }
    return false;
  }

  SchemaField getSchemaField(final IndexSchema schema, final FieldEntry entry) {
    Map<String, Boolean> options = new HashMap<String, Boolean>();
    return schema.newField(this.getTargetFieldname(entry), fieldType, options);
  }

  String getTargetFieldname(final FieldEntry entry) {
    return entry.getPath();
  }

}
