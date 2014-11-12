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

/**
 * The {@link com.sindicetech.siren.solr.handler.mapper.FieldMapper} for the unique key field 'id'.
 */
public class IdFieldMapper extends PathFieldMapper {

  public static final String INPUT_FIELD = "id";

  public static final String OUTPUT_FIELD = "id";

  public IdFieldMapper() {
    super(INPUT_FIELD, ""); // the fieldtype and field for id must be in the schema.xml
    this.setRequired(true);
  }

  @Override
  String getTargetFieldname(final FieldEntry entry) {
    return OUTPUT_FIELD;
  }

}
