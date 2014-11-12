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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

/**
 * Handles the field mapping logic and add automatically new fields to the schema.
 * It first tries to find a path-based field mapper, then a datatype-based field
 * mapper and finally fall-back to the default mapper
 * if no assocaited mappers were found.
 */
public class FieldMappersHandler {

  private final Set<String> processedMappers = new HashSet<>();

  private final FieldMappers mappers;

  private final SolrCore core;

  private static final Logger logger = LoggerFactory.getLogger(FieldMappersHandler.class);

  public FieldMappersHandler(FieldMappers mappers, SolrCore core) {
    this.mappers = mappers;
    this.core = core;
  }

  public SolrInputField map(FieldEntry entry) {
    String path = entry.getPath();
    String datatype = entry.getDatatype();

    FieldMapper mapper;

    // path mappers
    if (mappers.getPathMappers().containsKey(path)) {
      mapper = mappers.getPathMappers().get(path);
    }
    // datatype mappers
    else if (mappers.getTypeMappers().containsKey(datatype)) {
      mapper = mappers.getTypeMappers().get(datatype);
    }
    // default mapper
    else {
      mapper = mappers.getDefaultMapper();
    }

    return this.map(mapper, entry);
  }

  private SolrInputField map(FieldMapper mapper, FieldEntry entry) {
    if (mapper.isNull()) {
      logger.debug("Field entry {} match a null field mapper, it will be ignored.", entry);
      return null;
    }
    processedMappers.add(mapper.getKey());
    this.addSchemaField(mapper, entry);
    return mapper.map(entry);
  }

  private void addSchemaField(FieldMapper mapper, FieldEntry entry) {
    if (!core.getLatestSchema().isMutable()) {
      final String message = "This IndexSchema is not mutable.";
      throw new SolrException(BAD_REQUEST, message);
    }

    for (;;) {
      final IndexSchema oldSchema = core.getLatestSchema();
      if (oldSchema.getFieldTypeNoEx(mapper.getTargetFieldname(entry)) != null) {
        return; // the field already exists in the schema
      }

      try {
        SchemaField field = mapper.getSchemaField(core.getLatestSchema(), entry);
        IndexSchema newSchema = oldSchema.addField(field);
        if (newSchema != null) {
          core.setLatestSchema(newSchema);
          logger.debug("Successfully added field '{}' to the schema.", field.getName());
          return; // success - exit from the retry loop
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to add fields.");
        }
      }
      catch(ManagedIndexSchema.FieldExistsException e) {
        logger.debug("The field to be added already exists in the schema - retrying.");
        // No action: at least one field to be added already exists in the schema, so retry
        // We should never get here, since oldSchema.getFieldTypeNoEx(field) will exclude already existing fields
      }
      catch(ManagedIndexSchema.SchemaChangedInZkException e) {
        logger.debug("Schema changed while processing request - retrying.");
      }
    }
  }

  /**
   * Returns all the required mappers that have not been triggered during the processing.
   */
  public Set<String> getMissingRequiredMappers() {
    Set<String> required = new HashSet<>(mappers.getRequiredMappers().keySet());
    required.removeAll(processedMappers);
    return required;
  }

}

