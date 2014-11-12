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

import java.util.HashMap;
import java.util.Map;

/**
 * A holder for several {@link FieldMapper}s.
 */
public class FieldMappers {

  private final Map<String, FieldMapper> requiredMappers;

  private final Map<String, FieldMapper> optionalMappers;

  private final Map<String, FieldMapper> pathMappers;

  private final Map<String, FieldMapper> typeMappers;

  private FieldMapper defaultMapper;

  public FieldMappers() {
    this.requiredMappers = new HashMap<>();
    this.optionalMappers = new HashMap<>();
    this.pathMappers = new HashMap<>();
    this.typeMappers = new HashMap<>();
  }

  public boolean isEmpty() {
    return requiredMappers.isEmpty() && optionalMappers.isEmpty();
  }

  public void setDefaultMapper(FieldMapper defaultMapper) {
    this.defaultMapper = defaultMapper;
  }

  public FieldMapper getDefaultMapper() {
    return defaultMapper;
  }

  public Map<String, FieldMapper> getRequiredMappers() {
    return requiredMappers;
  }

  public Map<String, FieldMapper> getPathMappers() {
    return pathMappers;
  }

  public Map<String, FieldMapper> getTypeMappers() {
    return typeMappers;
  }

  public FieldMappers add(FieldMapper mapper) {
    if (mapper.isRequired()) {
      requiredMappers.put(mapper.getKey(), mapper);
    }
    else {
      optionalMappers.put(mapper.getKey(), mapper);
    }

    if (mapper instanceof PathFieldMapper) {
      pathMappers.put(mapper.getKey(), mapper);
    }
    else {
      typeMappers.put(mapper.getKey(), mapper);
    }

    return this;
  }

}

