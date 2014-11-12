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

/**
 * The base class for path-based {@link com.sindicetech.siren.solr.handler.mapper.FieldMapper}.
 */
public class TypeFieldMapper extends FieldMapper {

  private final String inputType;

  public TypeFieldMapper(final String inputType, final String fieldType) {
    super(fieldType);
    this.inputType = inputType;
  }

  @Override
  String getKey() {
    return getInputType();
  }

  public String getInputType() {
    return inputType;
  }

}
