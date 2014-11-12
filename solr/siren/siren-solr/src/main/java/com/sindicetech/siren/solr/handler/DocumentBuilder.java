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
package com.sindicetech.siren.solr.handler;

import com.sindicetech.siren.solr.handler.mapper.IdFieldMapper;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/**
 * Helper class to build {@link org.apache.solr.common.SolrInputDocument}.
 */
public class DocumentBuilder {

  private final SolrInputDocument doc;

  public DocumentBuilder() {
    doc = new SolrInputDocument();
  }

  public void add(SolrInputField field) {
    if (field != null) {
      // Overwrite the 'id' field
      if (field.getName().equals(IdFieldMapper.INPUT_FIELD)) {
        doc.setField(field.getName(), field.getValue(), field.getBoost());
      }
      // Append other fields
      else {
        doc.addField(field.getName(), field.getValue(), field.getBoost());
      }
    }
  }

  public void add(SolrInputField[] fields) {
    for (SolrInputField field : fields) {
      this.add(field);
    }
  }

  public SolrInputDocument getSolrInputDocument() {
    return doc;
  }

}
