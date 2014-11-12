/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
<<<<<<< HEAD:siren-solr/src/main/java/org/sindice/siren/solr/response/SirenProjectionTransformer.java
 * SIREn is not an open-source software. It is owned by Sindice Limited. SIREn
 * is licensed for evaluation purposes only under the terms and conditions of
 * the Sindice Limited Development License Agreement. Any form of modification
 * or reverse-engineering of SIREn is forbidden. SIREn is distributed without
 * any warranty.
=======
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
>>>>>>> develop:siren-solr/src/main/java/com/sindicetech/siren/solr/response/SirenProjectionTransformer.java
 */
package com.sindicetech.siren.solr.response;

import com.sindicetech.siren.qparser.tree.storage.ProjectionException;
import com.sindicetech.siren.qparser.tree.storage.SimpleJsonByQueryExtractor;
import com.sindicetech.siren.solr.schema.ExtendedJsonField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerWithContext;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

import java.io.IOException;

/**
 *  A {@link DocTransformer} for projecting documents according to json queries
 *  with variables.
 *
 *  It is necessary to specify the fl=[sirenProjection] parameter
 *  in order to use the transformer. For example:
 *      http://localhost:8983/solr/select?q=test&fl=id,[sirenProjection]
 *
 *  This assumes that solrconfig*.xml contains a trainsformer declaration as follows:
 *    &lt;transformer name=&quot;sirenProjection&quot; class=&quot;com.sindicetech.siren.solr.response.SirenProjectionTransformerFactory&quot; /&gt;
 *
 *  For more information about DocTrasnformers, see for example:
 *   - https://wiki.apache.org/solr/DocTransformers
 *   - http://solr.pl/en/2011/12/05/solr-4-0-doctransformers-first-look/
 */
public class SirenProjectionTransformer extends TransformerWithContext {

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void transform(SolrDocument doc, int docid) throws IOException {
    Query query = context.query;

    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();

    try {
      IndexSchema schema = context.req.getSchema();

      for (String fieldName : doc.getFieldNames()) {
        FieldType ft = schema.getFieldOrNull(fieldName).getType();
        if (ft instanceof ExtendedJsonField) {
          String sirenField = (String) doc.getFieldValue(fieldName);
          String json = extractor.extractAsString(sirenField, query);
          if (json == null) {
            // query doesn't contain variables, no transformation is necessary
            continue;
          }

          doc.setField(fieldName, json);
        }
      }
    } catch (ProjectionException e) {
      throw new IOException(String.format(
          "Problem while projecting (extracting variables from matched document id %s",
          doc.getFieldValue("id")), e);
    }
  }

}
