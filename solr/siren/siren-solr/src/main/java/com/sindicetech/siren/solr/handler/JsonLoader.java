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

import com.sindicetech.siren.solr.client.solrj.request.SirenUpdateRequest;
import com.sindicetech.siren.solr.handler.mapper.FieldMappers;
import com.sindicetech.siren.solr.handler.mapper.FieldMappersHandler;
import com.sindicetech.siren.solr.handler.mapper.IdFieldMapper;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

/**
 * A loader for JSON document used by {@link com.sindicetech.siren.solr.handler.SirenUpdateRequestHandler}. It will
 * be in charge of parsing the JSON document, performs the field mapping, creates a Solr document and send it
 * for indexing.
 */
public class JsonLoader extends ContentStreamLoader {

  private static final Logger logger = LoggerFactory.getLogger(JsonLoader.class);

  private static final ObjectMapper mapper = new ObjectMapper(); // ObjectMapper is thread-safe

  public static final String SOURCE_FIELDNAME = "_source_";

  /**
   * After its initialisation, this should be thread-safe
   */
  private final FieldMappers fieldMappers;

  JsonLoader(FieldMappers fieldMappers) {
    this.fieldMappers = fieldMappers;
  }

  @Override
  public String getDefaultWT() {
    return "json";
  }

  @Override
  public void load(final SolrQueryRequest req, final SolrQueryResponse rsp,
                   final ContentStream stream, final UpdateRequestProcessor processor) throws Exception {
    Reader reader = null;
    try {
      reader = stream.getReader();
      // keep a copy of the body for the source entry
      // TODO: instead of reading the stream to make a copy, try to create a copy of the json
      // while parsing it in the JsonReader
      String body = IOUtils.toString(reader);

      FieldMappersHandler mappersHandler = new FieldMappersHandler(fieldMappers, req.getCore());
      DocumentBuilder docBuilder = new DocumentBuilder();

      // Add the source field entry
      FieldEntry source = new FieldEntry(SOURCE_FIELDNAME, body);
      docBuilder.add(mappersHandler.map(source));

      // Add the id field initialised with a UUID. It will be overwritten if an id field exist in the JSON document.
      FieldEntry id = new FieldEntry(IdFieldMapper.INPUT_FIELD, UUID.randomUUID().toString().toLowerCase(Locale.ROOT));
      docBuilder.add(mappersHandler.map(id));

      JsonParser parser = mapper.getJsonFactory().createJsonParser(new StringReader(body));
      JsonReader jreader = new JsonReader(parser);

      FieldEntry entry;
      while ((entry = jreader.next()) != null) {
        docBuilder.add(mappersHandler.map(entry));
      }

      // the index schema might have changed
      req.updateSchemaToLatest();

      // check that we have seen all the required field mappers
      Set<String> missingRequiredMappers = mappersHandler.getMissingRequiredMappers();
      if (!missingRequiredMappers.isEmpty()) {
        throw new SolrException(BAD_REQUEST, "Document is missing the following required fields: " + missingRequiredMappers);
      }

      // Extract boost
      float boost = req.getParams().getFloat(SirenUpdateRequest.BOOST_PARAM, 1.0f);

      // Create and process the Add command
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = docBuilder.getSolrInputDocument();
      cmd.solrDoc.setDocumentBoost(boost);
      processor.processAdd(cmd);
    }
    finally {
      IOUtils.closeQuietly(reader);
    }
  }

}
