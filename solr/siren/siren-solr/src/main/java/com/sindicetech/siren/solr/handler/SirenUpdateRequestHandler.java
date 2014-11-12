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

import com.sindicetech.siren.solr.handler.mapper.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.ContentStreamLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

/**
 * <p>
 * This request handler accepts JSON as an input document and will automatically index it
 * using SIREn and Solr fields. Solr fields are created by flattening the paths found in the JSON document.
 * The fields will be dynamically added to the schema.
 * </p>
 * <p>
 * This request handler requires that the schema.xml defines the field 'id' as unique key. If the JSON document
 * contains an 'id' attribute, it will be mapped to this field. If the JSON document does not contain an 'id' attribute,
 * a UUID will be automatically generated.
 * </p>
 * <p>
 * The request handler is configured to map each path to a schema field type that will be used when adding the new
 * field to the schema.
 * </p>
 * <p>
 * This request handler takes as configuration a list of field mappings. There are four types of field mappings:
 * 'default', 'json', 'required' and 'optional'. The 'default' field mapping must be unique and specifies the
 * default mapping if no other mappings are found for a given path. The 'json' field mapping must be unique and
 * specifies the mapping for the SIREn's field '_json_' that is used to index the full JSON document. The 'required'
 * field mapping specifies one field mapping that must occur during the processing of the document. If the
 * field mapping is not triggered during processing, the document will be rejected. The 'optional' field mapping
 * specifies one field mapping that should occur. If an optional field mapping is not triggered during the processing
 * of a document, the document will not be rejected.
 * </p>
 * <p>
 * Every field mapping must specify the parameter 'fieldType'. This parameter must refer to one existing field type
 * in the schema.xml. There is one special field type 'null' that can be used to specify that this path
 * matching this field mapping must be ignored, i.e., this path will not be indexed.
 * </p>
 * <p>
 * The 'required' and 'optional' field mapping must specify a second parameter. This parameter indicates the type
 * of mapping. There are two types of mappings: path-based and type-based. Path-based mappings will be triggered if
 * the path name matches. A path is specified with the parameter 'path' and is using the dot notation, for example
 * "Address.Country". Type-based mapping will be triggered
 * if the value type matches. The value type is specified with the parameter 'type' and one of the following
 * value: 'String', 'Boolean', 'Integer', 'Long', 'Double', 'Float'.
 * </p>
 * <p>
 * Example configuration:
 * </p>
 *
 * <pre class="prettyprint">
 * &lt;requestHandler name="/siren/add" class="com.sindicetech.siren.solr.handler.SirenUpdateRequestHandler"&gt;
 *   &lt;lst name="default"&gt;
 *     &lt;str name="fieldType"&gt;null&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="json"&gt;
 *     &lt;str name="fieldType"&gt;concise&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="required"&gt;
 *     &lt;str name="path"&gt;Address.Country&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;string&lt;/str&gt;
 *   &lt;/lst&gt;
 *   &lt;lst name="optional"&gt;
 *     &lt;str name="type"&gt;String&lt;/str&gt;
 *     &lt;str name="fieldType"&gt;text&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/requestHandler&gt;</pre>
 */
public class SirenUpdateRequestHandler extends UpdateRequestHandler {

  private static final String JSON_PARAM = "json";
  private static final String REQUIRED_PARAM = "required";
  private static final String OPTIONAL_PARAM = "optional";
  private static final String DEFAULT_PARAM = "default";
  private static final String FIELD_TYPE_PARAM = "fieldType";
  private static final String PATH_PARAM = "path";
  private static final String TYPE_PARAM = "type";

  private final FieldMappers fieldMappers = new FieldMappers();

  @Override
  public void init(final NamedList args) {
    super.init(args);

    // ID mapper
    FieldMapper idMapper = new IdFieldMapper();
    fieldMappers.add(idMapper);

    // JSON mapper
    FieldMapper jsonMapper = this.parseJsonMapping(args);
    if (jsonMapper != null) {
      jsonMapper.setRequired(true);
      fieldMappers.add(jsonMapper);
    }

    // Required mappers
    for (FieldMapper mapper : this.parseMappings(args, REQUIRED_PARAM)) {
      mapper.setRequired(true);
      fieldMappers.add(mapper);
    }

    // Optional mappers
    for (FieldMapper mapper : this.parseMappings(args, OPTIONAL_PARAM)) {
      fieldMappers.add(mapper);
    }

    // Default mapper
    FieldMapper defaultMapper = this.parseDefaultMapping(args);
    if (defaultMapper != null) {
      fieldMappers.setDefaultMapper(defaultMapper);
    }
  }

  private List<FieldMapper> parseMappings(NamedList args, String paramName) {
    List<FieldMapper> mappers = new ArrayList<>();
    List params = args.getAll(paramName);

    if (!params.isEmpty()) {
      for (Object mapping : params) {
        NamedList mappingNamedList = this.validateParameter(paramName, mapping);
        String fieldType = this.parseFieldTypeParameter(mappingNamedList);
        String path = this.parseStringParameter(mappingNamedList, PATH_PARAM);
        String type = this.parseStringParameter(mappingNamedList, TYPE_PARAM);

        if ((path == null && type == null) || (path != null && type != null)) {
          throw new SolrException(SERVER_ERROR, "Each mapping must contain either a '" + PATH_PARAM +
          "' or a '" + TYPE_PARAM + "' sub-parameter");
        }

        if (mappingNamedList.size() != 0) {
          throw new SolrException(SERVER_ERROR, "Unexpected '" + paramName
          + "' sub-parameter(s): '" + mappingNamedList.toString() + "'");
        }

        if (path == null) {
          mappers.add(new TypeFieldMapper(type, fieldType));
        }
        else {
          mappers.add(new PathFieldMapper(path, fieldType));
        }
      }
      args.remove(paramName);
    }

    return mappers;
  }

  private FieldMapper parseDefaultMapping(NamedList args) {
    List defaultParams = args.getAll(DEFAULT_PARAM);
    if (defaultParams.size() > 1) {
      throw new SolrException(SERVER_ERROR, "Only one '" + DEFAULT_PARAM + "' mapping is allowed");
    }

    FieldMapper mapper = null;

    if (!defaultParams.isEmpty()) {
      NamedList defaultMappingNamedList = this.validateParameter(DEFAULT_PARAM, defaultParams.get(0));
      String fieldType = this.parseFieldTypeParameter(defaultMappingNamedList);
      mapper = new DefaultFieldMapper(fieldType);
      args.remove(DEFAULT_PARAM);
    }

    return mapper;
  }

  private FieldMapper parseJsonMapping(NamedList args) {
    List jsonParams = args.getAll(JSON_PARAM);
    if (jsonParams.size() > 1) {
      throw new SolrException(SERVER_ERROR, "Only one '" + JSON_PARAM + "' mapping is allowed");
    }

    FieldMapper mapper = null;

    if (!jsonParams.isEmpty()) {
      NamedList jsonMappingNamedList = this.validateParameter(JSON_PARAM, jsonParams.get(0));
      String fieldType = this.parseFieldTypeParameter(jsonMappingNamedList);
      mapper = new JsonFieldMapper(fieldType);
      args.remove(JSON_PARAM);
    }

    return mapper;
  }

  private NamedList validateParameter(String paramName, Object param) {
    if (param == null) {
      throw new SolrException(SERVER_ERROR, "'" + paramName + "' parameter cannot be null");
    }
    if (!(param instanceof NamedList)) {
      throw new SolrException(SERVER_ERROR, "'" + paramName + "' parameter must be a <lst>");
    }
    return (NamedList) param;
  }

  private String parseFieldTypeParameter(NamedList mappingNamedList) {
    Object fieldTypeObj = mappingNamedList.remove(FIELD_TYPE_PARAM);
    if (fieldTypeObj == null) {
      throw new SolrException(SERVER_ERROR, "Each mapping must contain a '" + FIELD_TYPE_PARAM + "' <str>");
    }
    if (!(fieldTypeObj instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "'" + FIELD_TYPE_PARAM + "' parameter must be a <str>");
    }
    if (mappingNamedList.get(FIELD_TYPE_PARAM) != null) {
      throw new SolrException(SERVER_ERROR, "Each mapping must contain only one '" + FIELD_TYPE_PARAM + "' <str>");
    }
    return fieldTypeObj.toString();
  }

  private String parseStringParameter(NamedList mappingNamedList, String paramName) {
    Object obj = mappingNamedList.remove(paramName);
    if (obj == null) {
      return null;
    }
    if (!(obj instanceof CharSequence)) {
      throw new SolrException(SERVER_ERROR, "'" + paramName + "' parameter must be a <str>");
    }
    if (mappingNamedList.get(paramName) != null) {
      throw new SolrException(SERVER_ERROR, "Each mapping must contain only one '" + paramName + "' <str>");
    }
    return obj.toString();
  }

  @Override
  protected Map<String,ContentStreamLoader> createDefaultLoaders(NamedList args) {
    SolrParams p = null;
    if (args != null) {
      p = SolrParams.toSolrParams(args);
    }
    Map<String,ContentStreamLoader> registry = new HashMap<>();
    registry.put("application/json", new JsonLoader(fieldMappers).init(p));
    registry.put("text/json", registry.get("application/json") );
    return registry;
  }

}
