package org.apache.solr.handler.component;

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

import java.io.IOException;

import org.apache.solr.search.facet.FacetModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("rawtypes")
public class HSFacetComponent extends SearchComponent {
  public static Logger log = LoggerFactory.getLogger(HSFacetComponent.class);

  public static final String COMPONENT_NAME = "hs_facet";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    FacetModule.prepare(rb);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    FacetModule.process(rb);
  }


  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }


  @Override
  public String getDescription() {
    return "Heliosearch Faceting";
  }

  @Override
  public String getSource() {
    return null;
  }

}

