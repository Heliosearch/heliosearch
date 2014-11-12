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
package com.sindicetech.siren.solr.response;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.response.transform.TransformerWithContext;

/**
 * A simple {@link TransformerFactory} for creating an instance of the
 * {@link SirenProjectionTransformer}.
 * 
 * It has to be registered in solrconfig*.xml as follows:
 *   &lt;transformer name=&quot;sirenProjection&quot; class=&quot;com.sindicetech.siren.solr.response.SirenProjectionTransformerFactory&quot; /&gt;
 *   
 * @see SirenProjectionTransformer
 */
public class SirenProjectionTransformerFactory extends TransformerFactory
{
  private static SirenProjectionTransformer transformer = new SirenProjectionTransformer();

  @Override
  public TransformerWithContext create(String field, SolrParams params, SolrQueryRequest req) {
    return transformer;
  }
}


