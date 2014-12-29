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
package org.apache.solr.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.query.TermsQParserPlugin;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.JoinQParserPlugin;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

import java.net.URL;

public abstract class QParserPlugin implements NamedListInitializedPlugin, SolrInfoMBean {
  /** internal use - name of the default parser */
  public static final String DEFAULT_QTYPE = LuceneQParserPlugin.NAME;

  /**
   * Internal use - name to class mappings of builtin parsers.
   * Each query parser plugin extending {@link QParserPlugin} has own instance of standardPlugins.
   * This leads to cyclic dependencies of static fields and to case when NAME field is not yet initialized.
   * This result to NPE during initialization.
   * For every plugin, listed here, NAME field has to be final and static.
   */
  public static final Object[] standardPlugins = {
    LuceneQParserPlugin.NAME, LuceneQParserPlugin.class,
    OldLuceneQParserPlugin.NAME, OldLuceneQParserPlugin.class,
    FunctionQParserPlugin.NAME, FunctionQParserPlugin.class,
    PrefixQParserPlugin.NAME, PrefixQParserPlugin.class,
    BoostQParserPlugin.NAME, BoostQParserPlugin.class,
    DisMaxQParserPlugin.NAME, DisMaxQParserPlugin.class,
    ExtendedDismaxQParserPlugin.NAME, ExtendedDismaxQParserPlugin.class,
    FieldQParserPlugin.NAME, FieldQParserPlugin.class,
    RawQParserPlugin.NAME, RawQParserPlugin.class,
    TermQParserPlugin.NAME, TermQParserPlugin.class,
    TermsQParserPlugin.NAME, TermsQParserPlugin.class,
    NestedQParserPlugin.NAME, NestedQParserPlugin.class,
    FunctionRangeQParserPlugin.NAME, FunctionRangeQParserPlugin.class,
    SpatialFilterQParserPlugin.NAME, SpatialFilterQParserPlugin.class,
    SpatialBoxQParserPlugin.NAME, SpatialBoxQParserPlugin.class,
    JoinQParserPlugin.NAME, JoinQParserPlugin.class,
    SurroundQParserPlugin.NAME, SurroundQParserPlugin.class,
    SwitchQParserPlugin.NAME, SwitchQParserPlugin.class,
    MaxScoreQParserPlugin.NAME, MaxScoreQParserPlugin.class,
    BlockJoinParentQParserPlugin.NAME, BlockJoinParentQParserPlugin.class,
    BlockJoinChildQParserPlugin.NAME, BlockJoinChildQParserPlugin.class,
    CollapsingQParserPlugin.NAME, CollapsingQParserPlugin.class,
    SimpleQParserPlugin.NAME, SimpleQParserPlugin.class,
    ComplexPhraseQParserPlugin.NAME, ComplexPhraseQParserPlugin.class,
    ReRankQParserPlugin.NAME, ReRankQParserPlugin.class,
    ExportQParserPlugin.NAME, ExportQParserPlugin.class,
    HashQParserPlugin.NAME, HashQParserPlugin.class
  };

  /** return a {@link QParser} */
  public abstract QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req);

  @Override
  public String getName() {
    // TODO: ideally use the NAME property that each qparser plugin has

    return this.getClass().getName();
  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public String getDescription() {
    return "";  // UI required non-null to work
  }

  @Override
  public Category getCategory() {
    return Category.QUERYPARSER;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return new URL[0];
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }
}


