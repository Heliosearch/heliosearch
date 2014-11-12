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

package com.sindicetech.siren.solr.schema;

import com.sindicetech.siren.index.codecs.siren10.Siren10AForPostingsFormat;
import com.sindicetech.siren.solr.analysis.DatatypeAnalyzerFilterFactory;
import com.sindicetech.siren.solr.analysis.ExtendedJsonTokenizerFactory;
import com.sindicetech.siren.solr.analysis.PositionAttributeFilterFactory;
import com.sindicetech.siren.solr.analysis.SirenPayloadFilterFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.FieldProperties;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.QParser;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <code>ExtendedJsonField</code> is the basic type for configurable tree-based data
 * analysis.
 *
 * <p>
 *
 * This field type relies on a datatype analyzers configuration using the parameter
 * <code>datatypeConfig</code>,
 *
 * <p>
 *
 * This field type enforces certain field properties
 * by throwing a {@link SolrException} if the field type does not set properly
 * the properties. By default all the properties are set properly, i.e.,
 * a user should not modify these properties. This field type enforces also
 * the <code>postingsFormat</code> to <code>Siren10Afor</code>. The list of
 * enforced field properties are:
 * <ul>
 * <li> indexed = true
 * <li> tokenized = true
 * <li> multiValued = false
 * <li> omitTermFreqAndPositions = false
 * <li> omitPositions = false
 * <li> termVectors = false
 * </ul>
 *
 * <p>
 *
 * A {@link SchemaField} can overwrite these properties, however an exception
 * will be thrown when converting it to a Lucene's
 * {@link org.apache.lucene.document.FieldType} in
 * {@link #createField(SchemaField, Object, float)}.
 *
 * <p>
 *
 * This field type extends {@link TextField} to have the
 * {@link FieldProperties#OMIT_TF_POSITIONS} set to false by default.
 */
public class ExtendedJsonField extends TextField implements ResourceLoaderAware {

  private String datatypeAnalyzerConfigPath;

  private final AtomicReference<SirenDatatypeAnalyzerConfig> datatypeConfigRef = new AtomicReference<SirenDatatypeAnalyzerConfig>();

  private Version luceneDefaultVersion;

  public static String DATATYPECONFIG_KEY = "datatypeConfig";

  @Override
  protected void init(final IndexSchema schema, final Map<String,String> args) {
    // first call TextField.init to set omitTermFreqAndPositions to false
    super.init(schema, args);
    this.checkFieldTypeProperties();
    // initialise specific SIREn's properties
    this.datatypeAnalyzerConfigPath = args.remove(DATATYPECONFIG_KEY);

    if (datatypeAnalyzerConfigPath == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField types require a '"+DATATYPECONFIG_KEY+"' parameter: " + this.typeName);
    }

    // set the posting format
    args.put("postingsFormat", Siren10AForPostingsFormat.NAME);

    this.luceneDefaultVersion = schema.getDefaultLuceneMatchVersion();

    // instantiate the index analyzer associated to the field
    Analyzer indexAnalyzer = new TokenizerChain(new CharFilterFactory[0],
                                                this.getTokenizerFactory(args),
                                                new TokenFilterFactory[0]);
    indexAnalyzer = this.appendSirenFilters(indexAnalyzer, this.getDatatypes());
    this.setIndexAnalyzer(indexAnalyzer);

    super.init(schema, args);
  }

  protected TokenizerFactory getTokenizerFactory(final Map<String,String> args) {
    return new ExtendedJsonTokenizerFactory(args);
  }

  private void checkFieldTypeProperties() {
    if (this.isMultiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField types can not be multiValued: " +
                              this.typeName);
    }
    if (this.hasProperty(FieldProperties.OMIT_TF_POSITIONS)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField types can not omit term frequencies " +
                              "and positions: " + this.typeName);
    }
    if (this.hasProperty(FieldProperties.OMIT_POSITIONS)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField types can not omit positions: " +
                              this.typeName);
    }
    if (this.hasProperty(FieldProperties.STORE_TERMVECTORS)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField types can not store term vectors: " +
                              this.typeName);
    }
  }

  @Override
  public IndexableField createField(final SchemaField field, final Object value,
                                    final float boost) {
    if (!field.indexed()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must be indexed: " +
                              field.getName());
    }
    if (field.multiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances can not be multivalued: " +
                              field.getName());
    }
    if (field.omitTermFreqAndPositions()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must not omit term " +
                              "frequencies and positions: " + field.getName());
    }
    if (field.omitPositions()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must not omit term " +
                              "positions: " + field.getName());
    }
    if (field.storeTermVector()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances can not store term vectors: " +
                              field.getName());
    }
    return super.createField(field, value, boost);
  }

  @Override
  protected IndexableField createField(final String name, final String val,
                                       final org.apache.lucene.document.FieldType type,
                                       final float boost){

    if (!type.indexed()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must be indexed: " + name);
    }
    if (!type.tokenized()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must be tokenised: " + name);
    }
    if (!type.indexOptions().equals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances must not omit term " +
                              "frequencies and positions: " + name);
    }
    if (type.storeTermVectors()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "ExtendedJsonField instances can not store term vectors: " +
                              name);
    }

    return super.createField(name, val, type, boost);
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return new Analyzer() { // create an analyzer to avoid NPE with Luke request - see #83
      @Override
      protected TokenStreamComponents createComponents(final String s, final Reader reader) {
        throw new SolrException(SolrException.ErrorCode.FORBIDDEN, "Unsupported operation.");
      }
    };
  }

  @Override
  public SortField getSortField(final SchemaField field, final boolean reverse) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Unsupported operation. Can not sort on SIREn field: "
                            + field.getName());
  }

  @Override
  public void write(final TextResponseWriter writer, final String name, final IndexableField f)
  throws IOException {
    writer.writeStr(name, f.stringValue(), true);
  }

  @Override
  public Query getFieldQuery(final QParser parser, final SchemaField field,
                             final String externalVal) {
    // Not useful for now in SIREn
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Not implemented operation."
                            + field.getName());
  }

  @Override
  public Query getRangeQuery(final QParser parser, final SchemaField field,
                             final String part1, final String part2,
                             final boolean minInclusive, final boolean maxInclusive) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Unsupported operation. Can not do range on SIREn field: "
                            + field.getName());
  }

  @Override
  public Analyzer getMultiTermAnalyzer() {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Unsupported operation. Use getAnalyzer instead.");
  }

  @Override
  public boolean getAutoGeneratePhraseQueries() {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Unsupported operation.");
  }

  public Map<String, Datatype> getDatatypes() {
    return this.datatypeConfigRef.get() == null ? new HashMap<String, Datatype>() : this.datatypeConfigRef.get().getDatatypes();
  }

  /**
   * Load the datatype analyzer config file specified by the schema.
   * <br/>
   * This should be called whenever the datatype analyzer configuration file changes.
   */
  private void loadDatatypeConfig(final SolrResourceLoader resourceLoader) {
    InputStream is;
    log.info("Loading datatype analyzer configuration file at " + datatypeAnalyzerConfigPath);

    try {
      is = resourceLoader.openResource(datatypeAnalyzerConfigPath);
    } catch (final IOException e) {
      log.error("Error loading datatype analyzer configuration file at " + datatypeAnalyzerConfigPath, e);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }

    try {
      final SirenDatatypeAnalyzerConfig newConfig =
              new SirenDatatypeAnalyzerConfig(resourceLoader,
                      datatypeAnalyzerConfigPath, new InputSource(is),
                      this.luceneDefaultVersion);
      log.info("Read new datatype analyzer configuration " + newConfig);
      datatypeConfigRef.set(newConfig);
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (final IOException ignored) {
        }
      }
    }
  }

  /**
   * Load the datatype config when resource loader initialized.
   *
   * @param resourceLoader The resource loader.
   */
  @Override
  public void inform(final ResourceLoader resourceLoader) {
    // load the datatypes
    this.loadDatatypeConfig((SolrResourceLoader) resourceLoader);

    // Register the datatypes in the DatatypeAnalyzerFilterFactory instance
    final TokenizerChain chain = (TokenizerChain) this.getIndexAnalyzer();
    for (TokenFilterFactory tokenFilterFactory : chain.getTokenFilterFactories()) {
      if (tokenFilterFactory instanceof DatatypeAnalyzerFilterFactory) {
        ((DatatypeAnalyzerFilterFactory) tokenFilterFactory).register(this.getDatatypes());
      }
    }
  }

  /**
   * Append the mandatory SIREn filters, i.e.,
   * {@link DatatypeAnalyzerFilterFactory},
   * {@link PositionAttributeFilterFactory} and
   * {@link SirenPayloadFilterFactory}, to the tokenizer chain.
   * <br/>
   * The first time this is called, it will create a
   * {@link com.sindicetech.siren.solr.analysis.DatatypeAnalyzerFilterFactory} with no datatype registered. The datatypes
   * will be loaded and registered later, when {@link #inform(org.apache.lucene.analysis.util.ResourceLoader)} is
   * called.
   * <br/>
   * This is necessary to avoid having to call {@link org.apache.solr.schema.IndexSchema#refreshAnalyzers()}.
   * The {@link org.apache.solr.schema.IndexSchema} will have a reference to the SIREn field's analyzer, and
   * to the {@link com.sindicetech.siren.solr.analysis.DatatypeAnalyzerFilterFactory}. When the datatypes will be loaded,
   * we will access this reference, and register the datatypes.
   */
  protected Analyzer appendSirenFilters(final Analyzer analyzer,
                                        final Map<String, Datatype> datatypes) {
    if (!(analyzer instanceof TokenizerChain)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Invalid index analyzer '" + analyzer.getClass() + "' received");
    }

    final TokenizerChain chain = (TokenizerChain) analyzer;
    // copy the existing list of token filters
    final TokenFilterFactory[] old = chain.getTokenFilterFactories();
    final TokenFilterFactory[] filterFactories = new TokenFilterFactory[old.length + 3];
    System.arraycopy(old, 0, filterFactories, 0, old.length);
    // append the datatype analyzer filter factory
    final DatatypeAnalyzerFilterFactory datatypeFactory = new DatatypeAnalyzerFilterFactory(new HashMap<String, String>());
    datatypeFactory.register(datatypes);
    filterFactories[old.length] = datatypeFactory;
    // append the position attribute filter factory
    filterFactories[old.length + 1] = new PositionAttributeFilterFactory(new HashMap<String,String>());
    // append the siren payload filter factory
    filterFactories[old.length + 2] = new SirenPayloadFilterFactory(new HashMap<String,String>());
    // create a new tokenizer chain with the updated list of filter factories
    return new TokenizerChain(chain.getCharFilterFactories(),
      chain.getTokenizerFactory(), filterFactories);
  }

}

