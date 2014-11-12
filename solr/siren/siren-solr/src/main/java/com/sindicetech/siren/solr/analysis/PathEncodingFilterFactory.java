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

package com.sindicetech.siren.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import com.sindicetech.siren.analysis.filter.PathEncodingFilter;

import java.util.Map;

/**
 * Factory for {@link com.sindicetech.siren.analysis.filter.PathEncodingFilter}.
 *
 * <p>
 *
 * The parameter {@value #ATTRIBUTEWILDCARD_KEY} can be set to true to preserve the original token. In this case, the
 * filter will output two tokens, one with the attribute label prepended to the token value, and a second one with just
 * the token value. The default value is false.
 */
public class PathEncodingFilterFactory extends TokenFilterFactory {

  public static final String ATTRIBUTEWILDCARD_KEY = "attributeWildcard";

  private boolean attributeWildcard = false;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  public PathEncodingFilterFactory(final Map<String, String> args) {
    super(args);
    attributeWildcard = getBoolean(args, ATTRIBUTEWILDCARD_KEY, PathEncodingFilter.DEFAULT_PRESERVE_ORIGINAL);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  public void setAttributeWildcard(boolean attributeWildcard) {
    this.attributeWildcard = attributeWildcard;
  }

  @Override
  public TokenStream create(final TokenStream input) {
    PathEncodingFilter filter = new PathEncodingFilter(input);
    filter.setPreserveOriginal(attributeWildcard);
    return filter;
  }

}
