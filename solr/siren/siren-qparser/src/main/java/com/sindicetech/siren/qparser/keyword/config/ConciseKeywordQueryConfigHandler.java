/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.sindicetech.siren.qparser.keyword.config;

import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;

/**
 * This is used to configure parameters for the
 * {@link com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser}.
 */
public class ConciseKeywordQueryConfigHandler extends ExtendedKeywordQueryConfigHandler {

  /**
   * Class holding the {@link com.sindicetech.siren.qparser.keyword.ConciseKeywordQueryParser} options.
   */
  final public static class ConciseKeywordConfigurationKeys {

    /**
     * Key used to set the attribute that will be applied to the node queries.
     */
    final public static ConfigurationKey<String> ATTRIBUTE = ConfigurationKey.newInstance();

    /**
     * Key used in {@link com.sindicetech.siren.qparser.keyword.builders.concise.ConciseMatchNoDocsQueryNodeBuilder} to set
     * the field of the attribute queries.
     *
     * @see com.sindicetech.siren.qparser.keyword.builders.concise.ConciseMatchNoDocsQueryNodeBuilder
     */
    final public static ConfigurationKey<String> FIELD = ConfigurationKey.newInstance();

  }

  public ConciseKeywordQueryConfigHandler() {
    super();
    // twig query not supported in the concise
    this.set(KeywordConfigurationKeys.ALLOW_TWIG, false);
  }

}
