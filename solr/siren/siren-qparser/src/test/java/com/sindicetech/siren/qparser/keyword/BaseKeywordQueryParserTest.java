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
package com.sindicetech.siren.qparser.keyword;

import com.sindicetech.siren.qparser.keyword.config.ExtendedKeywordQueryConfigHandler;
import com.sindicetech.siren.util.SirenTestCase;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.ConfigurationKey;
import org.apache.lucene.search.Query;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public abstract class BaseKeywordQueryParserTest {

  /**
   * Helper method to parse a query string using the provided
   * {@link ExtendedKeywordQueryParser}.
   */
  public Query parse(final ExtendedKeywordQueryParser parser, final HashMap<ConfigurationKey, Object> keys, final String query)
  throws QueryNodeException {
    if (keys != null) {
      final ExtendedKeywordQueryConfigHandler config = new ExtendedKeywordQueryConfigHandler();
      for (Entry<ConfigurationKey, Object> key: keys.entrySet()) {
        config.set(key.getKey(), key.getValue());
      }
      parser.setQueryConfigHandler(config);
    }
    return parser.parse(query, SirenTestCase.DEFAULT_TEST_FIELD);
  }

  /**
   * Helper method to parse a query string using the {@link StandardExtendedKeywordQueryParser}
   */
  public Query parse(final HashMap<ConfigurationKey, Object> keys, final String query) throws QueryNodeException {
    return this.parse(new StandardExtendedKeywordQueryParser(), keys, query);
  }

  protected void _assertSirenQuery(final Query expected, final String query)
  throws Exception {
    assertEquals(expected, this.parse(null, query));
    assertEquals(expected, this.parse(null, expected.toString()));
  }

  protected void _assertSirenQuery(final HashMap<ConfigurationKey, Object> keys,
                                   final Query expected,
                                   final String query)
  throws Exception {
    assertEquals(expected, parse(keys, query));
    assertEquals(expected, parse(keys, expected.toString()));
  }

  protected Properties loadQNamesFile(final String qnamesFile) throws IOException {
    final Properties qnames = new Properties();
    qnames.load(new FileInputStream(new File(qnamesFile)));
    return qnames;
  }

}
