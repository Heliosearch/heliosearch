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
package com.sindicetech.siren.qparser.tree;

import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

/**
 * Exception thrown by the {@link ExtendedTreeQueryParser} if an error occurs during
 * parsing.
 */
public class ParseException extends QueryNodeParseException {

  private static final long serialVersionUID = 1L;

  public ParseException(final String message, final Throwable throwable) {
    super(new MessageImpl(QueryParserMessages.INVALID_SYNTAX, message), throwable);
  }

  public ParseException(final String message) {
    super(new MessageImpl(QueryParserMessages.INVALID_SYNTAX, message));
  }

}
