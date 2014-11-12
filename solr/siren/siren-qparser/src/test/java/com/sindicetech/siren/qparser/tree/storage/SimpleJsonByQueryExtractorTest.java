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
package com.sindicetech.siren.qparser.tree.storage;

import com.sindicetech.siren.analysis.LongNumericAnalyzer;
import com.sindicetech.siren.qparser.tree.ExtendedTreeQueryParser;
import com.sindicetech.siren.search.node.NodeBooleanClause.Occur;
import com.sindicetech.siren.search.node.NodeBooleanQuery;
import com.sindicetech.siren.search.node.NodePhraseQuery;
import com.sindicetech.siren.util.JSONDatatype;
import com.sindicetech.siren.util.XSDDatatype;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SimpleJsonByQueryExtractorTest {
  private static final String json =
      "{"
        + "\"title\": \"Unforgiven\","
        + "\"year\": 1992,"
        + "\"genre\": \"Western\","
        + "\"summary\": \"The town of Big Whisky is full of normal people villainy, man and myth.\","
        + "\"country\": \"USA\","

      + "\"director\": {"
          + "\"last_name\": \"Eastwood\","
          + "\"first_name\": \"Clint\","
          + "\"birth_date\": 1930"
      + "},"
      + "\"actors\": ["
        + "{"
            + "\"first_name\": \"Clint\","
            + "\"last_name\": \"Eastwood\","
            + "\"birth_date\": 1930,"
            + "\"role\": \"William Munny\""
        + "},"
        + "{"
            + "\"first_name\": \"Gene\","
            + "\"last_name\": \"Hackman\","
            + "\"birth_date\": 1930,"
            + "\"role\": \"Little Bill Dagget\""
        + "},"
        + "{"
            + "\"first_name\": \"Morgan\","
            + "\"last_name\": \"Ereeman\","
            + "\"birth_date\": 1937,"
            + "\"role\": \"Ned Logan\""
        + "}"
      + "]"
    + "}";

  private final ExtendedTreeQueryParser parser = new ExtendedTreeQueryParser();
  private final ObjectMapper mapper = new ObjectMapper();
  private SimpleJsonByQueryExtractor extractor;

  @Before
  public void init() {
    Map<String,Analyzer> analyzers = new HashMap<String,Analyzer>();
    analyzers.put(XSDDatatype.XSD_STRING, new StandardAnalyzer(Version.LUCENE_46));
    analyzers.put(JSONDatatype.JSON_FIELD, new WhitespaceAnalyzer(Version.LUCENE_46));
    analyzers.put(XSDDatatype.XSD_LONG, new LongNumericAnalyzer(8));
    parser.getKeywordQueryParser().setDatatypeAnalyzers(analyzers);
    extractor = new SimpleJsonByQueryExtractor();
  }

  @Test
  public void testExtractNode() throws QueryNodeException, ProjectionException {
    final String query = "{ \"node\" : { \"query\" : \"Eastwood\" } }";
    Query q =  parser.parse(query, "");
    // no variable whole json should be returned
    assertEquals(json, extractor.extractAsString(json, q));
  }

  @Test
  public void testExtractTwigNoVariable() throws QueryNodeException, ProjectionException {
    String query = "{"
    + "\"twig\" : { "
      + "\"root\" : \"director\","
       + "\"child\" : [{\"occur\":\"MUST\","
                  + "\"twig\": {"
                       + "\"child\":[{\"occur\":\"MUST\","
                          + "\"twig\": {"
                               + "\"root\":\"first_name\","
                              + "\"child\":["
                                    + "{\"occur\":\"MUST\","
                                          + "\"node\" : { \"query\" : \"Clint\" } "
                                     + "}"
                                      + "]"
                                   + "}"
                                 + "}"
                                + "]"
                           + "}"
                   + "}"
                  + "]           "
              + "}"
    + "}";
    Query q = parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(json, extractor.extractAsString(json, q));
  }

  @Test
  public void testExtractTwig() throws QueryNodeException, ProjectionException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"director\","
           + "\"child\" : [{\"occur\":\"MUST\","
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"first_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"Clint\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"
                                    + "]"
                               + "}"
                       + "}"
                       + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                      + "]"
                  + "}"
        + "}";
    Query q = parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(
        "{\"director\":{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930}}",
        extractor.extractAsString(json, q));
  }

  @Test
  public void testExtractTwigMultipleInArray() throws QueryNodeException, ProjectionException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"first_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"Clint\" } "
                                         + "}"
                                         + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                                          + "]"
                                       + "}"
                                     + "}"
                                    + "]"
                               + "}"
                       + "}"
                      + "]"
                  + "}"
        + "}";
    Query q =  parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals("[{\"first_name\":\"Clint\"}]",
        extractor.extractAsString(json, q));
  }

  @Test
  public void testExtractTwigMultiple() throws QueryNodeException, ProjectionException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"director\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"first_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"Clint\" } "
                                         + "}"
                                         + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                                          + "]"
                                       + "}"
                                     + "}"
                                    + "]"
                               + "}"
                       + "}"
                      + "]"
                  + "}"
        + "}";
    Query q =  parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals("{\"first_name\":\"Clint\"}", extractor.extractAsString(json, q));
  }

  @Test
  public void testBooleanAndTermQuery() throws ProjectionException {
    NodeBooleanQuery bq = new NodeBooleanQuery();
    NodePhraseQuery pq = new NodePhraseQuery();
    pq.add(new Term("summary", "Whisky"));
    pq.add(new Term("summary", "Big"));
    bq.add(pq, Occur.MUST);
    assertEquals(json, extractor.extractAsString(json, bq));
  }

  @Test
  public void testTwoPrimitiveVariables() throws QueryNodeException, ProjectionException, IOException{
        String twig0 =     "\"twig\": {"
            + "\"root\":\"title\""
            + ",\"child\":["
                        + "{\"occur\":\"MUST\""
                        + ",\"node\" : { \"query\" : \"Unforgiven\" }"
                        + "}"
                        + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                       + "]"
       + "}";

        String twig1 ="\"twig\": {"
            + "\"root\":\"country\""
            + ",\"child\":["
                        + "{\"occur\":\"MUST\""
                        + ",\"node\" : { \"query\" : \"USA\" }"
                        + "}"
                        + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                       + "]"
       + "}";
    Query q = parser.parse("{\"boolean\":{\"clause\":[{\"occur\":\"MUST\"," +twig0 +"},{\"occur\":\"MUST\"," + twig1 +"}]}}", "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree("{\"country\":\"USA\",\"title\":\"Unforgiven\"}"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testTwoVariablesPrimitiveAndObject() throws QueryNodeException, ProjectionException, IOException{
   String twig0 =     "\"twig\": {"
            + "\"root\":\"title\""
            + ",\"child\":["
                        + "{\"occur\":\"MUST\""
                        + ",\"node\" : { \"query\" : \"Unforgiven\" }"
                        + "}"
                        + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                       + "]"
       + "}"     ;

        String twig1 =  "\"twig\" : { "
              + "\"root\" : \"director\","
               + "\"child\" : [{\"occur\":\"MUST\","
                         //blank node
                          + "\"twig\": {"
                               + "\"child\":[{\"occur\":\"MUST\","
                                  + "\"twig\": {"
                                       + "\"root\":\"first_name\","
                                      + "\"child\":["
                                            + "{\"occur\":\"MUST\","
                                                  + "\"node\" : { \"query\" : \"Clint\" } "
                                             + "}"

                                              + "]"
                                           + "}"
                                         + "}"
                                        + "]"
                                   + "}"
                           + "}"
                          + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                          + "]"
                      + "}";

     Query q = parser.parse("{\"boolean\":{\"clause\":[{\"occur\":\"MUST\"," +twig0 +"},{\"occur\":\"MUST\"," + twig1 +"}]}}", "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(
        mapper
            .readTree("{\"title\":\"Unforgiven\",\"director\":{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930}}"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testTwoVariablesPrimitiveAndObjectInArray() throws QueryNodeException, ProjectionException, IOException{
   String twig0 =
       "\"twig\" : { "
       + "\"root\" : \"director\","
        + "\"child\" : [{\"occur\":\"MUST\","
                  //blank node
                   + "\"twig\": {"
                        + "\"child\":[{\"occur\":\"MUST\","
                           + "\"twig\": {"
                                + "\"root\":\"first_name\","
                               + "\"child\":["
                                     + "{\"occur\":\"MUST\","
                                           + "\"node\" : { \"query\" : \"Clint\" } "
                                      + "}"
                                      + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                                       + "]"
                                    + "}"
                                  + "}"
                                 + "]"
                            + "}"
                    + "}"
                   + "]"
               + "}";

        String twig1 =  "\"twig\" : { "
              + "\"root\" : \"actors\","
               + "\"child\" : [{\"occur\":\"MUST\","
                         //blank node
                          + "\"twig\": {"
                               + "\"child\":[{\"occur\":\"MUST\","
                                  + "\"twig\": {"
                                       + "\"root\":\"first_name\","
                                      + "\"child\":["
                                            + "{\"occur\":\"MUST\","
                                                  + "\"node\" : { \"query\" : \"Clint\" } "
                                             + "}"

                                              + "]"
                                           + "}"
                                         + "}"
                                        + "]"
                                   + "}"
                           + "}"
                          + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                          + "]"
                      + "}";

     Query q = parser.parse("{\"boolean\":{\"clause\":[{\"occur\":\"MUST\"," +twig0 +"},{\"occur\":\"MUST\"," + twig1 +"}]}}", "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(
        mapper
            .readTree("{\"first_name\":\"Clint\",\"actors\": [{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930,\"role\": \"William Munny\"}]}"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testExtractTwigObjectVariableInArray() throws QueryNodeException, ProjectionException, IOException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"first_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"Clint\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"
                                     + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                                    + "]"
                               + "}"
                       + "}"
                      + "]"
                  + "}"
        + "}";
    Query q =  parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree("[{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930,\"role\": \"William Munny\"}]"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testExtractTwigMultipleInArrayObjectVariable() throws QueryNodeException, ProjectionException, IOException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"last_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"e*\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"

                                    + "]"
                               + "}"
                       + "}"
                       + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                      + "]"
                  + "}"
        + "}";
    Query q =  parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree("[{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930,\"role\": \"William Munny\"},"
        + "{\"first_name\": \"Morgan\",\"last_name\": \"Ereeman\",\"birth_date\": 1937,\"role\": \"Ned Logan\"}]"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testSimpleNumericRangeNoVars() throws QueryNodeException, ProjectionException, IOException {
    String query = "{ \"twig\": "
        + "{ \"root\":\"year\","
        + "  \"child\": [{\"occur\":\"MUST\","
        + "               \"node\" : { \"query\" : \"http://www.w3.org/2001/XMLSchema#long([1991 TO 2000])\" }}]}}";

    Query q = parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree(json), mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testSelectByNumericRangeFromArray() throws QueryNodeException, ProjectionException, IOException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"birth_date\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" :  \"http://www.w3.org/2001/XMLSchema#long([* TO 1932])\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"

                                    + "]"
                               + "}"
                       + "}"
                       + ", {\"occur\" : \"MUST\", \"variable\" : {}}"
                      + "]"
                  + "}"
        + "}";
    Query q = parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree(" ["
        + "{"
            + "\"first_name\": \"Clint\","
            + "\"last_name\": \"Eastwood\","
            + "\"birth_date\": 1930,"
            + "\"role\": \"William Munny\""
        + "},"
        + "{"
            + "\"first_name\": \"Gene\","
            + "\"last_name\": \"Hackman\","
            + "\"birth_date\": 1930,"
            + "\"role\": \"Little Bill Dagget\""
        + "}]"), mapper.readTree(extractor.extractAsString(json, q)));
  }
  @Test
  public void testSelectByNumericRangeFromArrayPrimitive() throws QueryNodeException, ProjectionException, IOException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"birth_date\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" :  \"http://www.w3.org/2001/XMLSchema#long([* TO 1932])\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "},"
                                     +"{\"occur\":\"MUST\","
                                         + "\"twig\": {"
                                              + "\"root\":\"first_name\","
                                             + "\"child\":["
                                             + "{\"occur\" : \"MUST\", \"variable\" : {}}"
                                                     + "]"
                                                  + "}"
                                                + "}"
                                    + "]"
                               + "}"
                       + "}"

                      + "]"
                  + "}"
        + "}";
    Query q = parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree("["
        + "{\"first_name\": \"Clint\"},"
            + "{\"first_name\": \"Gene\"}"
        + "]"), mapper.readTree(extractor.extractAsString(json, q)));
  }

  @Test
  public void testExtractTwigObjectVariableInArrayTwoQueries() throws QueryNodeException, ProjectionException, IOException {
    String query = "{"
        + "\"twig\" : { "
          + "\"root\" : \"actors\","
           + "\"child\" : [{\"occur\":\"MUST\","
                     //blank node
                      + "\"twig\": {"
                           + "\"child\":[{\"occur\":\"MUST\","
                              + "\"twig\": {"
                                   + "\"root\":\"last_name\","
                                  + "\"child\":["
                                        + "{\"occur\":\"MUST\","
                                              + "\"node\" : { \"query\" : \"E*\" } "
                                         + "}"
                                          + "]"
                                       + "}"
                                     + "}"
                                     + ", {\"occur\" : \"MUST\", \"variable\" : {}},"
                                     +"{\"occur\":\"MUST\","
                                         + "\"twig\": {"
                                              + "\"root\":\"birth_date\","
                                             + "\"child\":["
                                                   + "{\"occur\":\"MUST\","
                                                         + "\"node\" : { \"query\" :  \"http://www.w3.org/2001/XMLSchema#long([* TO 1932])\" } "
                                                    + "}"
                                                     + "]"
                                                  + "}"
                                                + "}"
                                    + "]"
                               + "}"
                       + "}"
                      + "]"
                  + "}"
        + "}";
    Query q =  parser.parse(query, "");
    SimpleJsonByQueryExtractor extractor = new SimpleJsonByQueryExtractor();
    assertEquals(mapper.readTree("[{\"last_name\":\"Eastwood\",\"first_name\":\"Clint\",\"birth_date\":1930,\"role\": \"William Munny\"}]"),
        mapper.readTree(extractor.extractAsString(json, q)));
  }
}