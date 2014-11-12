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

package com.sindicetech.siren.analysis.filter;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.junit.Test;

import com.sindicetech.siren.analysis.TupleTokenizer;
import com.sindicetech.siren.analysis.filter.URINormalisationFilter;

public class TestURINormalisationFilter {

  private final Tokenizer _t = new TupleTokenizer(new StringReader(""));

  @Test
  public void testURI()
  throws Exception {
    this.assertNormalisesTo(_t, "<http://renaud.delbru.fr/>",
      new String[] { "renaud", "delbru", "http://renaud.delbru.fr/" }, new String[] { "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://renaud.delbru.fr>",
      new String[] { "renaud", "delbru", "http://renaud.delbru.fr" }, new String[] { "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://user@renaud.delbru.fr>",
      new String[] { "user", "renaud", "delbru", "http://user@renaud.delbru.fr" }, new String[] { "<URI>", "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://user:passwd@renaud.delbru.fr>",
      new String[] { "user", "passwd", "renaud", "delbru", "http://user:passwd@renaud.delbru.fr" }, new String[] { "<URI>", "<URI>", "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://renaud.delbru.fr:8080>",
      new String[] { "renaud", "delbru", "8080", "http://renaud.delbru.fr:8080" }, new String[] { "<URI>", "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://renaud.delbru.fr/page.html#fragment>",
      new String[] { "renaud", "delbru", "page", "html", "fragment", "http://renaud.delbru.fr/page.html#fragment" }, new String[] { "<URI>", "<URI>", "<URI>", "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(
      _t,
      "<http://renaud.delbru.fr/page.html?query=a+query&hl=en&start=20&sa=N>",
      new String[] { "renaud", "delbru", "page", "html", "query", "query", "start", "http://renaud.delbru.fr/page.html?query=a+query&hl=en&start=20&sa=N" }, new String[] { "<URI>", "<URI>", "<URI>", "<URI>", "<URI>", "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<mailto:renaud@delbru.fr>",
      new String[] { "renaud", "delbru", "mailto:renaud@delbru.fr" }, new String[] { "<URI>", "<URI>", "<URI>" });
    this.assertNormalisesTo(_t, "<http://xmlns.com/foaf/0.1/workplaceHomepage/>",
      new String[] { "xmlns", "foaf", "workplace", "Homepage", "http://xmlns.com/foaf/0.1/workplaceHomepage/" },
      new String[] { "<URI>", "<URI>", "<URI>", "<URI>", "<URI>" });
  }

  public void assertNormalisesTo(final Tokenizer t, final String input,
                                 final String[] expected)
   throws Exception {
     this.assertNormalisesTo(t, input, expected, null);
   }

   public void assertNormalisesTo(final Tokenizer t, final String input,
                                 final String[] expectedImages,
                                 final String[] expectedTypes)
   throws Exception {

     assertTrue("has CharTermAttribute", t.hasAttribute(CharTermAttribute.class));
     final CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);

     TypeAttribute typeAtt = null;
     if (expectedTypes != null) {
       assertTrue("has TypeAttribute", t.hasAttribute(TypeAttribute.class));
       typeAtt = t.getAttribute(TypeAttribute.class);
     }

     t.setReader(new StringReader(input));
     t.reset();

     final TokenStream filter = new URINormalisationFilter(t);

     for (int i = 0; i < expectedImages.length; i++) {

       assertTrue("token "+i+" exists", filter.incrementToken());

       assertEquals(expectedImages[i], termAtt.toString());

       if (expectedTypes != null) {
         assertEquals(expectedTypes[i], typeAtt.type());
       }

     }

     assertFalse("end of stream", filter.incrementToken());
     filter.end();
     filter.close();
   }

}
