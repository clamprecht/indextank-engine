/*
 * Copyright (c) 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.flaptor.indextank.index.lsi;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Payload;

import com.flaptor.indextank.index.lsi.term.PayloadEncoder;

/**
 * "Tokenizes" the entire stream as a single token using
 * Lucene's {@link org.apache.lucene.analysis.KeywordTokenizer}.
 */
public final class PayloadAnalyzer extends ReusableAnalyzerBase {

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        TokenFilter filter = new Filter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
    }

    /*
    @Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		return new Filter(delegate.tokenStream(fieldName, reader));
	} */

    private static final class Filter extends TokenFilter {
		private TermAttribute termAtt;
		private PayloadAttribute payAtt;
	
		public Filter(TokenStream input) {
			super(input);
			termAtt = addAttribute(TermAttribute.class);
			payAtt = addAttribute(PayloadAttribute.class);
		}
	
		@Override
		public boolean incrementToken() throws IOException {
			boolean result = false;
			if (input.incrementToken()) {
				String docid = termAtt.term();
				termAtt.setTermBuffer(LsiIndex.PAYLOAD_TERM_TEXT);
				payAtt.setPayload(new Payload(PayloadEncoder.encodePayloadId(docid)));
				return true;
			}
			return result;
		}
	}
}
