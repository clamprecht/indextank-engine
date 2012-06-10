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

package com.flaptor.indextank.query;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.util.Version;
import org.json.simple.JSONArray;

import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class IndexEngineAnalyzer extends Analyzer {
    private static final String STOPWORDS = "stopwords";
    private final ClassicAnalyzer classic;
//    private final StandardAnalyzer delegate;
    private final Set<String> stopwords;

    public IndexEngineAnalyzer(Map<Object, Object> configuration) {
        super();
        this.stopwords = getStopWords(configuration);
        this.classic = new ClassicAnalyzer(Version.LUCENE_36, stopwords);
//        this.delegate = new StandardAnalyzer(Version.LUCENE_36, stopwords);
    }

    public IndexEngineAnalyzer() {
        this(Maps.newHashMap());
    }

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = classic.tokenStream(fieldName, reader);
//        TokenStream result = delegate.tokenStream(fieldName, reader);
        result = new ASCIIFoldingFilter(result);

        return result;
    }

    // todo - implement ReusableAnalyzerBase instead

    /*
    @Override
    protected ReusableAnalyzerBase.TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
        final ClassicTokenizer src = new ClassicTokenizer(Version.LUCENE_36, reader);
        src.setMaxTokenLength(maxTokenLength);
        //src.setReplaceInvalidAcronym(replaceInvalidAcronym);
        TokenStream tok = new ClassicFilter(src);
        tok = new LowerCaseFilter(Version.LUCENE_36, tok);
        tok = new StopFilter(Version.LUCENE_36, tok, stopwords);
        return new ReusableAnalyzerBase.TokenStreamComponents(src, tok) {
            @Override
            protected boolean reset(final Reader reader) throws IOException {
                src.setMaxTokenLength(maxTokenLength);
                return super.reset(reader);
            }
        };
    } */

    /*
    @Override
    public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
        return tokenStream(fieldName, reader);
    } */

    private static Set<String> getStopWords(Map<Object, Object> analyzerConfiguration) {
        if (analyzerConfiguration.containsKey(STOPWORDS)) {
            JSONArray stopwordList = (JSONArray)analyzerConfiguration.get(STOPWORDS);
            Set<String> stopwords = new HashSet<String>(stopwordList.size());
            for (Object stopword : stopwordList){
                if ( !(stopword instanceof String) ) {
                    throw new IllegalArgumentException("Stopwords aren't Strings");
                }
                stopwords.add((String)stopword);
            }
            return stopwords;
        } else {
            return ImmutableSet.of();
        }
    }

    public static Analyzer buildAnalyzer(Map<Object, Object> configuration) {
        return new IndexEngineAnalyzer(configuration);
    }

}
