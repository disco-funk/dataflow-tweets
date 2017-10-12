package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.pipeline.WordTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordTokenizerImpl implements WordTokenizer {

	private Tokenizer tokenizer;
	private TokenStream stream;

	private final String URL_PATTERN = "^(https?|ftp|file)://.*";
	private final String EMAIL_PATTERN = "([^.@\\s]+)(\\.[^.@\\s]+)*@([^.@\\s]+\\.)+([^.@\\s]+)";
	private final String NUMBERS_PATTERN = "[0-9]*";

	public WordTokenizerImpl() {
		if (tokenizer == null || stream == null)
			setup();
	}

	private void setup() {
		tokenizer = new UAX29URLEmailTokenizer(Version.LUCENE_47, new StringReader(""));
		stream = tokenizer;
	}

	public String[] tokenize(String sentence) {
		if (sentence == null)
			return new String[] {};

		List<String> words = new ArrayList<String>();
		try {
			tokenizer.setReader(new StringReader(sentence));
			stream.reset();
			while (stream.incrementToken()) {
				Iterator<AttributeImpl> it = tokenizer
						.getAttributeImplsIterator();
				if (it.hasNext()) {
					AttributeImpl a = it.next();
					if (a instanceof CharTermAttributeImpl) {
						String word = ((CharTermAttributeImpl) a).toString();
						if(!word.matches(EMAIL_PATTERN) &&
								!word.matches(NUMBERS_PATTERN) && ! word.matches(URL_PATTERN))
							words.add(word);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				tokenizer.reset();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return words.toArray(new String[words.size()]);
	}
}
