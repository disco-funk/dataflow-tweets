package com.zuhlke.ta.sentiment.pipeline;

/**
 * Find all the tokens present in
 * a sentence. 
 * 
 * @author hadoop
 *
 */
public interface WordTokenizer {

	String[] tokenize(String sentence);
}
