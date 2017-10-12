package com.zuhlke.ta.sentiment.pipeline;


/**
 * Detects the POS of each work and append the 
 * Penn Treebank tag to it
 * 
 * @author hadoop
 *
 */
public interface POSTokenizer {
	String[] tokenize(String sentence);
}
