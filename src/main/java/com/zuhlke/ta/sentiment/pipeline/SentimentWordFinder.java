package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;


/**
 * Finds sentiment words in a set of tokens that
 * come from the document.
 * 
 * @author hadoop
 *
 */
public interface SentimentWordFinder {
	List<WeightedWord> find(String... words);
}
