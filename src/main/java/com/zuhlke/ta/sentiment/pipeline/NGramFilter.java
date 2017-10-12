package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;

/**
 * Detects usual Ngrams before proceeding to the sentiment
 * scoring.
 * 
 * If a ngram is detected, its components are fusioned
 * in the ngram as a sentiment word
 * 
 * @author hadoop
 *
 */
public interface NGramFilter {
	public List<WeightedWord> filterNgrams(List<WeightedWord> words);
}
