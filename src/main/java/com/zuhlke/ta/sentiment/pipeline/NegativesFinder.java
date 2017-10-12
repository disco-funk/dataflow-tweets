package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;


/**
 * Find negations associated with opinion workds
 * 
 * @author hadoop
 */
public interface NegativesFinder {
	List<WeightedWord> find(List<WeightedWord> input);
}
