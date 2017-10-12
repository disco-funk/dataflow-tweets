package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;


/**
 * Finds the intensifiers associated to an
 * opinion word
 * 
 * @author hadoop
 */
public interface IntensifiersFinder {
	
	List<WeightedWord> find(List<WeightedWord> input);
	
}
