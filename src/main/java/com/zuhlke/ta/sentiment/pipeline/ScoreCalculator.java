package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;

/**
 * Calculates the final sentiment score of
 * a document
 * 
 * @author hadoop
 *
 */
public interface ScoreCalculator {
	float calculate(List<WeightedWord> input);
}
