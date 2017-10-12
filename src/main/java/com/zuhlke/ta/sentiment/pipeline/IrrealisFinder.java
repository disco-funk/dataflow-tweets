package com.zuhlke.ta.sentiment.pipeline;

import com.zuhlke.ta.sentiment.model.WeightedWord;

import java.util.List;


/**
 * Blocks an opinion workd which is in
 * an irrealis block (a phrase which does not
 * talk about reality)
 * 
 * @author hadoop
 *
 */
public interface IrrealisFinder {

	List<WeightedWord> find(List<WeightedWord> input);
}
