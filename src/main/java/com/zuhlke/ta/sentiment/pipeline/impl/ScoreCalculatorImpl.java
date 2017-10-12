package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.model.Intensifier;
import com.zuhlke.ta.sentiment.model.WeightedWord;
import com.zuhlke.ta.sentiment.pipeline.ScoreCalculator;

import java.util.List;


public class ScoreCalculatorImpl implements ScoreCalculator {

	private final int NEGATION_BIAS = 4;
	
    public float calculate(List<WeightedWord> input){
        float value = 0;
        for (WeightedWord weightedWord : input) {
            if (!weightedWord.isOpinionWord()) {
                continue;
            }
            float wordValue = weightedWord.getSentimentDegree();

            for (Intensifier intensifier : weightedWord.getIntensifiers()) {
                wordValue = wordValue * intensifier.getIntensity();
            }
            for (int i =0; i < weightedWord.getNegators().size(); i++) {
            	if(wordValue < 0)
            		wordValue = wordValue+NEGATION_BIAS;
            	else if (wordValue > 0)
            		wordValue = wordValue-NEGATION_BIAS;
            }

            value += wordValue;
        }
        return value;
    }
}
