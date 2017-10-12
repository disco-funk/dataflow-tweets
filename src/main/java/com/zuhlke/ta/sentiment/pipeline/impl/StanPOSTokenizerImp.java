package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.pipeline.POSTokenizer;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

/**
 * This POS tagger is trained for English
 * 
 * @author hadoop
 *
 */
public class StanPOSTokenizerImp implements POSTokenizer {

	private MaxentTagger tagger = new MaxentTagger("models/english-left3words-distsim.tagger");
	
	
	public StanPOSTokenizerImp() {
		super();
	}


	@Override
	public String[] tokenize(String sentence) {
		return tagger.tagString(sentence).split("\\s");
	}

}
