package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.pipeline.POSTokenizer;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

/**
 * This POS tagger is tailored for Twitter text
 * 
 * @author hadoop
 *
 */
public class TwitterStanPOSTokenizerImp implements POSTokenizer {

	private MaxentTagger tagger = new MaxentTagger("models/gate-EN-twitter.model");
	
	
	public TwitterStanPOSTokenizerImp() {
		super();
	}


	@Override
	public String[] tokenize(String sentence) {
		return tagger.tagString(sentence).split("\\s");
	}

}
