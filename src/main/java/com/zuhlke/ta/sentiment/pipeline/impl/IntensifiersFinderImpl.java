package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.model.Intensifier;
import com.zuhlke.ta.sentiment.model.WeightedWord;
import com.zuhlke.ta.sentiment.pipeline.IntensifiersFinder;
import com.zuhlke.ta.sentiment.utils.Dictionary;
import com.zuhlke.ta.sentiment.utils.DictionaryConstans;
import com.zuhlke.ta.sentiment.utils.SingleFileDictionary;
import com.zuhlke.ta.sentiment.utils.TokenNotFound;

import java.io.IOException;
import java.util.List;

import static com.zuhlke.ta.sentiment.utils.POSUtils.stripWord;
import static java.lang.Math.max;


public class IntensifiersFinderImpl implements IntensifiersFinder {

	private Dictionary intensifiers;
	private static int SCOPE_LEN = 6;
	
	public IntensifiersFinderImpl() throws IOException {
		try {
			intensifiers = new SingleFileDictionary(DictionaryConstans.INTENSIFIERS_FILE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * For each sentiment word found, it finds potential 
	 * intensifiers that can modify 
	 */
	public List<WeightedWord> find(List<WeightedWord> words) {
		for (int i = 0; i < words.size(); i++) {
			WeightedWord word = words.get(i);

			if(word.isOpinionWord()){
				for (int j = max(0, i-1); j >= max(0, i-SCOPE_LEN); j--) {
					WeightedWord previousWord = words.get(j);
					if (isIntensifier(previousWord)) {
						// There are words that can act as sentiment words or
						// as intensifiers, change the role!!
						previousWord.setOpinionWord(false);
						previousWord.setSentimentDegree(0);
						
						// Add intensifier
						word.addIntensifier(createIdentifier(previousWord));
					}else if(isStopMark(previousWord)){
						break;
					}
				}
			}
		}
		
		return words;
	}
	
	private boolean isStopMark(WeightedWord word){
		return word.isLimitWord();
	}
	
	private Boolean isIntensifier(WeightedWord word) {
		String ww = stripWord(word.getWord());
		return getIntensifiers().contains(ww);
	}
	
	private Intensifier createIdentifier(WeightedWord word) {
		try {
			String ww = stripWord(word.getWord());
			float intensifierValue = getIntensifiers().getWordWeight(ww);
			Intensifier intensifier = new Intensifier(ww, intensifierValue);
			return intensifier;
		} catch (TokenNotFound e) {
			return null;
		}
	}
	
	public Dictionary getIntensifiers() {
		return intensifiers;
	}

	public void setIntensifiers(Dictionary intensifiers) {
		this.intensifiers = intensifiers;
	}
}
