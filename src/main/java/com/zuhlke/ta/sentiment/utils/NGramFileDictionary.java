package com.zuhlke.ta.sentiment.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements a dictionary of Ngrams.
 * This dictionary separates the word and
 * the score by a tab
 * 
 * @author hadoop
 *
 */
public class NGramFileDictionary implements Dictionary {

	private final Map<String, Float> words = new HashMap<String, Float>();

	public NGramFileDictionary(String dictPath) throws IOException {
		
		DictionaryLineReader reader = DictionaryLineReaderFactory.getInstance().getReader();
		
		List<String> lines = reader.readLines(dictPath);
		for (String line : lines) {
			String[] splitLine = StringUtils.split(line, ':');
			
			if(splitLine.length > 1){
				words.put(splitLine[0], Float.parseFloat(splitLine[1])); // Score is present
			}else{
				words.put(splitLine[0], 0f); // No score is present
			}
		}
	}
	
	@Override
	public float getWordWeight(String word) throws TokenNotFound {
		if(words.containsKey(word))
			return words.get(word);
		else
			throw new TokenNotFound("Word not found " +  word);
	}

	@Override
	public int getWordCount() {
		return words.size();
	}

	@Override
	public boolean contains(String word) {
		return words.containsKey(word);
	}
}
