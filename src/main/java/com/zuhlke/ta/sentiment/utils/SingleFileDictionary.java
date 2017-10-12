package com.zuhlke.ta.sentiment.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * Reads a dictionary. The dictionary can be present
 * in the local file system or accesible in other
 * remote accesible location for cluster execution
 * 
 * @author hadoop
 *
 */
public class SingleFileDictionary implements Dictionary {

	private final Map<String, Float> words = new HashMap<String, Float>();

	public SingleFileDictionary(String dictPath) throws IOException {
		
		DictionaryLineReader reader = DictionaryLineReaderFactory.getInstance().getReader();
		
		List<String> lines = reader.readLines(dictPath);
		for (String line : lines) {
			String[] splitLine = StringUtils.split(line);
			
			if(splitLine.length > 1){
				words.put(splitLine[0], Float.parseFloat(splitLine[1])); // Score is present
			}else{
				words.put(splitLine[0], 0f); // No score is present
			}
		}
	}

	public float getWordWeight(String word) throws TokenNotFound {
		if(words.containsKey(word))
			return words.get(word);
		else
			throw new TokenNotFound("Word not found " +  word);
	}

	public int getWordCount() {
		return words.size();
	}

	public boolean contains(String word) {
		return words.containsKey(word);
	}
}
