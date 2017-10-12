package com.zuhlke.ta.sentiment.utils;

import java.util.List;

/**
 * Reads the lines of a dictionary
 * 
 * @author hadoop
 *
 */
public interface DictionaryLineReader {
	List<String> readLines(String filename);
}
