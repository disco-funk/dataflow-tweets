package com.zuhlke.ta.sentiment.utils;

import java.util.List;

public class LocalLineReader implements DictionaryLineReader {

	private final String folder;
	
	public LocalLineReader(String folder) {
		super();
		this.folder = folder;
	}

	@Override
	public List<String> readLines(String filename) {
		return FileToListReader.readFile(folder + filename);
	}

}
