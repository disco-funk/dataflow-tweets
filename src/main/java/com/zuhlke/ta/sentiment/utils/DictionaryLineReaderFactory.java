package com.zuhlke.ta.sentiment.utils;

/**
 * Abstracts the setting of a dictionary reader globally for all
 * the application
 * 
 * @author hadoop
 *
 */
public class DictionaryLineReaderFactory {

	private static DictionaryLineReaderFactory instance;
	private DictionaryLineReader reader;
	
	private DictionaryLineReaderFactory() {
		super();
		reader = new LocalLineReader("dict/");
	}


	public static DictionaryLineReaderFactory getInstance(){
		if(instance == null){
			instance = new DictionaryLineReaderFactory();
		}
		return instance;
	}
	
	public DictionaryLineReader getReader(){
		return reader;
	}
	
	public void setReader(DictionaryLineReader reader){
		this.reader = reader;
	}
}
