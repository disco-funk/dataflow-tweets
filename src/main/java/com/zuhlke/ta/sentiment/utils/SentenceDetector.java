package com.zuhlke.ta.sentiment.utils;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract superclass of all the text analyzers which need 
 * sentence detection and tokenization as previous steps
 * 
 * @author dama
 *
 */
public class SentenceDetector {

	private SentenceDetectorME sentenceDetector;	
	public final static String SENTENCE_MODEL = "en-sent.bin";
	private static SentenceDetector instance;
	
	
	private SentenceDetector() {
		super();
		InputStream smodelIn = null;
		try {
			// Load sentence detector model
			smodelIn = SentenceDetector.class.getResourceAsStream(SENTENCE_MODEL);
			SentenceModel sentenceModel = new SentenceModel(smodelIn);	
			sentenceDetector = new SentenceDetectorME(sentenceModel);
		}catch (IOException e) {
			throw new RuntimeException(e);
		}finally {
			if (smodelIn != null) {
				try {
					smodelIn.close();
				}
				catch (IOException e) {
				}
			}
		}
	}

	public static SentenceDetector getInstance(){
		if(instance == null){
			instance = new SentenceDetector();
		}
		return instance;
	}
	
	public String[] getSentences(String text) {
		return sentenceDetector.sentDetect(text);
	}
	
}
