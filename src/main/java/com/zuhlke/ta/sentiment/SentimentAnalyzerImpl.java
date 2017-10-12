package com.zuhlke.ta.sentiment;

import com.zuhlke.ta.prototype.SentimentAnalyzer;
import com.zuhlke.ta.sentiment.model.WeightedWord;
import com.zuhlke.ta.sentiment.pipeline.*;
import com.zuhlke.ta.sentiment.pipeline.impl.*;
import com.zuhlke.ta.sentiment.utils.SentenceDetector;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;


/**
 * Calculates the sentiment polarity and intensity
 * of a document. It is an implementation of the
 * work in. The POS tagger is tailored to written 
 * English 
 * 
 * Maite Taboada et al. Lexicon-Based Methos for Sentiment
 * Analysis. Compuational Linguistics 37, 267-307, 2011.
 * 
 * @author hadoop
 *
 */
public class SentimentAnalyzerImpl implements SentimentAnalyzer {

	private SentenceDetector sentenceDetector;
	private ScoreCalculator calculator;
	private WordTokenizer tokenizer;
	private POSTokenizer posTokenizer;
	private SentimentWordFinder wordFinder;
	private NGramFilter ngramFilter;
	
	private IrrealisFinder irrealisFinder;
	private NegativesFinder negativesFinder;
	private IntensifiersFinder intensifiersFinder;

	public SentimentAnalyzerImpl() {
		int maxNgram = 4; // max trimgrams

		try {
			sentenceDetector = SentenceDetector.getInstance();
			calculator = new ScoreCalculatorImpl();
			tokenizer = new WordTokenizerImpl();
			posTokenizer = new TwitterStanPOSTokenizerImp();
			wordFinder = new SentimentWordFinderImpl();
			ngramFilter = new NGramFilterImpl(maxNgram);
			irrealisFinder = new IrrealisFinderImpl();
			negativesFinder = new NegativesFinderImpl();
			intensifiersFinder = new IntensifiersFinderImpl();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Could not load WordNet");
		}
	}

	String[] getTokens(String sentence){
		String[] words = tokenizer.tokenize(sentence);
		StringBuilder builder =  new StringBuilder();
		for(String word : words)
			builder.append(word + " ");
		return posTokenizer.tokenize(builder.toString());
	}

	List<WeightedWord> getSentimentWords(String[] tokens){
		return wordFinder.find(tokens);
	}
	
	List<WeightedWord> getNgramFilteredWords(List<WeightedWord> words){
		return ngramFilter.filterNgrams(words);
	}
	
	public double getSentiment(String text) {
		String[] sentences = sentenceDetector.getSentences(text);
		float result = 0;
		for(String sentence: sentences){
			List<WeightedWord> input = getSentimentWords(getTokens(sentence));
			input = getNgramFilteredWords(input);
			input = irrealisFinder.find(input);
			input = intensifiersFinder.find(input);
			input = negativesFinder.find(input);
			
			//System.out.println(input);
			
			result += calculator.calculate(input);
		}
		return result;
	}

	public WordTokenizer getTokenizer() {
		return tokenizer;
	}

	public void setTokenizer(WordTokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}

	public ScoreCalculator getCalculator() {
		return calculator;
	}

	public void setCalculator(ScoreCalculator calculator) {
		this.calculator = calculator;
	}

	public SentimentWordFinder getWordFinder() {
		return wordFinder;
	}

	public void setWordFinder(SentimentWordFinder wordFinder) {
		this.wordFinder = wordFinder;
	}

	public SentenceDetector getSentenceDetector() {
		return sentenceDetector;
	}

	public void setSentenceDetector(SentenceDetector sentenceDetector) {
		this.sentenceDetector = sentenceDetector;
	}

	public IrrealisFinder getIrrealisFinder() {
		return irrealisFinder;
	}

	public void setIrrealisFinder(IrrealisFinder irrealisFinder) {
		this.irrealisFinder = irrealisFinder;
	}

	public NegativesFinder getNegativesFinder() {
		return negativesFinder;
	}

	public void setNegativesFinder(NegativesFinder negativesFinder) {
		this.negativesFinder = negativesFinder;
	}

	public IntensifiersFinder getIntensifiersFinder() {
		return intensifiersFinder;
	}

	public void setIntensifiersFinder(IntensifiersFinder intensifiersFinder) {
		this.intensifiersFinder = intensifiersFinder;
	}
}

