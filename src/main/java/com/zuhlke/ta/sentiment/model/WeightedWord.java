package com.zuhlke.ta.sentiment.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WeightedWord {
	private final String word;
    private boolean isOpinionWord;
    private boolean isLimitWord;
    private boolean isIntensifier;
	private float sentimentDegree;
	private int length; // It can be a n-gram
	private final List<Intensifier> intensifiers = new ArrayList<Intensifier>();
	private final List<Negator> negators = new ArrayList<Negator>();

	public WeightedWord(String word, int length) {
		this.word = word;
		this.length = length;
	}

	
	public int getLength() {
		return length;
	}


	public void setLength(int length) {
		this.length = length;
	}


	public boolean isIntensifier() {
		return isIntensifier;
	}

	public void setIntensifier(boolean isIntensifier) {
		this.isIntensifier = isIntensifier;
	}

	public boolean isLimitWord() {
		return isLimitWord;
	}

	public void setLimitWord(boolean isLimitWord) {
		this.isLimitWord = isLimitWord;
	}


	public boolean isOpinionWord() {
		return isOpinionWord;
	}

	public void setOpinionWord(boolean isOpinionWord) {
		this.isOpinionWord = isOpinionWord;
	}

	public float getSentimentDegree() {
		return sentimentDegree;
	}

	public void setSentimentDegree(float sentimentDegree) {
		this.sentimentDegree = sentimentDegree;
	}

	public String getWord() {
		return word;
	}

	public List<Intensifier> getIntensifiers() {
		return Collections.unmodifiableList(intensifiers);
	}

	public void addIntensifier(Intensifier intensifier) {
		intensifiers.add(intensifier);
	}

	public List<Negator> getNegators() {
		return Collections.unmodifiableList(negators);
	}

	public void addNegator(Negator negator) {
		negators.add(negator);
	}


	@Override
	public String toString() {
		return "WeightedWord [word=" + word + ", isOpinionWord="
				+ isOpinionWord + ", isLimitWord=" + isLimitWord
				+ ", isIntensifier=" + isIntensifier + ", sentimentDegree="
				+ sentimentDegree + ", length=" + length + ", intensifiers="
				+ intensifiers + ", negators=" + negators + "]";
	}
}
