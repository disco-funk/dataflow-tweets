package com.zuhlke.ta.sentiment.model;

public class Negator {
	private final String word;

	public Negator(String word) {
		this.word = word;
	}

	public String getWord() {
		return word;
	}

	@Override
	public String toString() {
		return "Negator [word=" + word + "]";
	}
}
