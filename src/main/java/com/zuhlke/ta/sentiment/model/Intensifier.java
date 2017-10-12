package com.zuhlke.ta.sentiment.model;

public class Intensifier {
	private final String word;
	private final float intensity;

	public Intensifier(String word, float intensity) {
		super();
		this.word = word;
		this.intensity = intensity;
	}

	public String getWord() {
		return word;
	}

	public float getIntensity() {
		return intensity;
	}
}
