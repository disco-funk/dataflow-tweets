package com.zuhlke.ta.prototype;

/**
 * Calculates the sentiment polarity and intensity
 * of a document.
 *
 * @author hadoop
 *
 */
public interface SentimentAnalyzer {
    double getSentiment(String text);
}
