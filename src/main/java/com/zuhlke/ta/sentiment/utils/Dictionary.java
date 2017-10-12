package com.zuhlke.ta.sentiment.utils;

/**
 * Implementation of a dictionary.
 * Each word contains a score used by
 * the calculator to sum up the sentiment
 * 
 * @author hadoop
 *
 */
public interface Dictionary {
    float getWordWeight(String word) throws TokenNotFound;
    int getWordCount();
    boolean contains(String word);
}
