package com.zuhlke.ta.sentiment.utils;

/**
 * Represents a exception in which a word was not found
 * @author hadoop
 *
 */
public class TokenNotFound extends Exception {

	/**
	 * Generated serial UID
	 */
	private static final long serialVersionUID = -3520587477089772674L;

	public TokenNotFound(String message) {
		super(message);
	}

}
