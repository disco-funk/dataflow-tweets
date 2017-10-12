package com.zuhlke.ta.sentiment.utils;

public class POSUtils {
	
	public static boolean isVerb(String word){
		if(word.endsWith("VB") || word.endsWith("VBD") || word.endsWith("VBG")
				|| word.endsWith("VBN") || word.endsWith("VBP") ||  word.endsWith("VBZ"))
			return true;
		return false;
	}
	
	public static boolean isNoun(String word){
		if(word.endsWith("NN") || word.endsWith("NNS"))
			return true;
		return false;
	}

	public static boolean isAdjective(String word){
		if(word.endsWith("JJ") || word.endsWith("JJR") || word.endsWith("JJS"))
			return true;
		return false;
	}

	public static boolean isAdverb(String word){
		if(word.endsWith("RB") || word.endsWith("RBR") || word.endsWith("RBS") || word.endsWith("WRB"))
			return true;
		return false;
	}
	
	public static boolean isLimitWord(String word){
		if(word.endsWith("CC") || word.endsWith("IN") || word.endsWith("WDT") || 
					word.endsWith("WP") || word.endsWith("WP$"))
			return true;
		return false;
	}
	
	public static boolean isSubject(String word){
		if(word.endsWith("NN") || word.endsWith("NNS") || word.endsWith("NNP") || 
					word.endsWith("NNPS") || word.endsWith("PRP") || word.endsWith("PRP$"))
			return true;
		return false;
	}
	
	public static boolean isModal(String word){
		if(word.endsWith("MD"))
			return true;
		return false;
	}
	
	public static String stripWord(String word){
		return word.toLowerCase().split("_")[0];
	}
}
