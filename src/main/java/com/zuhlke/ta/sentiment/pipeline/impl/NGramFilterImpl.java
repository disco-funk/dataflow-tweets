package com.zuhlke.ta.sentiment.pipeline.impl;

import com.zuhlke.ta.sentiment.model.WeightedWord;
import com.zuhlke.ta.sentiment.pipeline.NGramFilter;
import com.zuhlke.ta.sentiment.utils.Dictionary;
import com.zuhlke.ta.sentiment.utils.DictionaryConstans;
import com.zuhlke.ta.sentiment.utils.NGramFileDictionary;
import com.zuhlke.ta.sentiment.utils.TokenNotFound;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.morph.SimpleStemmer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.zuhlke.ta.sentiment.utils.POSUtils.*;


public class NGramFilterImpl implements NGramFilter {

	
	private int maxL;
	private final Dictionary nounsDictionary;
	private final Dictionary adjDictionary;
	private final Dictionary advDictionary;
	private final Dictionary verbDictionary;
	private final Dictionary intDictionary;
	
	//private final edu.mit.jwi.Dictionary dict;
	//private final WordnetStemmer stemmer;
    private final SimpleStemmer stemmer;

	public NGramFilterImpl(int maxNgram) throws IOException {
		super();
		this.maxL = maxNgram;
		
		this.nounsDictionary = new NGramFileDictionary(DictionaryConstans.NOUNS_NGRAM_FILE);
		this.adjDictionary = new NGramFileDictionary(DictionaryConstans.ADJECTIVES_NGRAM_FILE);
		this.advDictionary = new NGramFileDictionary(DictionaryConstans.ADVERBS_NGRAM_FILE);
		this.verbDictionary = new NGramFileDictionary(DictionaryConstans.VERBS_NGRAM_FILE);
		this.intDictionary = new NGramFileDictionary(DictionaryConstans.INTENSIFIERS_NGRAM_FILE);
		
		//dict = new edu.mit.jwi.Dictionary(SentimentWordFinderImpl.class.getClassLoader().getResource("wordnet"));
		//dict.open();
		//stemmer = new WordnetStemmer(dict);
        stemmer = new SimpleStemmer();
	}


	@Override
	public List<WeightedWord> filterNgrams(List<WeightedWord> words) {
		
		List<WeightedWord> last    = new ArrayList<WeightedWord>();
		List<WeightedWord> current = new ArrayList<WeightedWord>();
		
		// Copy original list as previous
		for(WeightedWord word : words)
			last.add(word);
		
		for(int cNgram = 2; cNgram <= maxL; cNgram++){ // At least bigrams
			current = new ArrayList<WeightedWord>();
			int currPos=0;
			while(currPos < last.size()){
				// Try to generate n-grams out of the current words
				int length = 0;
				int pp = 0;
				List<WeightedWord> nGramTok = new ArrayList<WeightedWord>(cNgram);
				while(length < cNgram && currPos+pp < last.size()){
					WeightedWord word = last.get(currPos+pp);
					length += word.getLength();
					nGramTok.add(word);
					pp++;
				}
				
				if(length == cNgram){
					StringBuilder builder =  new StringBuilder();
					for (WeightedWord token : nGramTok)
						builder.append(lemmatize(token.getWord())).append(" ");
					// Search the ngram					
					WeightedWord weightedNgram = findNgram(builder.toString().trim().toLowerCase(), length);
					
					if(weightedNgram != null){ // we found an ngram, substitute the group
						current.add(weightedNgram);
						currPos += nGramTok.size();
					}else{ // Maintain the word
						current.add(last.get(currPos));
						currPos++;
					}
				}else{ // Ngram not found, go to the next
					current.add(last.get(currPos));
					currPos++;
				}
			}
			
			// Clear last
			last.clear();
			for(WeightedWord word : current)
				last.add(word);
		}
			
		return current;
	}
	
	private WeightedWord findNgram(String ngram, int length){
		WeightedWord out = null;
		float weight = 0.0f;
		
		try {
			weight = nounsDictionary.getWordWeight(ngram);
			out = new WeightedWord(ngram, length);
			out.setOpinionWord(true);
			out.setSentimentDegree(weight);
		} catch (TokenNotFound e) {
			try {
				weight = adjDictionary.getWordWeight(ngram);
				out = new WeightedWord(ngram, length);
				out.setOpinionWord(true);
				out.setSentimentDegree(weight);
			} catch (TokenNotFound e1) {
				try {
					weight = advDictionary.getWordWeight(ngram);
					out = new WeightedWord(ngram, length);
					out.setOpinionWord(true);
					out.setSentimentDegree(weight);
				} catch (TokenNotFound e2) {
					try {
						weight = verbDictionary.getWordWeight(ngram);
						out = new WeightedWord(ngram, length);
						out.setOpinionWord(true);
						out.setSentimentDegree(weight);
					} catch (TokenNotFound e3) {
						try {
							weight = intDictionary.getWordWeight(ngram);
							out = new WeightedWord(ngram, length);
							out.setOpinionWord(false);
							out.setIntensifier(true);
							out.setSentimentDegree(weight);
						} catch (TokenNotFound e4) {
							// Simply not found
						}
					}
				}
			}
		}
		
		return out;
	}
	
	private String lemmatize(String word){
		POS partofSpeech = null;
		
		if(isVerb(word)){
			partofSpeech = POS.VERB;
		}else if(isNoun(word)){
			partofSpeech = POS.NOUN;
		}else if(isAdjective(word)){
			partofSpeech = POS.ADJECTIVE;
		}else if(isAdverb(word)){
			partofSpeech = POS.ADVERB;
		}else{
			return stripWord(word); // It is an ngram, does not need lematization
		}

        word = stripWord(word);
        if(!word.isEmpty()){
	        List<String> stems = new ArrayList<String>();
	        stems.addAll(stemmer.findStems(word, partofSpeech));
	        stems.add(word);
	
			return stems.get(0); // Return the first stemming
        }else{
        	return "";
        }
	}
}
