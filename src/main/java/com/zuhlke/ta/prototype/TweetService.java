package com.zuhlke.ta.prototype;

import java.util.Collection;

public interface TweetService {
    SentimentTimeline analyzeSentimentOverTime(Query q);
    void importTweets(Collection<Tweet> tweets);
}
