package com.zuhlke.ta.prototype;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class Importer {
    private static final int BATCH_SIZE = 100;
    private static final int MAX_TWEETS_TO_IMPORT = 10000;

    private List<Tweet> batch = new ArrayList<>(BATCH_SIZE);
    private TweetService tweetService;

    public Importer(TweetService tweetService) {
        this.tweetService = tweetService;
    }

    public void importTweetsFrom(File file) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(file));
        int count = 0;
        String s = in.readLine();
        while (s != null && count < MAX_TWEETS_TO_IMPORT) {
            count++;
            String[] parts = s.split("\\|");

            if (parts.length == 12) bufferTweet(parts);

            s = in.readLine();
        }
        flushTweets();
        in.close();
        System.out.println("imported " + count + " tweets");
    }

    private void bufferTweet(String[] parts) {
        try {
            batch.add(new Tweet(getId(parts[1]), parts[3], parts[2], parseDate(parts[4])));
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Could not parse number " + parts[1]);
        }
        if (batch.size() == BATCH_SIZE) flushTweets();
    }

    private long getId(String part) {
        return Strings.isNullOrEmpty(part) ? 0L : Long.parseLong(part);
    }

    private LocalDate parseDate(String part) throws ParseException {
        if (part.length() < 10) throw new ParseException("Could not parse date " + part, 0);
        try {
            return LocalDate.parse(part.substring(0, 10));
        } catch (DateTimeException e) {
            throw new ParseException("Could not parse date " + part + e.getMessage(), 0);
        }
    }

    private void flushTweets() {
        tweetService.importTweets(batch);
        batch = new ArrayList<>(BATCH_SIZE);
    }
}
