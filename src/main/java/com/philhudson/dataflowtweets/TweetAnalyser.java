package com.philhudson.dataflowtweets;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.zuhlke.ta.prototype.SentimentAnalyzer;
import com.zuhlke.ta.sentiment.SentimentAnalyzerImpl;

import java.util.ArrayList;
import java.util.List;

public class TweetAnalyser {

    private static final String PROJECT_ID = "genuine-axe-182507";
    private static final String STAGING_LOCATION = "gs://tweets-dataflow/staging/";
    private static SentimentAnalyzer analyzer = new SentimentAnalyzerImpl();

    public static void main(String[] args) {

        BigQueryOptions options = PipelineOptionsFactory
                .create()
                .as(BigQueryOptions.class);
        options.setRunner(BlockingDataflowPipelineRunner.class);
        options.setProject(PROJECT_ID);
        options.setTempLocation(STAGING_LOCATION);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("content").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sentiment").setType("FLOAT"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(BigQueryIO.Read
                .named("Load Tweet Input")
                .from("genuine-axe-182507:intalert.test"))
                .apply(ParDo.named("Analyse Sentiment").of(new DoFn<TableRow, TableRow>() {
                    @Override
                    public void processElement(ProcessContext c) {
                        TableRow newRow = new TableRow();
                        String content = (String) c.element().get("content");
                        newRow.set("content", content);
                        newRow.set("timestamp", c.element().get("timestamp"));
                        double sentiment = analyzer.getSentiment(content);
                        newRow.set("sentiment", sentiment);
                        c.output(newRow);
                    }
                }))
                .apply(BigQueryIO.Write
                        .named("Write Output Analysed Tweets")
                        .to("genuine-axe-182507:intalert.analysed")
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withSchema(schema));

        pipeline.run();
    }
}