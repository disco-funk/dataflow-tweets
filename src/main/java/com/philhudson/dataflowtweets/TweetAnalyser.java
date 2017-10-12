package com.philhudson.dataflowtweets;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.zuhlke.ta.prototype.SentimentAnalyzer;
import com.zuhlke.ta.sentiment.SentimentAnalyzerImpl;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

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
        options.setRunner(DataflowRunner.class);
        options.setProject(PROJECT_ID);
        options.setTempLocation(STAGING_LOCATION);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("content").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sentiment").setType("FLOAT"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(BigQueryIO.read()
                .from("genuine-axe-182507:intalert.test"))
                .apply(
                    MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via((TableRow c) -> {
                            TableRow newRow = new TableRow();
                            String content = (String) c.get("content");
                            newRow.set("content", content);
                            newRow.set("timestamp", c.get("timestamp"));
                            double sentiment = analyzer.getSentiment(content);
                            newRow.set("sentiment", sentiment);
                            return newRow;
                        })
                )
                .apply(BigQueryIO.writeTableRows()
                        .to("genuine-axe-182507:intalert.analysed")
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withSchema(schema));

        pipeline.run();
    }
}