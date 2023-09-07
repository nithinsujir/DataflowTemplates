/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.util.ValueProviderUtils.eitherOrValueProvider;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.templates.SpannerToJson.SpannerToJsonOptions;
import com.google.cloud.teleport.templates.common.SpannerConverters;
import com.google.cloud.teleport.templates.common.SpannerConverters.CreateTransactionFnWithTimestamp;
import com.google.cloud.teleport.templates.common.SpannerConverters.SpannerReadOptions;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which export data from Spanner table to GCS in json format. It exports a
 * Spanner table using <a
 * href="https://cloud.google.com/spanner/docs/reads#read_data_in_parallel">Batch API</a>, which
 * creates multiple workers in parallel for better performance. The result is written to a JSON file
 * in Google Cloud Storage. By default, schema won't be exported if required use optional arguments.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Spanner_to_GCS_Text.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Spanner_TO_JSON",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Spanner to JSON Files on Cloud Storage",
    description =
        "A pipeline which reads in Cloud Spanner table and writes it to Cloud Storage as JSON"
            + " files.",
    optionsClass = SpannerToJsonOptions.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-cloud-storage",
    contactInformation = "https://cloud.google.com/support")
public class SpannerToJson {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToJson.class);

  /** Custom PipelineOptions. */
  public interface SpannerToJsonOptions
      extends PipelineOptions, SpannerReadOptions, FilesystemWriteOptions {

    @TemplateParameter.GcsWriteFolder(
        order = 1,
        optional = true,
        description = "Cloud Storage temp directory for storing JSON files",
        helpText = "The Cloud Storage path where the temporary JSON files can be stored.",
        example = "gs://your-bucket/your-path")
    ValueProvider<String> getJSONTempDirectory();

    @SuppressWarnings("unused")
    void setJSONTempDirectory(ValueProvider<String> value);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [HIGH,MEDIUM,LOW].")
    ValueProvider<RpcPriority> getSpannerPriority();

    void setSpannerPriority(ValueProvider<RpcPriority> value);

    @TemplateParameter.Boolean(
        order = 3,
        optional = true,
        description = "Export Schema",
        helpText = "To export the schema of the table along with data")
    @Default.Boolean(false)
    ValueProvider<Boolean> getExportSchema();

    void setExportSchema(ValueProvider<Boolean> value);
  }

  /**
   * Runs a pipeline which reads in Records from Spanner, and writes the JSON to TextIO sink.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    LOG.info("Starting pipeline setup");
    PipelineOptionsFactory.register(SpannerToJsonOptions.class);
    SpannerToJsonOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToJsonOptions.class);

    FileSystems.setDefaultPipelineOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(options.getSpannerHost())
            .withProjectId(options.getSpannerProjectId())
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId())
            .withRpcPriority(options.getSpannerPriority())
            .withDataBoostEnabled(options.getDataBoostEnabled());

    PTransform<PBegin, PCollection<ReadOperation>> spannerExport =
        SpannerConverters.ExportTransformFactory.create(
            options.getSpannerTable(),
            spannerConfig,
            options.getTextWritePrefix(),
            options.getSpannerSnapshotTime(),
            options.getColumnNamesAliasMap(),
            options.getExportSchema());

    /* CreateTransaction and CreateTransactionFn classes in LocalSpannerIO
     * only take a timestamp object for exact staleness which works when
     * parameters are provided during template compile time. They do not work with
     * a Timestamp valueProvider which can take parameters at runtime. Hence a new
     * ParDo class CreateTransactionFnWithTimestamp had to be created for this
     * purpose.
     */
    PCollectionView<Transaction> tx =
        pipeline
            .apply("Setup for Transaction", Create.of(1))
            .apply(
                "Create transaction",
                ParDo.of(
                    new CreateTransactionFnWithTimestamp(
                        spannerConfig, options.getSpannerSnapshotTime())))
            .apply("As PCollectionView", View.asSingleton());

    PCollection<String> json =
        pipeline
            .apply("Create export", spannerExport)
            // We need to use LocalSpannerIO.readAll() instead of LocalSpannerIO.read()
            // because ValueProvider parameters such as table name required for
            // LocalSpannerIO.read() can be read only inside DoFn but LocalSpannerIO.read() is of
            // type PTransform<PBegin, Struct>, which prevents prepending it with DoFn that reads
            // f
            .apply(
                "Read all records",
                LocalSpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig))
            .apply(
                "Struct To JSON",
                MapElements.into(TypeDescriptors.strings())
                    .via(struct -> (new SpannerConverters.StructJSONPrinter()).print(struct)));

    ValueProvider<ResourceId> tempDirectoryResource =
        ValueProvider.NestedValueProvider.of(
            eitherOrValueProvider(options.getJSONTempDirectory(), options.getTextWritePrefix()),
            (SerializableFunction<String, ResourceId>) s -> FileSystems.matchNewResource(s, true));

    json.apply(
        "Write to storage",
        TextIO.write()
            .to(options.getTextWritePrefix())
            .withSuffix(".json")
            .withTempDirectory(tempDirectoryResource));

    pipeline.run();
    LOG.info("Completed pipeline setup");
  }
}
