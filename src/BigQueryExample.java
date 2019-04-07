import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.api.client.util.Charsets;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;

public class BigQueryExample {

	public static void main(String args[]) throws Exception {

		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * FROM `Prueba.Tabla1` ")
				.setUseLegacySql(false).build();
		
		queryConfig = QueryJobConfiguration.newBuilder("insert into `Prueba.Tabla1`(nombre,edad) values ('Andres',20) ")
				.setUseLegacySql(false).build();

		// Create a job ID so that we can safely retry.
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		// Wait for the query to complete.
		queryJob = queryJob.waitFor();

		// Check for errors
		if (queryJob == null) {
			throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
			// You can also look at queryJob.getStatus().getExecutionErrors() for all
			// errors, not just the latest one.
			throw new RuntimeException(queryJob.getStatus().getError().toString());
		}

		// Get the results.
		QueryResponse response = bigquery.getQueryResults(jobId);

		TableResult result = queryJob.getQueryResults();

		// Print all pages of the results.
		for (FieldValueList row : result.iterateAll()) {
			String url = row.get("nombre").getStringValue();
			long viewCount = row.get("edad").getLongValue();
			System.out.printf("url: %s views: %d%n", url, viewCount);
		}

		// ESCRITURA//
		TableId tableId = TableId.of("Prueba", "Tabla1");
		WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId)
				.setFormatOptions(FormatOptions.json()).build();
		// The location must be specified; other fields can be auto-detected.
		jobId = JobId.newBuilder().setLocation("US").build();
		// jobId = JobId.of(UUID.randomUUID().toString());
		TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
		// Write data to writer
//		try (OutputStream stream = Channels.newOutputStream(writer)) {
//			Files.copy(new File("c:/users/Carlos/prueba.json"), stream);
//		}
		
		 // Write data to writer
		 try {
		   writer.write(ByteBuffer.wrap("c:/users/Carlos/prueba.json".getBytes(Charsets.UTF_8)));
		 } finally {
		   writer.close();
		 }

		
		// Get load job
		Job job = writer.getJob();
		job = job.waitFor();
		LoadStatistics stats = job.getStatistics();
		System.out.println(stats.getOutputRows());

	}
}
