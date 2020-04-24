package io.hologres.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

import java.nio.file.Paths;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/**
 * Simple batch job reading csv data and bulk loads into Hologres via batch sink.
 * Data is only available once all is finished.
 */
public class SimpleBulkloadJobTest extends JobTestBase {

	// Flink's Hologres batch table sink
	private static final String CREATE_HOLOGRES_TABLE = String.format(
			"create table orders(" +
			"  orders integer," +
			"  custkey integer," +
			"  orderstatus string," +
			"  totalprice decimal(13, 2)" +
			") with (" +
			"  'connector.type'='hologres'," +
			"  'connector.database' = '%s'," +
			"  'connector.table' = '%s'," +
			"  'connector.username' = '%s'," +
			"  'connector.password' = '%s'," +
			"  'connector.endpoint' = '%s'," +
			"  'connector.bulkload'='true'" +
			")", DATABASE, TABLE, USERNAME, PASSWORD, ENDPOINT);

	// Flink's csv table source
	private static final String CREATE_CSV_TABLE =
			"create table csv_orders(" +
			"  orders integer," +
			"  custkey integer," +
			"  orderstatus string," +
			"  totalprice decimal(13, 2)" +
			") with (" +
			"  'connector.type'='filesystem'," +
			"  'connector.path'='file:///%s'," +
			"  'format.type'='csv')";

	@Test
	public void test() throws Exception {
		String csv = Paths.get(
				this.getClass().getClassLoader().getResource("csv/orders.csv").toURI())
				.toFile()
				.getAbsolutePath();

		TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance()
				.useBlinkPlanner().inBatchMode().build());
		// Set job parallelism to be 1
		env.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);

		// Create Hologres streaming table sink
		env.sqlUpdate(CREATE_HOLOGRES_TABLE);
		env.sqlUpdate(String.format(CREATE_CSV_TABLE, csv));
		env.sqlUpdate("insert into orders select * from csv_orders");

		// Run the job
		env.execute("Hologres Test");
	}
}
