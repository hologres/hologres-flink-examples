package io.hologres.flink.examples;

import io.hologres.flink.datagen.DataGenTableSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.junit.Test;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/**
 * Simple streaming job writing randomly generate data into Hologres via streaming sink.
 * All the data ingested will be immediately available for query!
 * So keep this job running, and run some queries to see data ingestion is happening at real-time,
 * queries like "select count(*) from mytable", and you can see row number is growing.
 *
 * mydb=# select count(*) from orders;
 *  count
 * -------
 *   1719
 * (1 row)
 *
 * mydb=# select count(*) from orders;
 *  count
 * -------
 *   1826
 * (1 row)
 *
 * mydb=# select count(*) from orders;
 *  count
 * -------
 *   1955
 * (1 row)
 *
 */
public class SimpleStreamingJobTest extends JobTestBase {

	// Flink's Hologres streaming table sink
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
			"  'connector.endpoint' = '%s'" +
			")", DATABASE, TABLE, USERNAME, PASSWORD, ENDPOINT);

	// Schema of Flink's DataGen table source
	// This should match the schema of Hologres table sink above
	private static final TableSchema schema = TableSchema
			.builder()
			.field("orders", DataTypes.INT())
			.field("custkey", DataTypes.INT())
			.field("orderstatus", DataTypes.STRING())
			.field("totalprice", DataTypes.DECIMAL(13, 2))
			.build();

	@Test
	public void test() throws Exception {
		int rate = 100;

		TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance()
				.useBlinkPlanner().inStreamingMode().build());
		// Set job parallelism to be 1
		env.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);

		// Create Hologres streaming table sink
		env.sqlUpdate(CREATE_HOLOGRES_TABLE);
		// execute query
		env.insertInto("orders",  env.fromTableSource(DataGenTableSource.create(schema, rate)));

		// Run the job
		env.execute("Hologres Test");
	}
}
