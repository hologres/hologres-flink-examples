package io.hologres.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;

/**
 * Simple batch job reading data from Hologres.
 */
public class SimpleReadJobTest extends JobTestBase {

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
			"  'connector.endpoint' = '%s'" +
			")", DATABASE, TABLE, USERNAME, PASSWORD, ENDPOINT);

	@Test
	public void test() throws Exception {
		TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance()
				.useBlinkPlanner().inBatchMode().build());
		// Set job parallelism to be 1
		env.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);

		// Create Hologres streaming table sink
		env.sqlUpdate(CREATE_HOLOGRES_TABLE);
		Table t = env.sqlQuery("select count(*) from orders");

		List<Row> results = TableUtils.collectToList(t);

		System.out.println(
				String.format("There are %s records in table 'orders' in Hologres", results.get(0).toString()));
	}
}
