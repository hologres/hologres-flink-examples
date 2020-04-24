package io.hologres.flink.examples;

/**
 * Test base for Hologres-Flink job.
 */
public class JobTestBase {
	// Make sure the following configs are in your environment variables
	// Or replace them with your plain strings
	static final String DATABASE = System.getenv("HOLO_TEST_DB");
	static final String USERNAME = System.getenv("HOLO_ACCESS_ID");
	static final String PASSWORD = System.getenv("HOLO_ACCESS_KEY");
	static final String ENDPOINT = System.getenv("HOLO_ENDPOINT");

	// Can be just table name if the table is in "public" schema
	// or "<schema>.<table>"
	static final String TABLE = "orders";

}
