package io.hologres.flink.example;

import io.hologres.flink.ordergen.OrderTableSource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;

public class HologresSinkExample {
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("e", "endpoint", true, "Hologres endpoint");
    options.addOption("u", "username", true, "Username");
    options.addOption("p", "password", true, "Password");
    options.addOption("d", "database", true, "Database");
    options.addOption("t", "tablename", true, "Table name");

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
    String endPoint = commandLine.getOptionValue("endpoint");
    String userName = commandLine.getOptionValue("username");
    String password = commandLine.getOptionValue("password");
    String database = commandLine.getOptionValue("database");
    String tableName = commandLine.getOptionValue("tablename");

    TableSchema schema = TableSchema.builder()
            .field("user_id", DataTypes.BIGINT())
            .field("user_name", DataTypes.STRING())
            .field("item_id", DataTypes.BIGINT())
            .field("item_name", DataTypes.STRING())
            .field("price", DataTypes.DECIMAL(38, 2))
            .field("province", DataTypes.STRING())
            .field("city", DataTypes.STRING())
            .field("longitude", DataTypes.STRING())
            .field("latitude", DataTypes.STRING())
            .field("ip", DataTypes.STRING())
            .field("sale_timestamp", Types.SQL_TIMESTAMP).build();

    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance()
            .useBlinkPlanner().inStreamingMode().build());

    String CREATE_HOLOGRES_TABLE = String.format(
            "create table orders(" +
                    "  user_id bigint," +
                    "  user_name string," +
                    "  item_id bigint," +
                    "  item_name string," +
                    "  price decimal(38,2)," +
                    "  province string," +
                    "  city string," +
                    "  longitude string," +
                    "  latitude string," +
                    "  ip string," +
                    "  sale_timestamp timestamp" +
                    ") with (" +
                    "  'connector'='hologres'," +
                    "  'dbname' = '%s'," +
                    "  'tablename' = '%s'," +
                    "  'username' = '%s'," +
                    "  'password' = '%s'," +
                    "  'endpoint' = '%s'" +
                    ")", database, tableName, userName, password, endPoint);
    // Using a random data generator, and write to Hologres directly
    env.sqlUpdate(CREATE_HOLOGRES_TABLE);

    env.insertInto("orders",  env.fromTableSource(new OrderTableSource(schema)));

    env.execute("Hologres Sink example");  }
}
