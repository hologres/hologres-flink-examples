package io.hologres.flink.example;

import com.alibaba.hologres.client.HoloConfig;
import io.hologres.flink.ordergen.OrderTableSource;
import io.hologres.flink.sink.HoloClientSinkFunction;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;

public class HologresDataStreamExample {
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

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

    HoloConfig holoConfig = new HoloConfig();
    holoConfig.setJdbcUrl("jdbc:postgresql://" + endPoint + "/" + database);
    holoConfig.setUsername(userName);
    holoConfig.setPassword(password);

    OrderTableSource source = new OrderTableSource(schema);
    source.getDataStream(env)
            .addSink(new HoloClientSinkFunction(holoConfig, tableName))
            .name("hologres-example");
    env.execute();
  }
}
