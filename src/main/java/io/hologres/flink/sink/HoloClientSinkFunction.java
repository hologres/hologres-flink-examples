package io.hologres.flink.sink;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.Record;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.postgresql.core.SqlCommandType;
import org.postgresql.model.TableSchema;

public class HoloClientSinkFunction extends RichSinkFunction<Row> {
  private final HoloConfig holoConfig;
  private final String tableName;
  private transient HoloClient holoClient;
  private transient TableSchema tableSchema;

  public HoloClientSinkFunction(HoloConfig holoConfig, String tableName) {
    this.holoConfig = holoConfig;
    this.tableName = tableName;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    Class.forName("org.postgresql.Driver");
    this.holoClient = new HoloClient(holoConfig);
    this.tableSchema = holoClient.getTableSchema(tableName);
  }

  @Override
  public void invoke(Row value, SinkFunction.Context context) throws Exception {
    Put put = new Put(tableSchema);
    Record record = put.getRecord();
    record.setType(SqlCommandType.INSERT);
    for (int i = 0; i < value.getArity(); i++) {
      record.setObject(i, value.getField(i));
    }
    holoClient.put(put);
  }

  @Override
  public void close() throws Exception {
    if (holoClient != null) {
      holoClient.close();
    }
  }
}
