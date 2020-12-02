package io.hologres.flink.ordergen;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class OrderTableSource implements StreamTableSource<Row>  {
  private final TableSchema schema;

  public OrderTableSource(TableSchema schema) {
    this.schema = schema;
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
    return streamExecutionEnvironment.addSource(new OrdersSourceFunction()).returns(schema.toRowType());
  }

  @Override
  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public DataType getProducedDataType() {
    return schema.toRowDataType();
  }
}
