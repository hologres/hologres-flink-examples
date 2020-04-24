package io.hologres.flink.datagen;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * A {@link StreamTableSource} that emits each number from a given interval exactly once,
 * possibly in parallel. See {@link StatefulSequenceSource}.
 */
public class DataGenTableSource implements StreamTableSource<Row> {

    private final DataGenerator[] fieldGenerators;
    private final TableSchema schema;
    private final long rowsPerSecond;

    public DataGenTableSource(DataGenerator[] fieldGenerators, TableSchema schema, long rowsPerSecond) {
        this.fieldGenerators = fieldGenerators;
        this.schema = schema;
        this.rowsPerSecond = rowsPerSecond;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(createSource()).returns(schema.toRowType());
    }

    @VisibleForTesting
    public DataGeneratorSource<Row> createSource() {
        return new DataGeneratorSource<>(new RowGenerator(fieldGenerators), rowsPerSecond);
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public DataType getProducedDataType() {
        return schema.toRowDataType();
    }

    private static class RowGenerator implements DataGenerator<Row> {

        private final DataGenerator[] fieldGenerators;

        private RowGenerator(DataGenerator[] fieldGenerators) {
            this.fieldGenerators = fieldGenerators;
        }

        @Override
        public void open(
                FunctionInitializationContext context,
                RuntimeContext runtimeContext) throws Exception {
            for (DataGenerator fieldGenerator : fieldGenerators) {
                fieldGenerator.open(context, runtimeContext);
            }
        }

        @Override
        public boolean hasNext() {
            for (DataGenerator generator : fieldGenerators) {
                if (!generator.hasNext()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Row next() {
            Row row = new Row(fieldGenerators.length);
            for (int i = 0; i < fieldGenerators.length; i++) {
                row.setField(i, fieldGenerators[i].next());
            }
            return row;
        }
    }

    public static DataGenTableSource create(TableSchema schema, int rowsPerSecond) {
        DataGenerator[] fieldGenerators = new DataGenerator[schema.getFieldCount()];
        for (int i = 0; i < fieldGenerators.length; i++) {
            fieldGenerators[i] = createRandomGenerator(schema.getFieldDataType(i).get());
        }

        return new DataGenTableSource(fieldGenerators, schema, rowsPerSecond);
    }

    private static DataGenerator createRandomGenerator(DataType type) {
        switch (type.getLogicalType().getTypeRoot()) {
            case BOOLEAN:
                return RandomGenerator.booleanGenerator();
            case CHAR:
            case VARCHAR:
                return RandomGenerator.stringGenerator(100);
            case TINYINT:
                return RandomGenerator.byteGenerator(Byte.MIN_VALUE, Byte.MAX_VALUE);
            case SMALLINT:
                return RandomGenerator.shortGenerator(Short.MIN_VALUE, Short.MAX_VALUE);
            case INTEGER:
                return RandomGenerator.intGenerator(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case BIGINT:
                return RandomGenerator.longGenerator(Long.MIN_VALUE, Long.MAX_VALUE);
            case FLOAT:
                return RandomGenerator.floatGenerator(Float.MIN_VALUE, Float.MAX_VALUE);
            case DOUBLE:
                return RandomGenerator.doubleGenerator(Double.MIN_VALUE, Double.MAX_VALUE);
            case DATE:
                return RandomGenerator.dateGenerator();
            case DECIMAL:
                return RandomGenerator.decimalRandomGenerator(13, 2);
            default:
                throw new ValidationException("Unsupported type: " + type);
        }
    }
}
