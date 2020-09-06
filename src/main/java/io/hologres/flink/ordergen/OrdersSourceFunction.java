package io.hologres.flink.ordergen;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Random;

public class OrdersSourceFunction extends RichParallelSourceFunction<Row> {
  private final int arity = 11;
  private transient Faker faker;
  private Province[] provinces;
  private Random random = new Random();
  private boolean shouldContinue = true;

  private void init() {
    faker = new Faker(new Locale("zh-CN"), random);
    Gson gson = new Gson();
    ClassLoader classLoader = Province.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("china_cities.json");
    Reader reader = new InputStreamReader(inputStream);
    provinces = gson.fromJson(reader, Province[].class);
  }

  @Override
  public void run(SourceContext<Row> sourceContext) throws Exception {
    if (faker == null) {
      init();
    }
    while (shouldContinue) {
      Row row = new Row(arity);
      int city_idx = Math.abs(random.nextInt());
      Province province = provinces[city_idx % provinces.length];
      PrefectureCity prefectureCity =
              province.getPrefectureCities().get(city_idx % province.getPrefectureCities().size());
      City city = prefectureCity.getCities().get(city_idx % prefectureCity.getCities().size());
      row.setField(0, Math.abs(random.nextLong())); // user id
      row.setField(1, faker.name().name()); // user name
      row.setField(2, Math.abs(random.nextLong())); // item id
      row.setField(3, faker.commerce().productName()); // item name
      row.setField(4, new BigDecimal(faker.commerce().price()));  // price
      row.setField(5, province.getProvinceNameZh()); // province
      row.setField(6, prefectureCity.getPrefectureNameZh()); // city
      row.setField(7, city.getLongtitude()); // city longitude
      row.setField(8, city.getLatitude()); // city latitude
      row.setField(9, faker.internet().ipV4Address()); // ip
      row.setField(10, new Timestamp(System.currentTimeMillis())); // time
      sourceContext.collect(row);
    }
  }

  @Override
  public void cancel() {
    shouldContinue = false;
  }
}
