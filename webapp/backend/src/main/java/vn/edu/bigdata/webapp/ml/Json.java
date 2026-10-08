package vn.edu.bigdata.webapp.ml;

import com.fasterxml.jackson.databind.JsonNode;

final class Json {
  private Json() {}

  static double[] doubles(JsonNode array) {
    if (!array.isArray()) throw new IllegalStateException("Cần mảng số, nhận " + array);
    double[] out = new double[array.size()];
    for (int i = 0; i < out.length; i++) out[i] = array.get(i).asDouble();
    return out;
  }
}
