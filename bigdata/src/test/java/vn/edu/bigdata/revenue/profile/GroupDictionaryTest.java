package vn.edu.bigdata.revenue.profile;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;
import vn.edu.bigdata.revenue.input.InputManifest;

class GroupDictionaryTest {
  @Test
  void stableIdsAndProvenanceAreValidated() {
    ProfileReport p = new ProfileReport();
    p.countByGroup = Map.of("2", 1L, "1", 3L);
    p.groupMode = "CATEGORY_ID";
    p.policyHash = InputManifest.policyHash();
    p.inputFingerprint = "input";
    GroupDictionary d = GroupDictionary.from(p);
    d.validate("input", "CATEGORY_ID");
    assertArrayEquals(new String[] {"1", "2"}, d.groups);
    assertEquals(0, d.index().get("1"));
    assertThrows(IllegalArgumentException.class, () -> d.validate("other", "CATEGORY_ID"));
    d.groups[1] = "1";
    assertThrows(IllegalArgumentException.class, () -> d.validate("input", "CATEGORY_ID"));
  }
}
