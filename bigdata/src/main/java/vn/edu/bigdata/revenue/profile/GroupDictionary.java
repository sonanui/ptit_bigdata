package vn.edu.bigdata.revenue.profile;

import java.nio.charset.StandardCharsets;
import java.util.*;
import vn.edu.bigdata.revenue.input.InputManifest;

/** Dense IDs have identical ownership on every mapper and reducer. */
public final class GroupDictionary {
  public int schemaVersion = 1;
  public String inputFingerprint, groupMode, policyHash, checksum;
  public String[] groups = new String[0];

  public static GroupDictionary from(ProfileReport profile) {
    GroupDictionary d = new GroupDictionary();
    d.inputFingerprint = profile.inputFingerprint;
    d.groupMode = profile.groupMode;
    d.policyHash = profile.policyHash;
    d.groups = profile.countByGroup.keySet().stream().sorted().toArray(String[]::new);
    d.checksum = d.computedChecksum();
    return d;
  }

  public String computedChecksum() {
    StringBuilder canonical =
        new StringBuilder()
            .append(schemaVersion)
            .append('|')
            .append(inputFingerprint)
            .append('|')
            .append(groupMode)
            .append('|')
            .append(policyHash);
    for (String key : groups) canonical.append('\n').append(key.length()).append(':').append(key);
    return InputManifest.sha256(canonical.toString());
  }

  public void validate(String fingerprint, String mode) {
    if (schemaVersion != 1
        || groups == null
        || groups.length > 100000
        || !Objects.equals(fingerprint, inputFingerprint)
        || !Objects.equals(mode, groupMode)
        || !Objects.equals(InputManifest.policyHash(), policyHash)
        || !Objects.equals(checksum, computedChecksum()))
      throw new IllegalArgumentException("Dictionary provenance/checksum mismatch");
    String previous = null;
    for (String group : groups) {
      if (group == null
          || group.isEmpty()
          || group.getBytes(StandardCharsets.UTF_8).length > 256
          || group.indexOf('\t') >= 0
          || group.indexOf('\n') >= 0
          || group.indexOf('\r') >= 0
          || (previous != null && previous.compareTo(group) >= 0))
        throw new IllegalArgumentException("Dictionary groups must be sorted and unique");
      previous = group;
    }
  }

  public Map<String, Integer> index() {
    Map<String, Integer> result = new HashMap<>();
    for (int i = 0; i < groups.length; i++) result.put(groups[i], i);
    return result;
  }
}
