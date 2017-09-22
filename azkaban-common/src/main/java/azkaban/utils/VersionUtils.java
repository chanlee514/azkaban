package azkaban.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class VersionUtils {

  private Props versionProps;
  private String version;
  private String versionDetails;

  private final List<String> VERSION_DETAILS_KEYS = Arrays.asList("version", "build_time", "azkaban_core", "riq_azkaban_plugins");

  public VersionUtils(Props serverProps) {
    String versionPropertiesPath = serverProps.getString("version.properties", null);
    if (versionPropertiesPath != null) {
      try {
        this.versionProps = new Props(null, versionPropertiesPath);
      } catch (IOException ignore) { }
    }
  }

  public String getVersion() {
    if (version == null) {
      if (versionProps != null) {
        version = versionProps.getString("version", "N/A");
      } else {
        version = "N/A";
      }
    }
    return version;
  }

  public String getVersionDetails() {
    if (versionDetails == null) {
      if (versionProps != null) {
        versionDetails = VERSION_DETAILS_KEYS.stream().map(key ->
                key + "=" + versionProps.getString(key, "N/A")
        ).collect(Collectors.joining(", "));
      } else {
        versionDetails = "N/A";
      }
    }
    return versionDetails;
  }
}
