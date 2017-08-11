package azkaban.jobExecutor.loaders;

import azkaban.utils.Props;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class DependencyLoader {

  public static final String LOADER_TYPE = "job.loader.type";
  public static final String TMP_DIR = "/tmp";
  public static final String JOB_ID = "azkaban.job.id";
  protected final String UNIQUE_FILE_DOWNLOAD = "job.loader.uniqueFilename";

  public String getDependency(String url) {
    return "";
  }

  public static String getTempDirectory(Props props) {
    String jobId = props.getString(JOB_ID);
    Path tempDirPath = Paths.get(TMP_DIR, jobId);
    File tempDir = new File(tempDirPath.toString());
    if (!tempDir.exists()) {
      boolean successful = tempDir.mkdirs();
      if (!successful) throw new RuntimeException("Unable to create temp dir for dependency download");
    }
    return tempDir.getAbsolutePath();
  }

  public static DependencyLoader getLoader(Props props) {
    String loaderType = props.getString(LOADER_TYPE, "");
    switch (loaderType) {
      case "s3": return new RemoteDependencyLoader(props);
      // case "artifactory": return new ArtifactoryLoader(props);
      default: return new LocalLoader(props);
    }
  }

}
