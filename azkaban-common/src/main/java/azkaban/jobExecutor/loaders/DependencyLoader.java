package azkaban.jobExecutor.loaders;

import azkaban.utils.Props;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public abstract class DependencyLoader {

  public static final String TMP_DIR = "/tmp";
  public static final String UNIQUE_FILE_DOWNLOAD = "job.loader.uniqueFilename";

  /**
   * Download all dependencies
   * @return a list of paths
   */
  public abstract List<String> getDependencies();

  /**
   * Get a job dependency directory for this process
   * @param props Job properties
   * @return A path as a string
   */
  public static String getTempDirectory(Props props) {
    Path tempDirPath = Paths.get(TMP_DIR, "cachedJars");
    File tempDir = new File(tempDirPath.toString());
    if (!tempDir.exists()) {
      boolean successful = tempDir.mkdirs();
      if (!successful) throw new RuntimeException("Unable to create temp dir for dependency download");
    }
    return tempDir.getAbsolutePath();
  }

}
