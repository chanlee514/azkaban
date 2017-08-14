package azkaban.jobExecutor.loaders;

import azkaban.utils.Props;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public abstract class DependencyLoader {

  public static final String TMP_DIR = "/tmp";
  public static final String JOB_ID = "azkaban.job.id";
  public static final String UNIQUE_FILE_DOWNLOAD = "job.loader.uniqueFilename";

  public abstract List<String> getDependencies();

  /**
   * Get a job dependency directory for this process
   * @param props Job properties
   * @return
   */
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

}
