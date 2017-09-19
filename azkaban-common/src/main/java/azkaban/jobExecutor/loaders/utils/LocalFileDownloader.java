package azkaban.jobExecutor.loaders.utils;

import java.io.File;
import java.nio.file.Paths;

public class LocalFileDownloader implements FileDownloader {

  private String baseDir;

  public LocalFileDownloader(String jobDir) {
    baseDir = jobDir;
  }

  public String download(String localPath, String destination) {
    File localDirFile = new File(Paths.get(baseDir, localPath).toAbsolutePath().toString());
    File absolutePathFile = new File(localPath);
    if (!localDirFile.exists() && !absolutePathFile.exists()){
      throw new RuntimeException("Local file does not exist: " + localPath);
    }
    return destination;
  }
}
