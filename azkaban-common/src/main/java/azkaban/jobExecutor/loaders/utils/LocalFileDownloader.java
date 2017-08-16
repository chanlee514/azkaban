package azkaban.jobExecutor.loaders.utils;

import java.io.File;

public class LocalFileDownloader implements FileDownloader {
  public String download(String localPath, String destination) {
    File localFile = new File(localPath);
    if (!localFile.exists()){
      throw new RuntimeException("Local file does not exist: " + localPath);
    }
    return destination;
  }
}
