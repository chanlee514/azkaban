package azkaban.jobExecutor.loaders.utils;

public class TestFileDownloader implements FileDownloader {

  public String download(String url, String destination) {
    return destination;
  }

}
