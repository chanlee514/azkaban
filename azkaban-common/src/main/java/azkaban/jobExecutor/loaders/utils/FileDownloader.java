package azkaban.jobExecutor.loaders.utils;

import java.io.IOException;

public interface FileDownloader {
  String download(String remotePath, String localPath) throws IOException;
}
