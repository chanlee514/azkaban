package azkaban.jobExecutor.loaders;

import azkaban.jobExecutor.loaders.utils.FileDownloader;
import azkaban.utils.Props;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


public class RemoteDependencyLoader extends DependencyLoader {

  private transient static Logger logger = Logger.getLogger(RemoteDependencyLoader.class);
  private boolean unique;
  private String targetDirectory;
  private FileDownloader downloader;

  public RemoteDependencyLoader(Props props) {
    unique = props.getBoolean(UNIQUE_FILE_DOWNLOAD, false);
    targetDirectory = getTempDirectory(props);
  }

  public void setDownloader(FileDownloader downloader) {
    this.downloader = downloader;
  }

  public String getFile(String path, String destination, boolean unique) throws IOException {
    try {
      File remoteFile = new File(path);
      String filename = remoteFile.getName();

      if (unique) {
        String extension = FilenameUtils.getExtension(filename);
        filename = java.util.UUID.randomUUID().toString() + "." + extension;
      }

      String localPath = Paths.get(destination, filename).toAbsolutePath().toString(); // new location on local
      File localFile = new File(localPath);

      if (!localFile.exists()) {
        return downloader.download(path, localPath);
      }
      return localPath;
    } catch (Exception e) {
      logger.error("Unable to download " + path + " from remote location, saw error \n", e);
      throw new IOException("File download failed for " + path);
    }
  }

  /**
   * Get the dependency
   * @param url Must be some sort of url - s3, artifactory, who cares
   * @return
   */
  @Override
  public String getDependency(String url) {
    try {
      if (this.downloader == null) {
        throw new RuntimeException("No downloader set");
      }
      return getFile(url, targetDirectory, unique);
    } catch (IOException e) {
      logger.error("Exception loading dependency: ", e);
      throw new RuntimeException("Unable to locate dependencies.");
    }
  }

}
