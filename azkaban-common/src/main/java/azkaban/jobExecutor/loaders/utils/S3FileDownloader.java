package azkaban.jobExecutor.loaders.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class S3FileDownloader implements FileDownloader {

  private transient static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(S3FileDownloader.class);

  /**
   * Download a s3 file to local disk
   *
   * @param url       Must be an S3 path
   * @param localPath local target
   */
  public String download(String url, String localPath) throws IOException {
    File remoteFile = new File(url);
    String filename = remoteFile.getName();

    try {
      URI jarURI = new URI(url);
      if (jarURI.getScheme() != null) { // location is a s3 path
        // download the jar to local
        logger.info("Specified from s3: " + url);
        logger.info("Downloading file to " + localPath);
        Path s3Path = new Path("s3a://" + jarURI.getHost() + jarURI.getPath());
        FileSystem s3Fs = s3Path.getFileSystem(new Configuration());
        s3Fs.copyToLocalFile(s3Path, new Path(localPath));
        return localPath;
      } else {
        throw new RuntimeException("Could not find file on local filesystem, and location is not an s3 path. Aborting...");
      }
    } catch (URISyntaxException e) {
      logger.error("Invalid URI: " + url);
      throw new IOException("Unable to download due to invalid URI " + url);
    }
  }

}
