package azkaban.jobExecutor.loaders.utils;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class S3FileDownloader implements FileDownloader {

  public static final String HADOOP_CONF_DIR_PROP = "hadoop.conf.dir"; // hadoop dir with xml conf files. from azkaban private.props
  public static final String HADOOP_INJECT_MASTER_IP = "hadoop-inject." + "hadoop.master.ip";

  private transient static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(S3FileDownloader.class);

  protected Configuration conf;

  public S3FileDownloader(Props jobProps) {
    try {
      conf = getHadoopConfigs(jobProps);
    } catch (IOException e) {
      logger.error("Invalid configuration for S3 Downloader: ", e);
    }
  }

  /**
   * Configure Hadoop endpoints and S3 access
   *
   * @param jobProps Job properties from job
   * @throws IOException
   */
  protected Configuration getHadoopConfigs(Props jobProps) throws IOException {
    Configuration hadoopConfigs = new Configuration();

    if (jobProps.containsKey(HADOOP_INJECT_MASTER_IP)) {
      hadoopConfigs.set("hadoop.master.ip", jobProps.getString(HADOOP_INJECT_MASTER_IP));
      throw new RuntimeException("Unable to instantiate S3FileDownloader");
    }
    return hadoopConfigs;
  }

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
        FileSystem s3Fs = s3Path.getFileSystem(conf);
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
