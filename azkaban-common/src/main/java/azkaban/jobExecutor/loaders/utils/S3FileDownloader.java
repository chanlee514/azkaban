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

  public static final String HADOOP_MASTER_IP = "hadoop.master.ip";
  public static final String HADOOP_INJECT_MASTER_IP = "hadoop-inject." + HADOOP_MASTER_IP;

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
      hadoopConfigs.set(HADOOP_MASTER_IP, jobProps.getString(HADOOP_INJECT_MASTER_IP));
      throw new RuntimeException("Unable to instantiate S3FileDownloader");
    }

    String masterIp = jobProps.get(HADOOP_MASTER_IP);

    hadoopConfigs.set("fs.s3.awsAccessKeyId", "AKIAJC4V44W4JL5DTZ7Q");
    hadoopConfigs.set("fs.s3.awsSecretAccessKey", "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");
    hadoopConfigs.set("fs.s3a.awsAccessKeyId", "AKIAJC4V44W4JL5DTZ7Q");
    hadoopConfigs.set("fs.s3a.awsSecretAccessKey", "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");
    hadoopConfigs.set("fs.s3n.awsAccessKeyId", "AKIAJC4V44W4JL5DTZ7Q");
    hadoopConfigs.set("fs.s3n.awsSecretAccessKey", "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");
    hadoopConfigs.set("s3.awsAccessKeyId", "AKIAJC4V44W4JL5DTZ7Q");
    hadoopConfigs.set("s3.awsSecretAccessKey", "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");
    hadoopConfigs.set("fs.defaultFS", masterIp + ":8020");
    return hadoopConfigs;
  }

  /**
   * Download a s3 file to local disk
   *
   * @param url       Must be an S3 path
   * @param localPath local target
   */
  public String download(String url, String localPath) throws IOException {
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
