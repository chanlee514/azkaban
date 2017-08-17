package azkaban.jobExecutor.loaders.utils;

import azkaban.utils.Props;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
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

  protected AmazonS3Client client;

  public S3FileDownloader(Props jobProps) {
    client = getS3Client();
  }

  private AWSCredentials getAWSCredentials() {
    DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
    return providerChain.getCredentials();
  }

  private AmazonS3Client getS3Client() {
    return new AmazonS3Client(getAWSCredentials()).withRegion(Regions.US_WEST_2);
  }

  public void setClient(AmazonS3Client client) {
    this.client = client;
  }

  public static String[] bucketAndKey(String url) {
    int firstSlash = url.indexOf("/");
    String bucket = url.substring(0, firstSlash);
    String key = url.substring(firstSlash + 1);
    String[] bucketAndKey = {bucket, key};
    return bucketAndKey;
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
        ;
        // download the jar to local
        logger.info("Specified from s3: " + url);
        logger.info("Downloading file to " + localPath);
        String[] bucketKey = bucketAndKey(url);
        String key = bucketKey[0];
        String bucket = bucketKey[1];
        File localFile = new File(localPath);
        client.getObject(new GetObjectRequest(bucket, key), localFile);
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
