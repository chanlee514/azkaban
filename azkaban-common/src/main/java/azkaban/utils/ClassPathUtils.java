package azkaban.utils;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


/**
 * Utility functions for getting class paths for all the process jobs
 */
public class ClassPathUtils {

  protected static Logger logger = Logger.getLogger(ClassPathUtils.class);

  protected static Configuration conf;

  protected static String HADOOP_PREFIX = "hadoop.master.ip";

  /**
   * public constructor
   *
   * @param props
   */
  public ClassPathUtils(Props props) {
    conf = new Configuration();
    if (props.containsKey(HADOOP_PREFIX))
    conf.set(HADOOP_PREFIX, props.getString(HADOOP_PREFIX));
  }

  /**
   * Return configuration
   *
   * @return
   */
  protected Configuration getConf() {
    return conf;
  }

  /**
   * Utility function for loading files from S3/local
   *
   * @param paths list of urls (local or s3)
   * @param job_path returned by getPath() in ProcessJob, directory path of the current job
   * @param jar_dir jar directory
   * @return
   * @throws RuntimeException
   */
  public List<String> getFromLocalOrS3Concurrent(List<String> paths, String job_path, String jar_dir) throws RuntimeException {
    ArrayList<String> classpaths = new ArrayList<>();
    // Download the file from S3 URL when
    // case 1: if the file doesn't exist locally
    // case 2: the file exists locally but it's different from S3

    File dir = new File(jar_dir);
    dir.mkdirs();

    try {
      for (String path : paths) {
        // check if the path is a local url
        if (checkIfLocal(job_path, path)) {
          String localPath = getLocalFile(job_path, path).getPath();
          logger.info("file exists locally: " + localPath);
          classpaths.add(localPath);
        } else if (checkIfS3(path)) {
          classpaths.addAll(downloadFromS3(path, jar_dir, conf));
        }
      }

    } catch (URISyntaxException e) {
      logger.error("URI syntax exception - Error: " + ExceptionUtils.getStackTrace(e));
    } catch (IOException e) {
      logger.error("IO exception - Error: " + ExceptionUtils.getStackTrace(e));
    }

    // if nothing is added, return the input paths
    if (classpaths.isEmpty()) {
      return paths;
    } else {
      return classpaths;
    }
  }

  /**
   * Get local file
   *
   * @param dir directory name
   * @param path relative path name
   * @return
   */
  protected File getLocalFile(String dir, String path) {
    File file = new File(dir, path);
    return file.exists() && file.isFile() ? file : null;
  }

  /**
   * Get local file
   *
   * @param path absolute name
   * @return
   */
  protected File getLocalFile(String path) {
//    File file = new File(path);
//    if (file.exists() && file.isFile()) {
//      return new File(path);
//    } else {
//      return null;
//    }
    File file = new File(path);
    return file != null && file.isFile() ? file : null;
  }

  /**
   * Check if the file is local
   *
   * @param dir directory name
   * @param path relative path name
   * @return
   */
  protected Boolean checkIfLocal(String dir, String path) {
    File localFile = getLocalFile(dir, path);
    return localFile != null && localFile.exists();
  }

  /**
   * Check if it's a local file
   *
   * @param path
   * @return
   */
  protected Boolean checkIfLocal(String path) {
    File localFile = getLocalFile(path);
    return localFile != null && localFile.exists();
  }

  protected Boolean checkIfS3(String path) throws URISyntaxException {
    URI input_path = new URI(path);

    String scheme = input_path.getScheme();
    // if it's a s3 path
    return (scheme != null && (scheme.equals("s3") || scheme.equals("s3a")));
  }

  protected List<String> downloadFromS3(String path, String jar_dir, Configuration conf) throws URISyntaxException, IOException {
    // Convert the path into a local path
    URI input_path = new URI(path);
    String key = input_path.getPath();
    File localJarFile = new File(jar_dir, key);
    String localJarPath = localJarFile.getAbsolutePath();

    // set up the hadoop configs for hadoop file system
    // setHadoopConfigs();

    List<String> output = new ArrayList<>();

    File localFile = new File(localJarPath);

    Path s3Path = new Path("s3a://" + input_path.getHost() + input_path.getPath());
    FileSystem s3Fs = s3Path.getFileSystem(conf);

    if (!localFile.exists()) {
      s3Fs.copyToLocalFile(s3Path, new Path(localJarPath));
    } else if (input_path.getPath().toLowerCase().contains("snapshot")) {
      logger.info("Updated file: " + key);
      s3Fs.copyToLocalFile(s3Path, new Path(localJarPath));
    }

    // check if the file exists locally
    if (new File(localJarPath).exists()) {
      logger.info("local path: " + localJarPath);
      output.add(localJarPath);
    }

    return output;
  }

}
