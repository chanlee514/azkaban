package azkaban.jobExecutor.loaders;

import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.loaders.utils.FileDownloader;
import azkaban.jobExecutor.loaders.utils.LocalFileDownloader;
import azkaban.jobExecutor.loaders.utils.S3FileDownloader;
import azkaban.utils.Props;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Currently the only implementation of DependencyLoader, downloads remote files
 * downloaders set to handle different protocols.
 */
public class RemoteDependencyLoader extends DependencyLoader {

  private transient static Logger logger = Logger.getLogger(RemoteDependencyLoader.class);
  private boolean unique;
  private String targetDirectory;
  protected List<String> loaderUrls;
  protected Map<String, FileDownloader> downloaders = new HashMap();

  protected Props sysProps;
  protected Props jobProps;

  protected String baseDir;

  protected static final String PROTOCOL_SEP = "://";

  /**
   * Set a downloader for a particular protocol prefix
   *
   * @param protocol   The protocol (such as s3)
   * @param downloader a FileDownloader capable of handling protocol in question
   */
  public void setDownloader(String protocol, FileDownloader downloader) {
    this.downloaders.put(protocol, downloader);
  }

  /**
   * @param jobProps Azkaban job properties
   *
   */
  public RemoteDependencyLoader(Props jobProps, String urls, String jobDir) {
    loaderUrls = jobProps.getStringList(urls, ",");
    unique = jobProps.getBoolean(UNIQUE_FILE_DOWNLOAD, false);
    targetDirectory = getTempDirectory(jobProps);
    baseDir = jobDir;
  }

  /**
   * Get the default downloader for the protocol. It's not super hot that this is
   * hardcoded here. Handling this here allows for injection for testing.
   *
   * @param protocol
   * @return A downloader for this file type
   */
  protected FileDownloader defaultDownloader(String protocol) {
    switch (protocol) {
      case "s3":
        return new S3FileDownloader(jobProps);
      case "s3a":
        return new S3FileDownloader(jobProps);
      case "local":
        return new LocalFileDownloader(baseDir);
      default:
        throw new RuntimeException("Protocol unknown: " + protocol);
    }
  }

  /**
   * Get a file at a remote location
   *
   * @param url         the url to be retrieved
   * @param destination local destination dir
   * @param unique      uniquify local download with UUID
   * @return local path of file downloaded
   * @throws IOException
   */
  public String getFile(String url, String destination, boolean unique) throws IOException {
    try {
      String[] protocolAndPath = url.split(PROTOCOL_SEP);
      String protocol;
      String path;
      if(protocolAndPath.length < 2){
        protocol = "local";
        path = url;
      } else {
        protocol = protocolAndPath[0];
        path = protocolAndPath[1];
      }

      File remoteFile = new File(path);
      String filename = remoteFile.getName();

      if (unique) {
        String extension = FilenameUtils.getExtension(filename);
        filename = java.util.UUID.randomUUID().toString() + "." + extension;
      }

      String localPath = Paths.get(destination, filename).toAbsolutePath().toString(); // new location on local
      File localFile = new File(localPath);

      if (!downloaders.containsKey(protocol)) {
        setDownloader(protocol, defaultDownloader(protocol));
      }

      if (!localFile.exists()) {
        return downloaders.get(protocol).download(path, localPath);
      }

      return localPath;
    } catch (Exception e) {
      logger.error("Unable to download " + url + " from remote location, saw error \n", e);
      throw new IOException("File download failed for " + url);
    }
  }

  /**
   * Get all required dependencies for this job
   *
   * @param urls The list of urls to download
   * @return paths of files on local FS
   */
  public List<String> getDependencies(List<String> urls) {
    List<String> downloadedFiles = new ArrayList();
    for (String url : urls) {
      try {
        downloadedFiles.add(getFile(url, targetDirectory, unique));
      } catch (IOException e) {
        logger.error("Exception loading dependency: ", e);
        throw new RuntimeException("Unable to locate dependencies: " + url);
      }
    }
    return downloadedFiles;
  }

  /**
   * Get the dependencies defined in properties
   *
   * @return a list of files downloaded
   */
  @Override
  public List<String> getDependencies() {
    return getDependencies(loaderUrls);
  }

}
