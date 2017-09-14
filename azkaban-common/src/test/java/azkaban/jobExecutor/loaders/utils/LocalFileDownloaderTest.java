package azkaban.jobExecutor.loaders.utils;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


public class LocalFileDownloaderTest {
  @ClassRule
  public static TemporaryFolder classTemp = new TemporaryFolder();

  @AfterClass
  public static void cleanup() {
    classTemp.delete();
  }

  @Test
  public void testDownloaderCanFindLocalWorkDirFiles() {
    LocalFileDownloader downloader = new LocalFileDownloader(classTemp.getRoot().getAbsolutePath());
    String local = "";
    try {
      File localFile = classTemp.newFile("blahJar.jar");
      localFile.createNewFile();
      String destination = Paths.get(localFile.getParent(), "someTempJar.jar").toString();
      // Works with absolute path
      local = downloader.download(localFile.getAbsolutePath(), destination);
      Assert.assertEquals(local, destination);
      // Works with relative path
      local = downloader.download(localFile.getName(), destination);
      Assert.assertEquals(local, destination);
    } catch (IOException e) {

    }
  }

}
