/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobExecutor;

import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import azkaban.jobExecutor.loaders.DependencyLoader;
import azkaban.jobExecutor.loaders.RemoteDependencyLoader;
import azkaban.jobExecutor.loaders.utils.S3FileDownloader;
import azkaban.jobExecutor.loaders.utils.TestFileDownloader;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

public class JavaProcessJobTest {
  @ClassRule
  public static TemporaryFolder classTemp = new TemporaryFolder();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private JavaProcessJob job = null;
  private Props props = null;
  private Logger log = Logger.getLogger(JavaProcessJob.class);

  private static String classPaths;

  private static final String inputContent =
      "Quick Change in Strategy for a Bookseller \n"
          + " By JULIE BOSMAN \n"
          + "Published: August 11, 2010 \n"
          + " \n"
          + "Twelve years later, it may be Joe Fox's turn to worry. Readers have gone from skipping small \n"
          + "bookstores to wondering if they need bookstores at all. More people are ordering books online  \n"
          + "or plucking them from the best-seller bin at Wal-Mart";

  private static final String errorInputContent =
      inputContent
          + "\n stop_here "
          + "But the threat that has the industry and some readers the most rattled is the growth of e-books. \n"
          + " In the first five months of 2009, e-books made up 2.9 percent of trade book sales. In the same period \n"
          + "in 2010, sales of e-books, which generally cost less than hardcover books, grew to 8.5 percent, according \n"
          + "to the Association of American Publishers, spurred by sales of the Amazon Kindle and the new Apple iPad. \n"
          + "For Barnes & Noble, long the largest and most powerful bookstore chain in the country, the new competition \n"
          + "has led to declining profits and store traffic.";

  private static String inputFile;
  private static String errorInputFile;
  private static String outputFile;

  @BeforeClass
  public static void init() throws IOException {
    // Get the classpath
    Properties prop = System.getProperties();
    classPaths =
        String.format("'%s'", prop.getProperty("java.class.path", null));

    long time = (new Date()).getTime();
    inputFile = classTemp.newFile("azkaban_input_" + time).getCanonicalPath();
    errorInputFile =
        classTemp.newFile("azkaban_input_error_" + time).getCanonicalPath();
    outputFile = classTemp.newFile("azkaban_output_" + time).getCanonicalPath();

    // Dump input files
    try {
      Utils.dumpFile(inputFile, inputContent);
      Utils.dumpFile(errorInputFile, errorInputContent);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      Assert.fail("error in creating input file:" + e.getLocalizedMessage());
    }
  }

  @AfterClass
  public static void cleanup() {
    classTemp.delete();
  }

  @Before
  public void setUp() throws IOException {
    File workingDir = temp.newFolder("testJavaProcess");

    // Initialize job
    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, workingDir.getCanonicalPath());
    props.put("type", "java");
    props.put("fullPath", ".");
    
    props.put(CommonJobProperties.PROJECT_NAME, "test_project");
    props.put(CommonJobProperties.FLOW_ID, "test_flow");
    props.put(CommonJobProperties.JOB_ID, "test_job");
    props.put(CommonJobProperties.EXEC_ID, "123");
    props.put(CommonJobProperties.SUBMIT_USER, "test_user");
    

    job = new JavaProcessJob("testJavaProcess", props, props, log);
  }

  @After
  public void tearDown() {
    temp.delete();
  }

  @Test
  public void testJavaJob() throws Exception {
    // initialize the Props
    props.put(JavaProcessJob.JAVA_CLASS,
        "azkaban.jobExecutor.WordCountLocal");
    props.put("input", inputFile);
    props.put("output", outputFile);
    props.put("classpath", classPaths);
    job.run();
  }

  @Test
  public void testJavaJobHashmap() throws Exception {
    // initialize the Props
    props.put(JavaProcessJob.JAVA_CLASS,
        "azkaban.executor.SleepJavaJob");
    props.put("seconds", 1);
    props.put("input", inputFile);
    props.put("output", outputFile);
    props.put("classpath", classPaths);
    job.run();
  }

  @Test
  public void testFailedJavaJob() throws Exception {
    props.put(JavaProcessJob.JAVA_CLASS,
        "azkaban.jobExecutor.WordCountLocal");
    props.put("input", errorInputFile);
    props.put("output", outputFile);
    props.put("classpath", classPaths);

    try {
      job.run();
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testLoaderAddsClassPaths() throws Exception {
    File localFile = classTemp.newFile("someLocalFile1.jar");
    localFile.createNewFile();
    props.put(JavaProcessJob.JAVA_CLASS,
            "azkaban.jobExecutor.WordCountLocal");
    props.put("input", errorInputFile);
    props.put("output", outputFile);
    props.put("classpath", classPaths);
    props.put(ProcessJob.EXTERNAL_DEPENDENCIES_URLS, "s3://blah/someThing.jar,s3://blah/testFile.jar," + localFile.getAbsolutePath());
    props.put(DependencyLoader.UNIQUE_FILE_DOWNLOAD, "false");
    RemoteDependencyLoader remoteLoader = new RemoteDependencyLoader(props, ProcessJob.EXTERNAL_DEPENDENCIES_URLS);
    remoteLoader.setDownloader("s3", new TestFileDownloader());
    job.setLoader(remoteLoader);

    job.run();
    Assert.assertEquals(job.externalFiles.size(), 3);
    String tmpDir = DependencyLoader.getTempDirectory(props);
    Assert.assertTrue(job.getClassPaths().get(1).contains(tmpDir));
    Assert.assertTrue(job.externalFiles.contains(tmpDir + "/someThing.jar"));
    Assert.assertTrue(job.externalFiles.contains(tmpDir + "/testFile.jar"));
    Assert.assertTrue(job.externalFiles.contains(tmpDir + "/someLocalFile1.jar"));

    // Make sure command line is built properly
    Pattern classPathPattern = Pattern.compile("(\\-cp\\W)('.*')");
    Matcher classPathMatcher = classPathPattern.matcher(job.createCommandLine());
    Assert.assertTrue(classPathMatcher.find());
    String classPath = classPathMatcher.group(2);
    Assert.assertTrue(classPath.startsWith("'"));
    Assert.assertTrue(classPath.endsWith("'"));
  }

  @Test
  public void testDownloaderCanFindS3Files() throws Exception {
    props.put("hadoop.master.ip", "10.36.75.138");
    S3FileDownloader downloader = new S3FileDownloader(props);
    downloader.download("s3://usw2-polaris-artifacts-dev/x/com.salesforce/utils_2.11/0.20.1/8e20529dd9bed0934f97a469959d0d8e326dbd19/utils_2.11-0.20.1.jar,s3://usw2-polaris-artifacts-dev/x/com.salesforce/koalavro/0.2.35/937e6f60077c3840efcfa059bc5c6c429878359c/koalavro-0.2.35.jar", "/tmp/localDownload.jar");
  }

}
