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

package azkaban.utils;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class ClassPathUtilsTest {
  private static final String inputContent = "for testing";
  private static String localFileName = "localFile";
  private static File localFile;
  private static String localFileAbsolutePath;
  private static String s3ClassPath = "s3://usw2-polaris-artifacts-dev/x/salescloud-azkabanworkflows-1.4.0-SNAPSHOT.jar";
  private static String s3aClassPath = "s3a://usw2-polaris-artifacts-dev/x/salescloud-azkabanworkflows-1.4.0-SNAPSHOT.jar";
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private ClassPathUtils classPathUtils;
  private Props jobProps;

  @Before
  public void setUp() throws IOException {
    localFile = temp.newFile(localFileName);
    localFileAbsolutePath = localFile.getAbsolutePath();
    jobProps = new Props();
    jobProps.put("hadoop.master.ip", "http://10.36.73.253");
    classPathUtils = new ClassPathUtils();

    // Dump local File
    try {
      azkaban.jobExecutor.Utils.dumpFile(localFileAbsolutePath, inputContent);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      Assert.fail("error in creating input file:" + e.getLocalizedMessage());
    }
  }

  @After
  public void tearDown() {
    temp.delete();
  }

  @Test
  public void testCheckIfLocalPath() throws Exception {
    String invalidLocalPath = localFileAbsolutePath + "2";

    // if provide absolute path only should still return true
    Assert.assertTrue(classPathUtils.checkIfLocal(localFileAbsolutePath));
    // if provide absolute path & empty directory name should still return true
    Assert.assertTrue(classPathUtils.checkIfLocal("", localFileAbsolutePath));
    // if provide a correct base directory and file name, should return true
    Assert.assertTrue(classPathUtils.checkIfLocal(localFile.getParent(), localFile.getName()));
    // if provide a wrong base directory and file name, should return false
    Assert.assertFalse(classPathUtils.checkIfLocal(localFile.getParent() + "1", localFile.getName()));
    // if provide a wrong base directory and file name, should return false
    Assert.assertFalse(classPathUtils.checkIfLocal(localFile.getParent() + "1", localFileAbsolutePath));
    // if provide a wrong base directory and file name, should return false
    Assert.assertFalse(classPathUtils.checkIfLocal("", invalidLocalPath));
    // if provide a wrong base directory and file name, should return false
    Assert.assertFalse(classPathUtils.checkIfLocal(invalidLocalPath));
    // if provide a s3 path, should return false
    Assert.assertFalse(classPathUtils.checkIfLocal(s3aClassPath));

  }

  @Test
  public void testS3ClassPaths() throws Exception {
    Assert.assertTrue(classPathUtils.checkIfS3(s3ClassPath));
    Assert.assertTrue(classPathUtils.checkIfS3(s3aClassPath));
    Assert.assertFalse(classPathUtils.checkIfS3(localFileAbsolutePath));
  }

  @Test
  public void testGetLocalPath() throws Exception {
    String invalidLocalPath = localFileAbsolutePath + "2";

    // similar to testCheckIfLocalPath(), check the existence of a local or non-local file
    // valid file scenarios
    Assert.assertTrue(classPathUtils.getLocalFile("", localFileAbsolutePath).equals(localFile));
    Assert.assertTrue(classPathUtils.getLocalFile(localFile.getParent(), localFile.getName()).equals(localFile));
    // invalid invalid scenarios
    Assert.assertNull(classPathUtils.getLocalFile(localFile.getParent() + "1", localFile.getName()));
    Assert.assertNull(classPathUtils.getLocalFile(localFile.getParent() + "1", localFileAbsolutePath));
    Assert.assertNull(classPathUtils.getLocalFile("", invalidLocalPath));

  }

}
