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
    public static String localFileName = "localFile";
    public static File localFile;
    public static String localFileAbsolutePath;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        localFile = temp.newFile(localFileName);
        localFileAbsolutePath = localFile.getAbsolutePath();

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
    public void testLocalPath() throws Exception {
        String invalidLocalPath = localFileAbsolutePath + "2";

        // if provide absolute path only should still return true
        Assert.assertTrue(ClassPathUtils.checkIfLocal(localFileAbsolutePath));
        // if provide absolute path & empty directory name should still return true
        Assert.assertTrue(ClassPathUtils.checkIfLocal("", localFileAbsolutePath));
        // if provide a correct base directory and file name, should return true
        Assert.assertTrue(ClassPathUtils.checkIfLocal(localFile.getParent(), localFile.getName()));
        // if provide a wrong base directory and file name, should return false
        Assert.assertFalse(ClassPathUtils.checkIfLocal(localFile.getParent() + "1", localFile.getName()));
        // if provide a wrong base directory and file name, should return false
        Assert.assertFalse(ClassPathUtils.checkIfLocal(localFile.getParent() + "1", localFileAbsolutePath));
        Assert.assertFalse(ClassPathUtils.checkIfLocal("", invalidLocalPath));
    }


    @Test
    public void testS3ClassPaths() throws Exception {
        String s3ClassPath = "s3://usw2-polaris-artifacts-dev/x/salescloud-azkabanworkflows-1.4.0-SNAPSHOT.jar";
        String s3aClassPath = "s3a://usw2-polaris-artifacts-dev/x/salescloud-azkabanworkflows-1.4.0-SNAPSHOT.jar";

        Assert.assertTrue(ClassPathUtils.checkIfS3(s3ClassPath));
        Assert.assertTrue(ClassPathUtils.checkIfS3(s3aClassPath));
        Assert.assertFalse(ClassPathUtils.checkIfS3(localFileAbsolutePath));
    }

}
