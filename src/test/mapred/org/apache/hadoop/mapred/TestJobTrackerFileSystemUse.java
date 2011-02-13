/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestJobTrackerFileSystemUse {

  private static final Log LOG =
      LogFactory.getLog(TestJobTrackerFileSystemUse.class);
  private MiniMRCluster mrCluster;
  protected static FileSystem fileSystem;
  protected static MiniDFSCluster dfsCluster;
  protected JobTracker jobTracker;
  protected JobConf jobConf;


  public TestJobTrackerFileSystemUse() {
  }

  @BeforeClass
  public static void setUpDFS() throws IOException {
    dfsCluster = new MiniDFSCluster(new JobConf(), 1, true, null);
    fileSystem = dfsCluster.getFileSystem();
  }

  @Before
  public void setUpMR() throws IOException {
    mrCluster = new MiniMRCluster(1, fileSystem.getUri().toString(), 1);
    jobConf = mrCluster.createJobConf();
    jobTracker = mrCluster.getJobTrackerRunner().getJobTracker();
  }

  @After
  public void teardownMR() throws IOException {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
  }

  @AfterClass
  public static void teardownDFS() throws IOException {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Test
  public void testFileSystemIsNotShared() throws Throwable {
    //get the JT

    //make a call of it that requires a valid FS
    FileSystem jobFS = jobTracker.fs;
    Assert.assertNotSame("JT is using a shared filesystem", fileSystem, jobFS);
    //close the FS
    fileSystem.close();

    //repeat the call
  }

  @Test
  public void testFileSystemClose() throws Throwable {

    FileSystem jobFS = jobTracker.fs;
    jobTracker.stopTracker();
    //check that the FS is now closed by looking up a path
    try {
      jobFS.exists(jobFS.getHomeDirectory());
      Assert.fail("Job Tracker did not close the filesystem " + jobFS);
    } catch (IOException e) {
      //this was expected
    }
    Assert.assertNull("jobTracker.fs", jobTracker.fs);
  }

}
