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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.LifecycleService;
import org.junit.Assert;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Test that the {@link TaskTracker} follows the {@link LifecycleService} 
 * lifecycle
 */

public class TestTaskTrackerLifecycle extends Assert {
  private TaskTracker tracker;

  /**
   * At the end of each test run, close any non-null TaskTracker
   */
  @After
  public void tearDown() {
    LifecycleService.close(tracker);
  }

  /**
   * Create a job conf suitable for testing
   * @return a new job conf instance
   */
  private JobConf createJobConf() {
    JobConf config = new JobConf();
    //extra fast timeout
    config.set("mapred.task.tracker.connect.timeout","10");
    String dataDir = System.getProperty("test.build.data");
    File hdfsDir = new File(dataDir, "dfs");
    config.set("dfs.name.dir", new File(hdfsDir, "name1").getPath());
    FileSystem.setDefaultUri(config, "hdfs://localhost:0");
    config.set("dfs.http.address", "hdfs://localhost:0");
    config.set("mapred.job.tracker", "localhost:8012");
    config.set("ipc.client.connect.max.retries", "1");
    return config;
  }

  /**
   * Assert that the throwable is some kind of IOException, 
   * containing the string "Connection refused"
   * @param thrown what was thrown
   * @throws Throwable the exception, rethrown, if it is not what was expected
   */
  private void assertConnectionRefused(Throwable thrown) throws Throwable {
    assertNotNull("Null Exception", thrown);
    if (!(thrown instanceof IOException)) {
      throw thrown;
    }
    if (!thrown.getMessage().contains("Connection refused")) {
      throw thrown;
    }
  }

  /**
   * Test that if a tracker isn't started, it can still be closed 
   * @throws Throwable on a failure
   */
  @Test
  public void testTerminateUnstartedTracker() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    tracker.close();
  }

  /**
   * Expect the classic {@link TaskTracker#TaskTracker(JobConf)} method
   * tries to start the service, as it always does, and that if it cannot
   * start then it fails.
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testClassicTrackerConstructorTriesToStartsService() throws Throwable {
    try {
      tracker = new TaskTracker(createJobConf());
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
    }
  }

  /**
   * Expect the TT to fail to start from its {@link TaskTracker#start()} method
   * and is then in the failed state.
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testFailingTracker() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    try {
      tracker.start();
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
      assertEquals(LifecycleService.ServiceState.FAILED, tracker.getServiceState());
    }
  }


  /**
   * Check that the {@link LifecycleService#startService(LifecycleService)}
   * method not only tries to start the service, if it fails it is pushed into
   * the closed state. Then check that a second attempt to close the service
   * does not have any adverse effects.
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testStartServiceOperation() throws Throwable {
    tracker = new TaskTracker(createJobConf(), false);
    try {
      LifecycleService.startService(tracker);
      fail("Expected a failure");
    } catch (IOException e) {
      assertConnectionRefused(e);
      assertEquals(LifecycleService.ServiceState.CLOSED, tracker.getServiceState());
    }
    assertConnectionRefused(tracker.getFailureCause());
    tracker.close();
    assertConnectionRefused(tracker.getFailureCause());
  }

}
