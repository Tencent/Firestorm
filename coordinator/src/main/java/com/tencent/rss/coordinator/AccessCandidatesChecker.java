/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.coordinator;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.Constants;

/**
 * AccessCandidatesChecker maintain a list of candidate access id and update it periodically,
 * it checks the access id in the access request and reject if the id is not in the candidate list.
 */
public class AccessCandidatesChecker implements AccessChecker {
  private static final Logger LOG = LoggerFactory.getLogger(AccessCandidatesChecker.class);

  private final AtomicReference<Set<String>> candidates = new AtomicReference<>();
  private final AtomicLong lastCandidatesUpdateMS = new AtomicLong(0L);
  private final Path path;
  private final ScheduledExecutorService updateAccessCandidatesSES;
  private final FileSystem fileSystem;

  public AccessCandidatesChecker(AccessManager accessManager) throws Exception {
    CoordinatorConf conf = accessManager.getCoordinatorConf();
    String pathStr = conf.get(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH);
    this.path = new Path(pathStr);
    Configuration hadoopConf = accessManager.getHadoopConf();
    this.fileSystem = CoordinatorUtils.getFileSystemForPath(path, hadoopConf);
    if (!fileSystem.isFile(path)) {
      String msg = String.format("Fail to init AccessCandidatesChecker, %s is not a file", path.toUri());
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
    int updateIntervalS = conf.getInteger(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_UPDATE_INTERVAL_SEC);
    updateAccessCandidatesSES = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("UpdateAccessCandidates-%d").build());
    updateAccessCandidatesSES.scheduleAtFixedRate(
        this::updateAccessCandidates, 0, updateIntervalS, TimeUnit.SECONDS);
  }

  public AccessCheckResult check(AccessInfo accessInfo) {
    String accessId = accessInfo.getAccessId().trim();
    if (!candidates.get().contains(accessId)) {
      String msg = String.format("Denied by AccessCandidatesChecker, accessInfo[%s].", accessInfo);
      LOG.debug("Candidates is {}, {}", candidates.get(), msg);
      return new AccessCheckResult(false, msg);
    }

    return new AccessCheckResult(true, Constants.COMMON_SUCCESS_MESSAGE);
  }

  public void close() {
    if (updateAccessCandidatesSES != null) {
      updateAccessCandidatesSES.shutdownNow();
    }
  }

  private void updateAccessCandidates() {
    try {
      FileStatus[] fileStatus = fileSystem.listStatus(path);
      if (!ArrayUtils.isEmpty(fileStatus)) {
        long lastModifiedMS = fileStatus[0].getModificationTime();
        if (lastCandidatesUpdateMS.get() != lastModifiedMS) {
          updateAccessCandidatesInternal();
          lastCandidatesUpdateMS.set(lastModifiedMS);
        }
      } else {
        candidates.set(Sets.newConcurrentHashSet());
      }
      // TODO: add access num metrics
    } catch (Exception e) {
      LOG.warn("Error when update access candidates", e);
    }
  }

  private void updateAccessCandidatesInternal() {
    Set<String> newCandidates = Sets.newHashSet();
    String content = loadFileContent();
    if (StringUtils.isEmpty(content)) {
      LOG.warn("Load empty content from {}", path.toUri().toString());
      candidates.set(newCandidates);
      return;
    }

    for (String item : content.split(IOUtils.LINE_SEPARATOR_UNIX)) {
      String accessId = item.trim();
      if (!StringUtils.isEmpty(accessId)) {
        newCandidates.add(accessId);
      }
    }

    if (newCandidates.isEmpty()) {
      LOG.warn("Empty content in {}", path.toUri().toString());
    }

    candidates.set(newCandidates);
  }

  private String loadFileContent() {
    String content = null;
    try (FSDataInputStream in = fileSystem.open(path)) {
      content = IOUtils.toString(in, StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOG.error("Fail to load content from {}", path.toUri().toString());
    }
    return content;
  }

  public AtomicReference<Set<String>> getCandidates() {
    return candidates;
  }
}
