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

package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComposedClientReadHandler implements ClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposedClientReadHandler.class);

  private ClientReadHandler level1ReadHandler;
  private ClientReadHandler level2ReadHandler;
  private ClientReadHandler level3ReadHandler;
  private static final int LEVEL1 = 1;
  private static final int LEVEL2 = 2;
  private static final int LEVEL3 = 3;
  private int currentLevel = LEVEL1;

  public ComposedClientReadHandler(ClientReadHandler[] handlers) {
    int size = handlers.length;
    if (size > 0) {
      this.level1ReadHandler = handlers[0];
    }
    if (size > 1) {
      this.level2ReadHandler = handlers[1];
    }
    if (size > 2) {
      this.level3ReadHandler = handlers[2];
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    switch (currentLevel) {
      case LEVEL1:
        try {
          shuffleDataResult = level1ReadHandler.readShuffleData();
        } catch (Exception e) {
          LOG.error("Failed to read shuffle data from level1 handler", e);
        }
        break;
      case LEVEL2:
        if (level2ReadHandler != null) {
          try {
            shuffleDataResult = level2ReadHandler.readShuffleData();
          } catch (Exception e) {
            LOG.error("Failed to read shuffle data from leve2 handler", e);
          }
        } else {
          return null;
        }
        break;
      case LEVEL3:
        if (level3ReadHandler != null) {
          try {
            shuffleDataResult = level3ReadHandler.readShuffleData();
          } catch (Exception e) {
            LOG.error("Failed to read shuffle data from leve3 handler", e);
          }
        } else {
          return null;
        }
        break;
      default:
        return null;
    }
    // there is no data for current handler, try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      currentLevel++;
      return readShuffleData();
    }
    return shuffleDataResult;
  }

  @Override
  public void close() {
    if (level1ReadHandler != null) {
      level1ReadHandler.close();
    }

    if (level2ReadHandler != null) {
      level2ReadHandler.close();
    }

    if (level3ReadHandler != null) {
      level3ReadHandler.close();
    }
  }
}
