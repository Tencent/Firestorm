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

import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.storage.handler.api.ClientReadHandler;


public class ComposedClientReadHandler implements ClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ComposedClientReadHandler.class);

  private ClientReadHandler hotDataReadHandler;
  private ClientReadHandler warmDataReadHandler;
  private ClientReadHandler coldDataReadHandler;
  private ClientReadHandler frozenDataReadHandler;
  private Callable<ClientReadHandler> hotHandlerCreator;
  private Callable<ClientReadHandler> warmHandlerCreator;
  private Callable<ClientReadHandler> coldHandlerCreator;
  private Callable<ClientReadHandler> frozenHandlerCreator;
  private static final int HOT = 1;
  private static final int WARM = 2;
  private static final int COLD = 3;
  private static final int FROZEN = 4;
  private int currentHandler = HOT;
  private final int topLevelOfHandler;

  private long hotReadBlockNum = 0L;
  private long warmReadBlockNum = 0L;
  private long coldReadBlockNum = 0L;
  private long frozenReadBlockNum = 0L;

  private long hotReadLength = 0L;
  private long warmReadLength = 0L;
  private long coldReadLength = 0L;
  private long frozenReadLength = 0L;

  private long hotReadUncompressLength = 0L;
  private long warmReadUncompressLength = 0L;
  private long coldReadUncompressLength = 0L;
  private long frozenReadUncompressLength = 0L;

  public ComposedClientReadHandler(ClientReadHandler... handlers) {
    topLevelOfHandler = handlers.length;
    if (topLevelOfHandler > 0) {
      this.hotDataReadHandler = handlers[0];
    }
    if (topLevelOfHandler > 1) {
      this.warmDataReadHandler = handlers[1];
    }
    if (topLevelOfHandler > 2) {
      this.coldDataReadHandler = handlers[2];
    }
    if (topLevelOfHandler > 3) {
      this.frozenDataReadHandler = handlers[3];
    }
  }

  public ComposedClientReadHandler(Callable<ClientReadHandler>... creators) {
    topLevelOfHandler = creators.length;
    if (topLevelOfHandler > 0) {
      this.hotHandlerCreator = creators[0];
    }
    if (topLevelOfHandler > 1) {
      this.warmHandlerCreator = creators[1];
    }
    if (topLevelOfHandler > 2) {
      this.coldHandlerCreator = creators[2];
    }
    if (topLevelOfHandler > 3) {
      this.frozenHandlerCreator = creators[3];
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    try {
      switch (currentHandler) {
        case HOT:
          if (hotDataReadHandler == null) {
            hotDataReadHandler = createReadHandlerIfNotExist(hotHandlerCreator);
          }
          shuffleDataResult = hotDataReadHandler.readShuffleData();
          break;
        case WARM:
          if (warmDataReadHandler == null) {
            warmDataReadHandler = createReadHandlerIfNotExist(warmHandlerCreator);
          }
          shuffleDataResult = warmDataReadHandler.readShuffleData();
          break;
        case COLD:
          if (coldDataReadHandler == null) {
            coldDataReadHandler = createReadHandlerIfNotExist(coldHandlerCreator);
          }
          shuffleDataResult = coldDataReadHandler.readShuffleData();
          break;
        case FROZEN:
          if (frozenDataReadHandler == null) {
            frozenDataReadHandler = createReadHandlerIfNotExist(frozenHandlerCreator);
          }
          shuffleDataResult = frozenDataReadHandler.readShuffleData();
          break;
        default:
          return null;
      }
    } catch (Exception e) {
      LOG.error("Failed to read shuffle data from " + getCurrentHandlerName() + " handler", e);
    }
    // when is no data for current handler, and the upmostLevel is not reached,
    // then try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      if (currentHandler < topLevelOfHandler) {
        currentHandler++;
      } else {
        return null;
      }
      return readShuffleData();
    }

    return shuffleDataResult;
  }

  private ClientReadHandler createReadHandlerIfNotExist(Callable<ClientReadHandler> creator) throws Exception {
    if (creator == null) {
      throw new IllegalStateException("creator " + getCurrentHandlerName() + " handler doesn't exist");
    }
    return creator.call();
  }

  private String getCurrentHandlerName() {
    String name = "UNKNOWN";
    switch (currentHandler) {
      case HOT:
        name = "HOT";
        break;
      case WARM:
        name = "WARM";
        break;
      case COLD:
        name = "COLD";
        break;
      case FROZEN:
        name = "FROZEN";
        break;
      default:
        break;
    }
    return name;
  }

  @Override
  public void close() {
    if (hotDataReadHandler != null) {
      hotDataReadHandler.close();
    }

    if (warmDataReadHandler != null) {
      warmDataReadHandler.close();
    }

    if (coldDataReadHandler != null) {
      coldDataReadHandler.close();
    }

    if (frozenDataReadHandler != null) {
      frozenDataReadHandler.close();
    }
  }

  @Override
  public void updateConsumedBlockInfo(BufferSegment bs) {
    if (bs == null) {
      return;
    }
    switch (currentHandler) {
      case HOT:
        hotReadBlockNum++;
        hotReadLength += bs.getLength();
        hotReadUncompressLength += bs.getUncompressLength();
        break;
      case WARM:
        warmReadBlockNum++;
        warmReadLength += bs.getLength();
        warmReadUncompressLength += bs.getUncompressLength();
        break;
      case COLD:
        coldReadBlockNum++;
        coldReadLength += bs.getLength();
        coldReadUncompressLength += bs.getUncompressLength();
        break;
      case FROZEN:
        frozenReadBlockNum++;
        frozenReadLength += bs.getLength();
        frozenReadUncompressLength += bs.getUncompressLength();
        break;
      default:
        break;
    }
  }

  @Override
  public void logConsumedBlockInfo() {
    LOG.info(getReadBlokNumInfo());
    LOG.info(getReadLengthInfo());
    LOG.info(getReadUncompressLengthInfo());
  }

  @VisibleForTesting
  public String getReadBlokNumInfo() {
    long totalBlockNum = hotReadBlockNum + warmReadBlockNum
      + coldReadBlockNum + frozenReadBlockNum;
    return "Client read " + totalBlockNum + " blocks ["
      + " hot:" + hotReadBlockNum + " warm:" + warmReadBlockNum
      + " cold:" + coldReadBlockNum + " frozen:" + frozenReadBlockNum + " ]";
  }

  @VisibleForTesting
  public String getReadLengthInfo() {
    long totalReadLength = hotReadLength + warmReadLength
      + coldReadLength + frozenReadLength;
    return "Client read " + totalReadLength + " bytes ["
      + " hot:" + hotReadLength + " warm:" + warmReadLength
      + " cold:" + coldReadLength + " frozen:" + frozenReadLength + " ]";
  }

  @VisibleForTesting
  public String getReadUncompressLengthInfo() {
    long totalReadUncompressLength = hotReadUncompressLength + warmReadUncompressLength
      + coldReadUncompressLength + frozenReadUncompressLength;
    return "Client read " + totalReadUncompressLength + " uncompressed bytes ["
      + " hot:" + hotReadUncompressLength
      + " warm:" + warmReadUncompressLength
      + " cold:" + coldReadUncompressLength
      + " frozen:" + frozenReadUncompressLength + " ]";
  }

}
