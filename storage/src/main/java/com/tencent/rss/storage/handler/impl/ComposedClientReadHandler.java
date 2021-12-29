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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public ComposedClientReadHandler(ClientReadHandler... handlers) {
    int size = handlers.length;
    if (size > 0) {
      this.hotDataReadHandler = handlers[0];
    }
    if (size > 1) {
      this.warmDataReadHandler = handlers[1];
    }
    if (size > 2) {
      this.coldDataReadHandler = handlers[2];
    }
    if (size > 3) {
      this.frozenDataReadHandler = handlers[3];
    }
  }

  public ComposedClientReadHandler(Callable<ClientReadHandler>... creators) {
    int size = creators.length;
    if (size > 0) {
      this.hotHandlerCreator = creators[0];
    }
    if (size > 1) {
      this.warmHandlerCreator = creators[1];
    }
    if (size > 2) {
      this.coldHandlerCreator = creators[2];
    }
    if (size > 3) {
      this.frozenHandlerCreator = creators[3];
    }
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult shuffleDataResult = null;
    switch (currentHandler) {
      case HOT:
        try {
          if (hotDataReadHandler == null) {
            if (hotHandlerCreator == null) {
              return null;
            }
            hotDataReadHandler = hotHandlerCreator.call();
          }
          shuffleDataResult = hotDataReadHandler.readShuffleData();
        } catch (Exception e) {
          LOG.error("Failed to read shuffle data from hot handler", e);
        }
        break;
      case WARM:
          try {
            if (warmDataReadHandler == null) {
              if (warmHandlerCreator == null) {
                return null;
              }
              warmDataReadHandler = warmHandlerCreator.call();
            }
            shuffleDataResult = warmDataReadHandler.readShuffleData();
          } catch (Exception e) {
            LOG.error("Failed to read shuffle data from warm handler", e);
          }
        break;
      case COLD:
          try {
            if (coldDataReadHandler == null) {
              if (coldHandlerCreator == null) {
                return null;
              }
              coldDataReadHandler = coldHandlerCreator.call();
            }
            shuffleDataResult = coldDataReadHandler.readShuffleData();
          } catch (Exception e) {
            LOG.error("Failed to read shuffle data from cold handler", e);
          }
        break;
      case FROZEN:
          try {
            if (frozenDataReadHandler == null)  {
              if (frozenHandlerCreator == null) {
                return null;
              }
              frozenDataReadHandler = frozenHandlerCreator.call();
            }
            shuffleDataResult = frozenDataReadHandler.readShuffleData();
          } catch (Exception e) {
            LOG.error("Failed to read shuffle data from frozen handler", e);
          }
        break;
      default:
        return null;
    }
    // there is no data for current handler, try next one if there has
    if (shuffleDataResult == null || shuffleDataResult.isEmpty()) {
      currentHandler++;
      return readShuffleData();
    }
    return shuffleDataResult;
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
}
