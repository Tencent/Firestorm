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

package com.tencent.rss.storage.handler.api;

import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;

public interface ClientReadHandler {

  ShuffleDataResult readShuffleData();

  void close();

  // The handler only returns the segment,
  // but does not know the actually consumed blocks,
  // so the consumer should let the handler update statistics.
  // Each type of handler can design their rules.
  void updateConsumedBlockInfo(BufferSegment bs);

  // Display the statistics of consumed blocks
  void logConsumedBlockInfo();

}
