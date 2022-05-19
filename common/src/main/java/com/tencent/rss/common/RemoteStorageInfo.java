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

package com.tencent.rss.common;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

public class RemoteStorageInfo implements Serializable {
  public static final RemoteStorageInfo EMPTY_REMOTE_STORAGE = new RemoteStorageInfo("", null);
  private final String path;
  private final Map<String, String> confItems;

  public RemoteStorageInfo(String path) {
    this(path, Maps.newHashMap());
  }

  public RemoteStorageInfo(String path, Map<String, String> confItems) {
    this.path = path;
    this.confItems = confItems;
  }

  public String getPath() {
    return path;
  }

  public Map<String, String> getConfItems() {
    return confItems;
  }

  public boolean isEmpty() {
    return StringUtils.isEmpty(path);
  }
}
