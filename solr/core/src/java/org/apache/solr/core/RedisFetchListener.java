/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.RedisHelper;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisFetchListener implements SolrEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisFetchListener.class);
  private NamedList args;

  @Override
  public void postCommit() {

  }

  @Override
  public void postSoftCommit() {

  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    LOGGER.warn("New searcher event for {}",newSearcher.getCore().getName());
    RedisHelper.initialize(args.asShallowMap(),newSearcher.getCore().getName());
    new Thread(()->RedisHelper.getInstance(newSearcher.getCore().getName()).populateTokenMap(newSearcher)).start();

  }

  @Override
  public void init(NamedList args) {
    this.args = args;
  }
}
