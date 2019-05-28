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

package org.apache.solr.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * for filter query ---
 * {!frange l=1}getVal()
 *
 * for json facet isInStock ----
 * json.facet={stock:{type:query,limit:-1,q:"{!frange l=1}getVal(id,'isInStock')",domain:{filter : "{!frange l=1}getVal(id,'buyable')" },facet:{ group.count:"unique(level0Id)" }}}
 *
 * stats---
 * json.facet.max_price="max(getVal(id,'salePrice'))"
 */
public class ValueFetchValueSourceParserStock extends ValueSourceParser {

  public static final ValueSource VALUE_SOURCE = new ValueSource() {

    @Override
    public FunctionValues getValues(Map map, LeafReaderContext leafReaderContext) throws IOException {
      return FunctionValuesHelper.getFunctionValues(((SolrIndexSearcher)map.get("searcher")).getCore().getName(),"isInStock");
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public String description() {
      return "";
    }
  };

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    return VALUE_SOURCE;

  }


}
