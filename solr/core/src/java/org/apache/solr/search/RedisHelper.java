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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.StringUtils;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IntValueFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisHelper implements AutoCloseable{

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisHelper.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static Map<String, Map<Double, Set<Integer>>> invertedIndexMap = new HashMap<>();
  private static Map<String, Map<BytesRef, Set<Integer>>> invertedByteRefIndexMap = new HashMap<>();
  private static Map<String,FastLRUCache<Double,Set<Integer>>> lruCacheMap = new HashMap<>();
  private  Map<String, Map<Double, DocSet>> invertedValueDocSetMap = new HashMap<>();
  private  Map<String, Map<Integer, Double>> forwardMap = new HashMap<>();
  private  AtomicBoolean status = new AtomicBoolean(false);
  private  AtomicBoolean mapRefresh = new AtomicBoolean(false);
  private  String redisKey = "documents.1";
//  private  Jedis jedis;
  private  Map<String, Integer> uniqueIdToDocIdMap;
  private DocSet allDocsSet = null;
  private RedisClient redisClient;
  private static StatefulRedisPubSubConnection<String, String> con;
  private static RedisPubSubAdapter<String, String> redisPubSubAdapter;



  private static Map<String,RedisHelper> redisHelper = new HashMap<>();

  public static void initialize(Map map,String coreName) {
    if(null == redisHelper.get(coreName)) {

      RedisHelper helper = new RedisHelper(map);
      redisHelper.put(coreName,helper);
      new Thread(()->helper.addSubscriber(map)).start();
    }
  }

  private void addSubscriber(Map map) {
    TypeReference<List<Map<String,Object>>> eventType = new TypeReference<List<Map<String, Object>>>() {};
    con = redisClient.connectPubSub();
    redisPubSubAdapter = new RedisPubSubAdapter<String, String>() {
      @Override
      public void message(String channel, String message) {
        try {
          LOGGER.info("Channel: {}, Message: {}", channel, message);
          List<Map<String, Object>> data = objectMapper.readValue(message, eventType);
          data.stream().filter(entry->uniqueIdToDocIdMap.containsKey(entry.get("id").toString())).forEach(eventData ->{
            int docId = uniqueIdToDocIdMap.get(eventData.get("id").toString());
            Double value = (Double) eventData.get("value");
            String field = (String) eventData.get("field");
            if(null != value && null != field){
              Double oldValue = (Double) eventData.get("oldValue");
              if(null!=oldValue){
                if(oldValue.equals(value)){
                  LOGGER.debug("since old and new value are same so not updating the data");
                  return;
                }else{
                  forwardMap.get(field).put(docId, value);
                  DocSet docSet = invertedValueDocSetMap.get(field).get(oldValue);
                  invertedValueDocSetMap.get(field).put(oldValue,docSet.andNot(DocSetUtil.createDocSetFromSet(Collections.singleton(docId),docSet.size())));
                }
              }
              invertedValueDocSetMap.get(field).get(value).addUnique(docId);
            }else{
              LOGGER.error("Invalid event type as Mandetory fields are missing {}", eventData);
            }
          });

        } catch (Exception ex) {
          LOGGER.error("Error occur while listening to the redis message {}", message, ex);
        }
      }
    };
    con.addListener(redisPubSubAdapter);
    con.sync().subscribe((String) map.getOrDefault("redis.pub.sub.channel", "redis.pub.10M"));
    /*jedis.subscribe(new JedisPubSub() {
      @Override
      public void onMessage(String channel, String message) {
        super.onMessage(channel, message);
        try {
          LOGGER.warn("Channel: {}, Message: {}", channel, message);
          List<Map<String, Object>> data = objectMapper.readValue(message, eventType);
          data.stream().filter(entry->uniqueIdToDocIdMap.containsKey(entry.get("id").toString())).forEach(eventData ->{
            int docId = uniqueIdToDocIdMap.get(eventData.get("id").toString());
            Double value = (Double) eventData.get("value");
            String field = (String) eventData.get("field");
            if(null != value && null != field){
              Double oldValue = (Double) eventData.get("oldValue");
              if(null!=oldValue){
                if(oldValue.equals(value)){
                  LOGGER.debug("since old and new value are same so not updating the data");
                  return;
                }else{
                  forwardMap.get(field).put(docId, value);
                  DocSet docSet = invertedValueDocSetMap.get(field).get(oldValue);
                  invertedValueDocSetMap.get(field).put(oldValue,docSet.andNot(DocSetUtil.createDocSetFromSet(Collections.singleton(docId),docSet.size())));
                }
              }
              invertedValueDocSetMap.get(field).get(value).addUnique(docId);
            }else{
              LOGGER.error("Invalid event type as Mandetory fields are missing {}", eventData);
            }
          });

        } catch (Exception ex) {
          LOGGER.error("Error occur while listening to the redis message {}", message, ex);
        }
      }
    },(String) map.getOrDefault("redis.sub.channel", "redis.pub.10M"));*/

  }

  private  RedisHelper(Map map) {



    if (null == redisClient) {

      redisKey = (String) map.getOrDefault("redis.key", "documents.docId");

    }
    try {
      redisClient = RedisClient.create((String) map.getOrDefault("redis.uri", "redis://localhost:6379?timeout=131232132132"));
//      jedis = new Jedis(new URI((String) map.getOrDefault("redis.uri", "redis://localhost:6379")), 122121212, 121212121);
      redisKey = (String) map.getOrDefault("redis.key", "documents.1");
    } catch (Exception ex) {
      LOGGER.error("Error while creating the jedis connection", ex);
    }
  }

  public static RedisHelper getInstance(String coreName){
      return redisHelper.get(coreName);
  }

  public  boolean hasData(String field) {
    return invertedValueDocSetMap.containsKey(field);
  }

 /* public static Set<Integer> getDocSet(String field, BytesRef data) {
//    return Optional.ofNullable(invertedIndexMap.get(field))
//        .map(map -> map.get(Double.valueOf(data.utf8ToString())))
//        .orElse(Collections.emptySet());
    return Optional.ofNullable(invertedByteRefIndexMap.get(field))
        .map(map->map.get(data))
        .orElse(null);
  }*/

  public  DocSet getDocSet(Term term) {
    return Optional.ofNullable(invertedValueDocSetMap.get(term.field()))
        .map(map -> map.get(Double.valueOf(term.bytes().utf8ToString())))
//        .map(map->map.get(data))
        .orElse(null);
  }

  public void populateTokenMap(SolrIndexSearcher searcher) {
    createUniqueIdToDocIdMapping(searcher);
    refreshMapFromRedis(searcher);
  }

  private void createUniqueIdToDocIdMapping(SolrIndexSearcher searcher) {
    if (status.get()) {
      LOGGER.error("Since one reload is working so can't run more than 1 job parallel");
      return;
    }
    status.set(true);
    long startTime = System.currentTimeMillis();
    Map<String, Integer> map = new ConcurrentHashMap<>();
    try {
      String name = searcher.getSchema().getUniqueKeyField().getName();
      Set<String> set = Collections.singleton(name);
      List<Integer> docList = new ArrayList<>();
      searcher.getLiveDocs().iterator().forEachRemaining(docList::add);
      ExecutorService executorService = Executors.newFixedThreadPool(10);
      Lists.partition(docList, 100_000)
          .forEach(docL -> executorService.submit(() -> docL.forEach(doc -> {
            try {
              Document doc1 = searcher.doc(doc, set);
              map.put(doc1.get(name), doc);
            } catch (IOException e) {
              LOGGER.error("Error While fetching the data from searcher ", e);
            }
          })));
      executorService.shutdown();
      while (!executorService.isTerminated()) {
        Thread.sleep(100);
      }
    } catch (Exception ex) {
      LOGGER.error("Error While fecthing the data from searcher ", ex);
    } finally {
      LOGGER.warn("Total time taken while populating the reverse uniqueId to DocId mapping with {} entries is {} ms",
          map.size(), System.currentTimeMillis() - startTime);
      uniqueIdToDocIdMap = map;
      status.set(false);
    }
  }

  private void refreshMapFromRedis(SolrIndexSearcher searcher) {
    long startTime = System.currentTimeMillis();
    if (mapRefresh.get()) {
      LOGGER.error("Already map refresh is in progress");

    }
    mapRefresh.set(true);
    Map<String, Map<Double, Set<Integer>>> tokenMap = new HashMap<>();
    Map<String, Map<BytesRef, Set<Integer>>> byteRefTokenMap = new HashMap<>();
    Map<String, Map<Double, DocSet>> docSetMap = new HashMap<>();
    Map<String, Map<Integer, Double>> valueMap = new HashMap<>();
    RedisCommands<String, String> sync = redisClient.connect().sync();
    Map<String, String> hgetall = sync.hgetall(redisKey);
    try {
      uniqueIdToDocIdMap.forEach((id,docId)->{
        String data = sync.hget(redisKey,id);
        if(!StringUtils.isEmpty(data)){
          try {
            Map<String, Double> map = objectMapper.readValue(data, new TypeReference<Map<String, Double>>() {});
            map.forEach((key,value)->{
              tokenMap.computeIfAbsent(key, k -> new HashMap<>())
                  .computeIfAbsent(value, k -> new HashSet<>())
                  .add(docId);
              BytesRefBuilder br = new BytesRefBuilder();
              FieldType type = searcher.getSchema().getField(key).getType();
              if (type instanceof IntValueFieldType) {
                type.readableToIndexed(String.valueOf(value.intValue()), br);
              } else {
                type.readableToIndexed(String.valueOf(value), br);
              }
              byteRefTokenMap.computeIfAbsent(key, k -> new HashMap<>()).computeIfAbsent(br.get(), k -> new HashSet<>()).add(docId);
              valueMap.computeIfAbsent(key, k -> new HashMap<>()).put(docId, value);
            });

          } catch (Exception e) {
            LOGGER.error("Error while getting data", e);
          }
        }
      });
      allDocsSet = DocSetUtil.createDocSet(searcher, new MatchAllDocsQuery(), null);
      tokenMap.forEach((key, value) -> docSetMap.put(key, value.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey,
              entry -> allDocsSet.intersection(DocSetUtil.createDocSetFromSet(entry.getValue(), Integer.MAX_VALUE)),DocSet::union,ConcurrentHashMap::new))));
      /*byteRefTokenMap.forEach((key, value) -> docSetMap.put(key, value.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey,
              entry -> docSet.intersection(DocSetUtil.createDocSetFromSet(entry.getValue(), uniqueIdToDocIdMap.size())), DocSet::union,ConcurrentHashMap::new))));*/
    } catch (Exception ex) {
      LOGGER.error("Error while getting data from redis {}", redisKey, ex);
    } finally {
      LOGGER.warn("Field cache map is reloaded with {} entries in {} ms", tokenMap.size(), System.currentTimeMillis() - startTime);
//      invertedIndexMap = tokenMap;
      forwardMap = valueMap;
      mapRefresh.set(false);
//      invertedByteRefIndexMap = byteRefTokenMap;
      invertedValueDocSetMap = docSetMap;
    }

  }

  public DocSet getDocSet(LegacyNumericRangeQuery query) {

    double minVal = (null == query.getMin()) ? Double.MIN_VALUE : query.getMin().doubleValue();
    double maxVal = (null == query.getMax()) ? Double.MAX_VALUE : query.getMax().doubleValue();
    BoundType minBound = query.includesMin() ? BoundType.CLOSED : BoundType.OPEN;
    BoundType maxBound = query.includesMax() ? BoundType.CLOSED : BoundType.OPEN;
    Range<Double> range = Range.range(minVal, minBound, maxVal, maxBound);
    return Optional.ofNullable(invertedValueDocSetMap.get(query.getField()))
        .map(data -> data
            .entrySet()
            .stream()
            .filter(entry -> range.contains(entry.getKey()))
            .map(Map.Entry::getValue)
            .reduce(DocSet::union)
            .orElse(DocSet.EMPTY))
        .orElse(null);

  }

  /*public static Set<Integer> getDocSet(LegacyNumericRangeQuery query) {

    double minVal = (null == query.getMin()) ? Double.MIN_VALUE : query.getMin().doubleValue();
    double maxVal = (null == query.getMax()) ? Double.MAX_VALUE : query.getMax().doubleValue();
    BoundType minBound = query.includesMin() ? BoundType.CLOSED : BoundType.OPEN;
    BoundType maxBound = query.includesMax() ? BoundType.CLOSED : BoundType.OPEN;
    Range<Double> range = Range.range(minVal, minBound, maxVal, maxBound);
    return Optional.ofNullable(invertedIndexMap.get(query.getField())).map(data ->data
        .entrySet()
        .stream()
        .filter(entry -> range.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toSet())).orElse(null);
  }*/

  public Double getVal(int docId, String field) {
    return Optional.ofNullable(forwardMap.get(field)).map(map -> map.get(docId)).orElse(0.0D);
  }

  public Double getValForStat(int docId, String field) {
    return Optional.ofNullable(forwardMap.get(field)).map(map -> map.get(docId)).orElse(null);
  }

  public Map<Integer, Double> getMap(String fieldName) {
    return forwardMap.get(fieldName);
  }

  @Override
  public void close() throws Exception {

    try {
      if (null != con) {
        con.removeListener(redisPubSubAdapter);
        con.close();
        forwardMap.clear();
        invertedValueDocSetMap.clear();
      }
    } catch (Exception ex) {
      LOGGER.error("Error while closing redis listener", ex);
    }


  }
}
