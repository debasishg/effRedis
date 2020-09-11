/*
 * Copyright 2020 Debasish Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package effredis

// import java.net.URI

class EffRedisSpec
    extends EffRedisFunSuite
    with TestListScenarios
    with TestStringScenarios
    with TestHashScenarios
    with TestSetScenarios
    with TestSortedSetScenarios
    with TestBaseScenarios
    with cluster.TestClusterScenarios {
  // test("parse cluster slots")(withRedisForURI(new URI("http://localhost:7000"))(parseClusterSlots))
  test("strings api get and set")(withRedis(stringsGetAndSet))
  test("strings api get and set if exists or not")(withRedis(stringsGetAndSetIfExistsOrNot))
  test("strings api getset")(withRedis(stringsGetSet))
  test("strings api setnx setex")(withRedis(stringsSetNxEx))
  test("strings api incr")(withRedis(stringsIncr))
  test("strings api decr")(withRedis(stringsDecr))
  test("strings api mget")(withRedis(stringsMget))
  test("strings api mset")(withRedis(stringsMset))
  test("strings api with spaces in keys")(withRedis(stringsWithSpacesInKeys))
  test("strings api get set range")(withRedis(stringsGetSetRange))
  test("strings api strlen")(withRedis(stringsStrlen))
  test("strings api append")(withRedis(stringsAppend))
  test("strings api bit manipulation")(withRedis(stringsBitManip))
  test("list api lpush")(withRedis(listsLPushNil))
  test("list api lpush")(withRedis(listsLPush))
  test("list api rpush")(withRedis(listsRPush))
  test("list api llen")(withRedis(listsLlen))
  test("list api lrange")(withRedis(listsLrange))
  test("list api ltrim")(withRedis(listsLtrim))
  test("list api lindex")(withRedis(listsLIndex))
  test("list api lset")(withRedis(listsLSet))
  test("list api lrem")(withRedis(listsLRem))
  test("list api lpop")(withRedis(listsLPop))
  test("list api rpop")(withRedis(listsRPop))
  test("list api rpoplpush")(withRedis(listsRPopLPush))
  test("list api push pop with nl")(withRedis(listsLPushPopWithNL))
  test("list api push pop with array bytes")(withRedis(listsLPushPopWithArrayBytes))
  test("list api brpoplpush")(withRedis(listsBRPoplPush))
  test("list api brpoplpush with blocking")(withRedis2(listsBRPoplPushWithBlockingPop))
  test("list api blocking with blpop")(withRedis2(listsBLPop))
  test("hash hset 1")(withRedis(hashHSet1))
  test("hash hset 2")(withRedis(hashHSet2))
  test("hash hgetall")(withRedis(hashHGetAll))
  test("sets api add")(withRedis(setsAdd))
  test("sets api add variadic")(withRedis(setsAddVariadic))
  test("sets api rem")(withRedis(setsRem))
  test("sets api rem variadic")(withRedis(setsRemVariadic))
  test("sets api pop")(withRedis(setsPop))
  test("sets api pop with count")(withRedis(setsPopWithCount))
  test("sets api move")(withRedis(setsMove))
  test("sets api card")(withRedis(setsCard))
  test("sets api ismember")(withRedis(setsIsMember))
  test("sets api intersection")(withRedis(setsInter))
  test("sets api intersection")(withRedis(setsInterstore))
  test("sets api union")(withRedis(setsUnion))
  test("sets api unionstore")(withRedis(setsUnionstore))
  test("sets api diff")(withRedis(setsDiff))
  test("sets api member")(withRedis(setsMember))
  test("sets api random member")(withRedis(setsRandomMemberWithCount))
  test("sortedsets zrangebylex")(withRedis(sortedSetsZrangeByLex))
  test("sortedsets zadd")(withRedis(sortedSetsZAdd))
  test("sortedsets zrem")(withRedis(sortedSetsZRem))
  test("sortedsets zrange")(withRedis(sortedSetsZRange))
  test("sortedsets zrank")(withRedis(sortedSetsZRank))
  test("sortedsets zremrange")(withRedis(sortedSetsZRemRange))
  test("sortedsets zunion")(withRedis(sortedSetsZUnion))
  test("sortedsets zinter")(withRedis(sortedSetsZInter))
  test("sortedsets zcount")(withRedis(sortedSetsZCount))
  test("sortedsets zrangebyscore")(withRedis(sortedSetsZRangeByScore))
  test("sortedsets zrangebyscorewithscore")(withRedis(sortedSetsZRangeByScoreWithScore))
  test("base api misc-1")(withRedis(baseMisc1))
  test("base api misc-2")(withRedis(baseMisc2))
  test("base api misc-3")(withRedis(baseMisc3))
  test("base api misc-4")(withRedis(baseMisc4))
  test("base api misc-5")(withRedis(baseMisc5))
}
