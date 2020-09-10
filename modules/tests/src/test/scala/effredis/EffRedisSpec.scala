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

class EffRedisSpec extends EffRedisFunSuite with TestListScenarios {
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
}
