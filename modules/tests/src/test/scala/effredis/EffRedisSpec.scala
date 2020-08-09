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

class EffRedisSpec extends EffRedisFunSuite with TestStringScenarios {
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
}
