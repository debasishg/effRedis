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

package effredis.cluster

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

/**
  * Redis Cluster does not use consistent hashing, but a different form of sharding where
  * every key is conceptually part of what we call an hash slot.
  * There are 16384 hash slots in Redis Cluster, and to compute what is the hash slot of a given key,
  * we simply take the CRC16 of the key modulo 16384.(from https://redis.io/topics/cluster-tutorial)
  */
object HashSlot {
  final val TOTAL_HASH_SLOTS = 16384

  def find(key: String)(implicit C: Charset = StandardCharsets.UTF_8): Int = {
    val toHash = hashKey(key)
    CRC16.crc16(toHash) % TOTAL_HASH_SLOTS
  }

  def hashKey(key: String): String = {
    val s = key.indexOf('{')
    if (s >= 0) {
      val e = key.indexOf('}')
      if (e >= 0 && e != s + 1) key.substring(s + 1, e)
      else key
    } else key
  }
}
