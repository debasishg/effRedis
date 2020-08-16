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
