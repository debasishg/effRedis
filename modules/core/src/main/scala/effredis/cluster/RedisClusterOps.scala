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

import scala.concurrent.duration._

import cats.effect._
import cats.implicits._

import effredis.{ Error, Log, Resp, Value }
import effredis.codecs._
import effredis.algebra.StringApi._

abstract class RedisClusterOps[F[+_]: Concurrent: ContextShift: Log: Timer] { self: RedisClusterClient[F] =>

  def onANode[R](fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] =
    topology.nodes.headOption
      .map(fn)
      .getOrElse(F.raiseError(new IllegalArgumentException("No cluster node found")))

  def onAllNodes(fn: RedisClusterNode[F] => F[Resp[Boolean]]): F[Resp[Boolean]] = {
    val _ = topology.nodes.foreach(fn)
    Value(true).pure[F]
  }

  def forKey[R](key: String)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slot = HashSlot.find(key)
    val node = topology.nodes.filter(_.hasSlot(slot)).headOption

    F.info(s"Command mapped to slot $slot node uri ${node.get.uri}") *>
      executeOnNode(node, slot, key)(fn).flatMap {
        case r @ Value(_) => r.pure[F]
        case Error(err) =>
          F.error(s"Error from server $err - will retry") *>
              retryForMoved(err, key)(fn)
        case err => F.raiseError(new IllegalStateException(s"Unexpected response from server $err"))
      }
  }

  def cset(key: Any, value: Any, whenSet: SetBehaviour = Always, expire: Duration = null, keepTTL: Boolean = false)(
      implicit format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.set(key, value, whenSet, expire, keepTTL)
      }
    }

  def cget[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.get[A](key)
      }
    }

  def clpush(key: Any, value: Any, values: Any*)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lpush(key, value, values)
      }
    }

  def executeOnNode[R](node: Option[RedisClusterNode[F]], slot: Int, key: String)(
      fn: RedisClusterNode[F] => F[Resp[R]]
  ): F[Resp[R]] =
    node
      .map(fn)
      .getOrElse(
        F.raiseError(
          new IllegalArgumentException(
            s"Redis Cluster Node $node not found corresponding to slot $slot for $key"
          )
        )
      )

  def retryForMoved[R](err: String, key: String)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] =
    if (err.startsWith("MOVED")) {
      val parts = err.split(" ")
      val slot  = parts(1).toInt

      F.info(s"Retrying with ${parts(1)} ${parts(2)}") *> {
        if (parts.size != 3) {
          F.raiseError(
            new IllegalStateException(s"Expected error for MOVED to contain 3 parts (MOVED, slot, URI) - found $err")
          )
        } else {
          val node = topology.nodes.filter(_.hasSlot(slot)).headOption
          executeOnNode(node, slot, key)(fn)
        }
      }
    } else {
      F.raiseError(
        new IllegalStateException(
          s"Expected MOVED error but found $err"
        )
      )
    }

  def forKeys[R](key: String, keys: String*)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slots = (key :: keys.toList).map(HashSlot.find(_))
    if (slots.forall(_ == slots.head)) {
      val node = topology.nodes.filter(_.hasSlot(slots.head)).headOption
      node
        .map(fn)
        .getOrElse(
          F.raiseError(
            new IllegalArgumentException(
              s"Slot not found corresponding to keys ${(key :: keys.toList).mkString(",")}"
            )
          )
        )
    } else {
      F.raiseError(
        new IllegalArgumentException(
          s"Keys ${(key :: keys.toList).mkString(",")} do not map to the same slot"
        )
      )
    }
  }

  /**
    * sets the key with the specified value.
    * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
    *
    * NX -- Only set the key if it does not already exist.
    * XX -- Only set the key if it already exist.
    * PX milliseconds -- Set the specified expire time, in milliseconds.
    */
  def set(key: Any, value: Any, whenSet: SetBehaviour = Always, expire: Duration = null, keepTTL: Boolean = false)(
      implicit format: Format
  ): F[Resp[Boolean]] = forKey(key.toString) {
    _.managedClient.use {
      _.value.set(key, value, whenSet, expire, keepTTL)
    }
  }

  /**
    * gets the value for the specified key.
    */
  def get[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.get(key)
      }
    }

  /**
    * is an atomic set this value and return the old value command.
    */
  def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.getset[A](key, value)
      }
    }

  /**
    * sets the value for the specified key, only if the key is not there.
    */
  def setnx(key: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.setnx(key, value)
      }
    }

  def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.setex(key, expiry, value)
      }
    }

  def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.psetex(key, expiryInMillis, value)
      }
    }

  /**
    * increments the specified key by 1
    */
  def incr(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.incr(key)
      }
    }

  /**
    * increments the specified key by increment
    */
  def incrby(key: Any, increment: Long)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.incrby(key, increment)
      }
    }

  def incrbyfloat(key: Any, increment: Float)(implicit format: Format): F[Resp[Option[Float]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.incrbyfloat(key, increment)
      }
    }

  /**
    * decrements the specified key by 1
    */
  def decr(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.decr(key)
      }
    }

  /**
    * decrements the specified key by increment
    */
  def decrby(key: Any, increment: Long)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.decrby(key, increment)
      }
    }

  /**
    * get the values of all the specified keys.
    */
  def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.mget[A](key, keys: _*)
      }
    }

  /**
    * set the respective key value pairs. Overwrite value if key exists
    */
  def mset(kvs: (Any, Any)*)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(kvs.head._1.toString, kvs.tail.map(_._1.toString): _*) {
      _.managedClient.use {
        _.value.mset(kvs: _*)
      }
    }

  /**
    * set the respective key value pairs. Noop if any key exists
    */
  def msetnx(kvs: (Any, Any)*)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(kvs.head._1.toString, kvs.tail.map(_._1.toString): _*) {
      _.managedClient.use {
        _.value.msetnx(kvs: _*)
      }
    }

  /**
    * SETRANGE key offset value
    * Overwrites part of the string stored at key, starting at the specified offset,
    * for the entire length of value.
    */
  def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.setrange(key, offset, value)
      }
    }

  /**
    * Returns the substring of the string value stored at key, determined by the offsets
    * start and end (both are inclusive).
    */
  def getrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.getrange[A](key, start, end)
      }
    }

  /**
    * gets the length of the value associated with the key
    */
  def strlen(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.strlen(key)
      }
    }

  /**
    * appends the key value with the specified value.
    */
  def append(key: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.append(key, value)
      }
    }

  /**
    * Returns the bit value at offset in the string value stored at key
    */
  def getbit(key: Any, offset: Int)(implicit format: Format): F[Resp[Option[Int]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.getbit(key, offset)
      }
    }

  /**
    * Sets or clears the bit at offset in the string value stored at key
    */
  def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): F[Resp[Option[Int]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.setbit(key, offset, value)
      }
    }

  /**
    * Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
    */
  def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): F[Resp[Option[Int]]] =
    forKeys(destKey.toString, srcKeys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.bitop(op, destKey, srcKeys: _*)
      }
    }

  /**
    * Count the number of set bits in the given key within the optional range
    */
  def bitcount(key: Any, range: Option[(Int, Int)] = None)(implicit format: Format): F[Resp[Option[Int]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.bitcount(key, range)
      }
    }

  /**
    * add values to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpush(key: Any, value: Any, values: Any*)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lpush(key, value, values)
      }
    }

  /**
    * add value to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lpushx(key, value)
      }
    }

  /**
    * add values to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.rpush(key, value, values)
      }
    }

  /**
    * add value to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.rpushx(key, value)
      }
    }

  /**
    * return the length of the list stored at the specified key.
    * If the key does not exist zero is returned (the same behaviour as for empty lists).
    * If the value stored at key is not a list an error is returned.
    */
  def llen(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.llen(key)
      }
    }

  /**
    * return the specified elements of the list stored at the specified key.
    * Start and end are zero-based indexes.
    */
  def lrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[Option[A]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lrange(key, start, end)
      }
    }

  /**
    * Trim an existing list so that it will contain only the specified range of elements specified.
    */
  def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.ltrim(key, start, end)
      }
    }

  /**
    * return the especified element of the list stored at the specified key.
    * Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
    */
  def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lindex(key, index)
      }
    }

  /**
    * set the list element at index with the new value. Out of range indexes will generate an error
    */
  def lset(key: Any, index: Int, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lset(key, index, value)
      }
    }

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def lrem(key: Any, count: Int, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lrem(key, count, value)
      }
    }

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.lpop(key)
      }
    }

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.rpop(key)
      }
    }

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString) {
      _.managedClient.use {
        _.value.rpoplpush[A](srcKey, dstKey)
      }
    }

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString) {
      _.managedClient.use {
        _.value.brpoplpush[A](srcKey, dstKey, timeoutInSeconds)
      }
    }

  def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.blpop[K, V](timeoutInSeconds, key, keys: _*)
      }
    }

  def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.brpop[K, V](timeoutInSeconds, key, keys: _*)
      }
    }

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @deprecated return value semantics is inconsistent with [[effredis.HashOperations#hsetnx]] and
    *             [[effredis.HashOperations#hmset]]. Use [[effredis.HashOperations#hset1]] instead
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>False</code> if <code>field</code> already exists in the hash and the value was updated.
    *
    */
  def hset(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hset(key, field, value)
      }
    }

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @return <code>Some(0)</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>Some(1)</code> if <code>field</code> already exists in the hash and the value was updated.
    */
  def hset1(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hset1(key, field, value)
      }
    }

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>, only if field does not yet exist.
    * If key does not exist, a new key holding a hash is created.
    * If field already exists, this operation has no effect.
    *
    * @see [[http://redis.io/commands/hsetnx HSETNX documentation]]
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set.
    *         </code>False</code> if <code>field</code> exists in the hash and no operation was performed.
    */
  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hsetnx(key, field, value)
      }
    }

  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hget[A](key, field)
      }
    }

  /**
    * Sets the specified fields to their respective values in the hash stored at key.
    * This command overwrites any existing fields in the hash.
    * If key does not exist, a new key holding a hash is created.
    *
    * @param map from fields to values
    * @see [[http://redis.io/commands/hmset HMSET documentation]]
    * @return <code>True</code> if operation completed successfully,
    *         <code>False</code> otherwise.
    */
  def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hmset(key, map)
      }
    }

  def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): F[Resp[Option[Map[K, V]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hmget[K, V](key, fields: _*)
      }
    }

  def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hincrby(key, field, value)
      }
    }

  def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): F[Resp[Option[Float]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hincrbyfloat(key, field, value)
      }
    }

  def hexists(key: Any, field: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hexists(key, field)
      }
    }

  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hdel(key, field, fields)
      }
    }

  def hlen(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hlen(key)
      }
    }

  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hkeys[A](key)
      }
    }

  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hvals(key)
      }
    }

  @deprecated(
    "Use the more idiomatic variant hgetall1, which has the returned Map behavior more consistent. See issue https://github.com/debasishg/scala-redis/issues/122",
    "3.2"
  )
  def hgetall[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hgetall[K, V](key)
      }
    }

  def hgetall1[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hgetall1[K, V](key)
      }
    }

  /**
    * Incrementally iterate hash fields and associated values (since 2.8)
    */
  def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.hscan(key, cursor, pattern, count)
      }
    }

  /**
    * sort keys in a set, and optionally pull values for them
    */
  def sort[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.sort[A](key, limit, desc, alpha, by, get)
      }
    }

  /**
    * sort keys in a set, and stores result in the supplied key
    */
  def sortNStore[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil,
      storeAt: String
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.sortNStore[A](key, limit, desc, alpha, by, get, storeAt)
      }
    }

  /**
    * returns all the keys matching the glob-style pattern.
    */
  def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    conc.raiseError(new NotAllowedInClusterError(s"KEYS $pattern $format $parse not allowed in cluster mode"))

  /**
    * returns the current server time as a two items lists:
    * a Unix timestamp and the amount of microseconds already elapsed in the current second.
    */
  def time[A](implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    onANode(_.managedClient.use {
      _.value.time[A]
    })

  /**
    * returns a randomly selected key from the currently selected DB.
    */
  def randomkey[A](implicit parse: Parse[A]): F[Resp[Option[A]]] =
    onANode(_.managedClient.use {
      _.value.randomkey[A]
    })

  /**
    * atomically renames the key oldkey to newkey.
    */
  def rename(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(oldkey.toString, newkey.toString) {
      _.managedClient.use {
        _.value.rename(oldkey, newkey)
      }
    }

  /**
    * rename oldkey into newkey but fails if the destination key newkey already exists.
    */
  def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(oldkey.toString, newkey.toString) {
      _.managedClient.use {
        _.value.renamenx(oldkey, newkey)
      }
    }

  /**
    * returns the size of the db.
    */
  def dbsize: F[Resp[Option[Long]]] =
    onANode(_.managedClient.use {
      _.value.dbsize
    })

  /**
    * test if the specified key exists.
    */
  def exists(key: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.exists(key)
      }
    }

  /**
    * deletes the specified keys.
    */
  def del(key: Any, keys: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.del(key, keys: _*)
      }
    }

  /**
    * returns the type of the value stored at key in form of a string.
    */
  def getType(key: Any)(implicit format: Format): F[Resp[Option[String]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.getType(key)
      }
    }

  /**
    * sets the expire time (in sec.) for the specified key.
    */
  def expire(key: Any, ttl: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.expire(key, ttl)
      }
    }

  /**
    * sets the expire time (in milli sec.) for the specified key.
    */
  def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.pexpire(key, ttlInMillis)
      }
    }

  /**
    * sets the expire time for the specified key.
    */
  def expireat(key: Any, timestamp: Long)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.expireat(key, timestamp)
      }
    }

  /**
    * sets the expire timestamp in millis for the specified key.
    */
  def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.pexpireat(key, timestampInMillis)
      }
    }

  /**
    * returns the remaining time to live of a key that has a timeout
    */
  def ttl(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.ttl(key)
      }
    }

  /**
    * returns the remaining time to live of a key that has a timeout in millis
    */
  def pttl(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.pttl(key)
      }
    }

  /**
    * selects the DB to connect, defaults to 0 (zero).
    */
  def select(index: Int): F[Resp[Boolean]] =
    conc.raiseError(new NotAllowedInClusterError(s"SELECT $index not allowed in cluster mode"))

  /**
    * removes all the DB data.
    */
  def flushdb: F[Resp[Boolean]] =
    onANode(_.managedClient.use {
      _.value.flushdb
    })

  /**
    * removes data from all the DB's.
    */
  def flushall: F[Resp[Boolean]] =
    onANode(_.managedClient.use {
      _.value.flushall
    })

  /**
    * Move the specified key from the currently selected DB to the specified destination DB.
    */
  def move(key: Any, db: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.move(key, db)
      }
    }

  /**
    * exits the server.
    */
  def quit: F[Resp[Boolean]] =
    onANode(_.managedClient.use {
      _.value.quit
    })

  /**
    * auths with the server.
    */
  def auth(secret: Any)(implicit format: Format): F[Resp[Boolean]] =
    onANode(_.managedClient.use {
      _.value.auth(secret)
    })

  /**
    * Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
    * to persistent (a key that will never expire as no timeout is associated).
    */
  def persist(key: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.persist(key)
      }
    }

  /**
    * Incrementally iterate the keys space (since 2.8)
    */
  def scan[A](
      cursor: Int,
      pattern: Any = "*",
      count: Int = 10
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    conc.raiseError(new NotAllowedInClusterError(s"SCAN $cursor $pattern $count not allowed in cluster mode"))

  /**
    * ping
    */
  def ping: F[Resp[Option[String]]] =
    onANode(_.managedClient.use {
      _.value.ping
    })

  protected val pong: Option[String] = Some("PONG")

  /**
    * Marks the given keys to be watched for conditional execution of a transaction.
    */
  def watch(key: Any, keys: Any*)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) {
      _.managedClient.use {
        _.value.watch(key, keys: _*)
      }
    }

  /**
    * Flushes all the previously watched keys for a transaction
    */
  def unwatch(): F[Resp[Boolean]] =
    onAllNodes(_.managedClient.use {
      _.value.unwatch()
    })

  /**
    * CONFIG GET
    */
  def getConfig(key: Any = "*")(implicit format: Format): F[Resp[Option[Map[String, Option[String]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.getConfig(key)
      }
    }

  /**
    * CONFIG SET
    */
  def setConfig(key: Any, value: Any)(implicit format: Format): F[Resp[Option[String]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.setConfig(key, value)
      }
    }

  def echo(message: Any)(implicit format: Format): F[Resp[Option[String]]] =
    onANode(_.managedClient.use {
      _.value.echo(message)
    })

  /**
    * Add the specified members to the set value stored at key. (VARIADIC: >= 2.4)
    */
  def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.sadd(key, value, values)
      }
    }

  /**
    * Remove the specified members from the set value stored at key. (VARIADIC: >= 2.4)
    */
  def srem(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.srem(key, value, values)
      }
    }

  /**
    * Remove and return (pop) a random element from the Set value at key.
    */
  def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.spop(key)
      }
    }

  /**
    * Remove and return multiple random elements (pop) from the Set value at key since (3.2).
    */
  def spop[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): F[Resp[Option[Set[Option[A]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.spop(key, count)
      }
    }

  /**
    * Move the specified member from one Set to another atomically.
    */
  def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(sourceKey.toString, destKey.toString) {
      _.managedClient.use {
        _.value.smove(sourceKey, destKey, value)
      }
    }

  /**
    * Return the number of elements (the cardinality) of the Set at key.
    */
  def scard(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.scard(key)
      }
    }

  /**
    * Test if the specified value is a member of the Set at key.
    */
  def sismember(key: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.sismember(key, value)
      }
    }

  /**
    * Return the intersection between the Sets stored at key1, key2, ..., keyN.
    */
  def sinter[A](key: Any, keys: Any*)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[Set[Option[A]]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sinter[A](key, keys)
      }
    }

  /**
    * Compute the intersection between the Sets stored at key1, key2, ..., keyN,
    * and store the resulting Set at dstkey.
    * SINTERSTORE returns the size of the intersection, unlike what the documentation says
    * refer http://code.google.com/p/redis/issues/detail?id=121
    */
  def sinterstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sinterstore(key, keys)
      }
    }

  /**
    * Return the union between the Sets stored at key1, key2, ..., keyN.
    */
  def sunion[A](key: Any, keys: Any*)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[Set[Option[A]]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sunion[A](key, keys)
      }
    }

  /**
    * Compute the union between the Sets stored at key1, key2, ..., keyN,
    * and store the resulting Set at dstkey.
    * SUNIONSTORE returns the size of the union, unlike what the documentation says
    * refer http://code.google.com/p/redis/issues/detail?id=121
    */
  def sunionstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sunionstore(key, keys)
      }
    }

  /**
    * Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
    */
  def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): F[Resp[Option[Set[Option[A]]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sdiff[A](key, keys)
      }
    }

  /**
    * Compute the difference between the Set key1 and all the Sets key2, ..., keyN,
    * and store the resulting Set at dstkey.
    */
  def sdiffstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(key.toString, keys.map(_.toString): _*) {
      _.managedClient.use {
        _.value.sdiffstore(key, keys)
      }
    }

  /**
    * Return all the members of the Set value at key.
    */
  def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[Set[Option[A]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.smembers(key)
      }
    }

  /**
    * Return a random element from a Set
    */
  def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.srandmember[A](key)
      }
    }

  /**
    * Return multiple random elements from a Set (since 2.6)
    */
  def srandmember[A](key: Any, count: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[Option[A]]]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.srandmember[A](key, count)
      }
    }

  /**
    * Incrementally iterate Set elements (since 2.8)
    */
  def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    forKey(key.toString) {
      _.managedClient.use {
        _.value.sscan[A](key, cursor, pattern, count)
      }
    }
}

class NotAllowedInClusterError(message: String) extends RuntimeException(message)
