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

import io.chrisdavenport.keypool._
import effredis.RedisClient
import java.net.URI

import cats.effect._
import cats.syntax.all._

import effredis.{ Error, Log, Resp, Value }
import effredis.codecs._
import effredis.algebra.StringApi._
import effredis.RedisClient._

abstract class RedisClusterOps[F[+_]: Concurrent: ContextShift: Log: Timer, M <: Mode] {
  self: RedisClusterClient[F, M] =>

  /*
   * Run the function on one specific node of the cluster. This is given by the
   * slot that the node contains.
   */
  private def onANode[R](fn: RedisClusterNode => F[Resp[R]]): F[Resp[R]] =
    topologyCache.get.flatMap { t =>
      t.nodes.headOption
        .map(fn)
        .getOrElse(F.raiseError(new IllegalArgumentException("No cluster node found")))
    }

  /**
    * Run the function on all nodes of the cluster. Currently it's only side-effects
    * and does not implement any form of aggregation.
    *
    * TODO: Need to check: some commands like flushall are not allowed on replica
    * nodes. Need to eliminate them
    */
  private def onAllNodes[R](fn: RedisClusterNode => F[Resp[R]]): F[List[Resp[R]]] =
    topologyCache.get.flatMap {
      _.nodes
        .map(fn)
        .sequence
        .pure[F]
        .flatten
    }

  /**
    * Runs the function in the node that the key hashes to. Implements a retry
    * semantics on getting a MOVED error from the server.
    *
    * @param key the redis key for the command
    * @param fn the fucntion to execute
    * @return the response in F
    */
  private def forKey[R](
      key: String
  )(fn: RedisClusterNode => F[Resp[R]])(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[R]] = {
    val slot = HashSlot.find(key)
    val node = topologyCache.get.map(_.nodes.filter(_.hasSlot(slot)).headOption)

    node.flatMap { n =>
      F.info(s"Command with key $key mapped to slot $slot node uri ${n.get.uri}") *>
        executeOnNode(n, slot, List(key))(fn).flatMap {
          case r @ Value(_) => r.pure[F]
          case Error(err) =>
            F.error(s"Error from server $err for key $key originally mapped to $slot - will retry") *>
                retryForMovedOrAskRedirection(err, List(key))(fn)
          case err => F.raiseError(new IllegalStateException(s"Unexpected response from server $err"))
        }
    }
  }

  /**
    * The execution function for the key.
    */
  private def executeOnNode[R](node: Option[RedisClusterNode], slot: Int, keys: List[String])(
      fn: RedisClusterNode => F[Resp[R]]
  ): F[Resp[R]] =
    node
      .map(fn)
      .getOrElse(
        F.raiseError(
          new IllegalArgumentException(
            s"""Redis Cluster Node $node not found corresponding to slot $slot for [${keys.mkString(",")}]"""
          )
        )
      )

  /**
    * Retry semantics for MOVED or ASK redirection errors
    *
    * @param err the error string
    * @param key the redis key involved in the operation
    * @param fn the function to run
    * @return the response from redis server
    */
  private def retryForMovedOrAskRedirection[R](err: String, keys: List[String])(
      fn: RedisClusterNode => F[Resp[R]]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[R]] =
    if (err.startsWith("MOVED")) retryForMovedRedirection(err, keys)(fn)
    else if (err.startsWith("ASK")) retryForAskRedirection(err, keys)(fn)
    else {
      F.raiseError(
        new IllegalStateException(
          s"Expected MOVED or ASK redirection but found $err"
        )
      )
    }

  /**
    * Retry semantics for MOVED redirection errors
    *
    * @param err the error string
    * @param key the redis key involved in the operation
    * @param fn the function to run
    * @return the repsonse from redis server
    */
  def retryForMovedRedirection[R](err: String, keys: List[String])(
      fn: RedisClusterNode => F[Resp[R]]
  ): F[Resp[R]] = {
    val parts = err.split(" ")
    val slot  = parts(1).toInt

    F.debug(s"Got MOVED redirection: Retrying with ${parts(1)} ${parts(2)}") *> {
      if (parts.size != 3) {
        F.raiseError(
          new IllegalStateException(
            s"Expected error for MOVED redirection to contain 3 parts (MOVED, slot, URI) - found $err"
          )
        )
      } else {
        val node = topologyCache.get.flatMap(_.nodes.filter(_.hasSlot(slot)).headOption.pure[F])
        node.flatMap(executeOnNode(_, slot, keys)(fn)) <* topologyCache.expire
      }
    }
  }

  /**
    * Retry semantics for ASK redirection errors
    *
    * @param err the error string
    * @param key the redis key involved in the operation
    * @param fn the function to run
    * @return the repsonse from redis server
    */
  def retryForAskRedirection[R](err: String, keys: List[String])(
      fn: RedisClusterNode => F[Resp[R]]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[R]] = {
    val parts = err.split(" ")
    val slot  = parts(1).toInt

    F.debug(s"Got ASK redirection: Retrying with ${parts(1)} ${parts(2)}") *> {
      if (parts.size != 3) {
        F.raiseError(
          new IllegalStateException(
            s"Expected error for ASK redirection to contain 3 parts (ASK, slot, URI) - found $err"
          )
        )
      } else {
        val node = topologyCache.get.flatMap(t => t.nodes.filter(_.hasSlot(slot)).headOption.pure[F])
        node.flatMap(n => executeOnNode(n, slot, keys)(_ => asking(pool) *> fn(n.get)))
      }
    }
  }

  /**
    * Runs the function in the node that the keys hash to. Implements a retry
    * semantics on getting a MOVED error from the server.
    *
    * @param key the redis key for the command
    * @param fn the fucntion to execute
    * @return the response in F
    */
  private def forKeys[R](key: String, keys: String*)(
      fn: RedisClusterNode => F[Resp[R]]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[R]] = {
    val slots = (key :: keys.toList).map(HashSlot.find(_))
    if (slots.forall(_ == slots.head)) forKey(key)(fn)
    else {
      F.raiseError(
        new IllegalArgumentException(
          s"Keys ${(key :: keys.toList).mkString(",")} do not map to the same slot"
        )
      )
    }
  }

  // String Operations

  /**
    * sets the key with the specified value.
    * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
    *
    * NX -- Only set the key if it does not already exist.
    * XX -- Only set the key if it already exist.
    * PX milliseconds -- Set the specified expire time, in milliseconds.
    */
  final def set(
      key: Any,
      value: Any,
      whenSet: SetBehaviour = Always,
      expire: Duration = null,
      keepTTL: Boolean = false
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.set(key, value, whenSet, expire, keepTTL)
      }
    }

  /**
    * gets the value for the specified key.
    */
  final def get[A](
      key: Any
  )(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.get(key)
      }
    }

  final def asking[A](implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Boolean]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.asking
      }
    )

  /**
    * is an atomic set this value and return the old value command.
    */
  final def getset[A](
      key: Any,
      value: Any
  )(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.getset[A](key, value)
      }
    }

  /**
    * sets the value for the specified key, only if the key is not there.
    */
  final def setnx(
      key: Any,
      value: Any
  )(implicit format: Format, pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.setnx(key, value)
      }
    }

  final def setex(key: Any, expiry: Long, value: Any)(
      implicit format: Format,
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.setex(key, expiry, value)
      }
    }

  final def psetex(key: Any, expiryInMillis: Long, value: Any)(
      implicit format: Format,
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.psetex(key, expiryInMillis, value)
      }
    }

  /**
    * increments the specified key by 1
    */
  final def incr(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.incr(key)
      }
    }

  /**
    * increments the specified key by increment
    */
  final def incrby(
      key: Any,
      increment: Long
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.incrby(key, increment)
      }
    }

  final def incrbyfloat(
      key: Any,
      increment: Float
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Option[Float]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.incrbyfloat(key, increment)
      }
    }

  /**
    * decrements the specified key by 1
    */
  final def decr(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.decr(key)
      }
    }

  /**
    * decrements the specified key by increment
    */
  final def decrby(
      key: Any,
      increment: Long
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.decrby(key, increment)
      }
    }

  /**
    * get the values of all the specified keys.
    */
  final def mget[A](key: Any, keys: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.mget[A](key, keys: _*)
      }
    }

  /**
    * set the respective key value pairs. Overwrite value if key exists
    */
  final def mset(
      kvs: (Any, Any)*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKeys(kvs.head._1.toString, kvs.tail.map(_._1.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.mset(kvs: _*)
      }
    }

  /**
    * set the respective key value pairs. Noop if any key exists
    */
  final def msetnx(
      kvs: (Any, Any)*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKeys(kvs.head._1.toString, kvs.tail.map(_._1.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.msetnx(kvs: _*)
      }
    }

  /**
    * SETRANGE key offset value
    * Overwrites part of the string stored at key, starting at the specified offset,
    * for the entire length of
    */
  final def setrange(key: Any, offset: Int, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.setrange(key, offset, value)
      }
    }

  /**
    * Returns the substring of the string value stored at key, determined by the offsets
    * start and end (both are inclusive).
    */
  final def getrange[A](
      key: Any,
      start: Int,
      end: Int
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.getrange[A](key, start, end)
      }
    }

  /**
    * gets the length of the value associated with the key
    */
  final def strlen(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.strlen(key)
      }
    }

  /**
    * appends the key value with the specified
    */
  final def append(
      key: Any,
      value: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.append(key, value)
      }
    }

  /**
    * Returns the bit value at offset in the string value stored at key
    */
  final def getbit(
      key: Any,
      offset: Int
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.getbit(key, offset)
      }
    }

  /**
    * Sets or clears the bit at offset in the string value stored at key
    */
  final def setbit(key: Any, offset: Int, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.setbit(key, offset, value)
      }
    }

  /**
    * Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
    */
  final def bitop(op: String, destKey: Any, srcKeys: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKeys(destKey.toString, srcKeys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.bitop(op, destKey, srcKeys: _*)
      }
    }

  /**
    * Count the number of set bits in the given key within the optional range
    */
  final def bitcount(
      key: Any,
      range: Option[(Int, Int)] = None
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.bitcount(key, range)
      }
    }

  // List Operations

  /**
    * add values to the head of the list stored at key (Variadic: >= 2.4)
    */
  final def lpush(key: Any, value: Any, values: Any*)(
      implicit format: Format,
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lpush(key, value, values: _*)
      }
    }

  /**
    * add value to the head of the list stored at key (Variadic: >= 2.4)
    */
  final def lpushx(
      key: Any,
      value: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lpushx(key, value)
      }
    }

  /**
    * add values to the tail of the list stored at key (Variadic: >= 2.4)
    */
  final def rpush(key: Any, value: Any, values: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.rpush(key, value, values: _*)
      }
    }

  /**
    * add value to the tail of the list stored at key (Variadic: >= 2.4)
    */
  final def rpushx(
      key: Any,
      value: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.rpushx(key, value)
      }
    }

  /**
    * return the length of the list stored at the specified key.
    * If the key does not exist zero is returned (the same behaviour as for empty lists).
    * If the value stored at key is not a list an error is returned.
    */
  final def llen(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.llen(key)
      }
    }

  /**
    * return the specified elements of the list stored at the specified key.
    * Start and end are zero-based indexes.
    */
  final def lrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lrange(key, start, end)
      }
    }

  /**
    * Trim an existing list so that it will contain only the specified range of elements specified.
    */
  final def ltrim(key: Any, start: Int, end: Int)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.ltrim(key, start, end)
      }
    }

  /**
    * return the especified element of the list stored at the specified key.
    * Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
    */
  final def lindex[A](
      key: Any,
      index: Int
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lindex(key, index)
      }
    }

  /**
    * set the list element at index with the new  Out of range indexes will generate an error
    */
  final def lset(key: Any, index: Int, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lset(key, index, value)
      }
    }

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  final def lrem(key: Any, count: Int, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lrem(key, count, value)
      }
    }

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  final def lpop[A](
      key: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.lpop[A](key)
      }
    }

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  final def rpop[A](
      key: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.rpop[A](key)
      }
    }

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  final def rpoplpush[A](
      srcKey: Any,
      dstKey: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.rpoplpush[A](srcKey, dstKey)
      }
    }

  final def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.brpoplpush[A](srcKey, dstKey, timeoutInSeconds)
      }
    }

  final def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parse: Parse[V],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.blpop[K, V](timeoutInSeconds, key, keys: _*)
      }
    }

  final def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parse: Parse[V],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.brpop[K, V](timeoutInSeconds, key, keys: _*)
      }
    }

  // Hash Operations

  final def hset(key: Any, field: Any, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hset(key, field, value)
      }
    }

  final def hset(
      key: Any,
      map: Iterable[Product2[Any, Any]]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hset(key, map)
      }
    }

  final def hsetnx(key: Any, field: Any, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hsetnx(key, field, value)
      }
    }

  final def hget[A](
      key: Any,
      field: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hget[A](key, field)
      }
    }

  final def hmset(
      key: Any,
      map: Iterable[Product2[Any, Any]]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hmset(key, map)
      }
    }

  final def hmget[K, V](key: Any, fields: K*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parseV: Parse[V]
  ): F[Resp[Map[K, Option[V]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hmget[K, V](key, fields: _*)
      }
    }

  final def hincrby(key: Any, field: Any, value: Long)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hincrby(key, field, value)
      }
    }

  final def hincrbyfloat(key: Any, field: Any, value: Float)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Option[Float]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hincrbyfloat(key, field, value)
      }
    }

  final def hexists(
      key: Any,
      field: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hexists(key, field)
      }
    }

  final def hdel(key: Any, field: Any, fields: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hdel(key, field, fields)
      }
    }

  final def hlen(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hlen(key)
      }
    }

  final def hkeys[A](key: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hkeys[A](key)
      }
    }

  final def hvals[A](key: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hvals(key)
      }
    }

  final def hgetall[K, V](
      key: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[Map[K, V]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hgetall[K, V](key)
      }
    }

  /**
    * Incrementally iterate hash fields and associated values (since 2.8)
    */
  final def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[(Int, List[Option[A]])]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.hscan(key, cursor, pattern, count)
      }
    }

  // Base Operations

  /**
    * sort keys in a set, and optionally pull values for them
    */
  final def sort[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.sort[A](key, limit, desc, alpha, by, get)
      }
    }

  /**
    * sort keys in a set, and stores result in the supplied key
    */
  final def sortNStore[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil,
      storeAt: String
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.sortNStore[A](key, limit, desc, alpha, by, get, storeAt)
      }
    }

  /**
    * returns all the keys matching the glob-style pattern.
    */
  final def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    conc.raiseError(new NotAllowedInClusterError(s"KEYS $pattern $format $parse not allowed in cluster mode"))

  /**
    * returns the current server time as a two items lists:
    * a Unix timestamp and the amount of microseconds already elapsed in the current second.
    */
  final def time(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[List[Option[Long]]]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.time
      }
    )

  /**
    * returns a randomly selected key from the currently selected DB.
    */
  final def randomkey[A](
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.randomkey[A]
      }
    )

  /**
    * atomically renames the key oldkey to newkey.
    */
  final def rename(
      oldkey: Any,
      newkey: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKeys(oldkey.toString, newkey.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.rename(oldkey, newkey)
      }
    }

  /**
    * rename oldkey into newkey but fails if the destination key newkey already exists.
    */
  final def renamenx(
      oldkey: Any,
      newkey: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKeys(oldkey.toString, newkey.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.renamenx(oldkey, newkey)
      }
    }

  /**
    * returns the size of the db.
    */
  final def dbsize(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Long]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.dbsize
      }
    )

  /**
    * test if the specified key exists.
    */
  final def exists(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.exists(key)
      }
    }

  /**
    * deletes the specified keys.
    */
  final def del(
      key: Any,
      keys: Any*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.del(key, keys: _*)
      }
    }

  /**
    * returns the type of the value stored at key in form of a string.
    */
  final def getType(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Option[String]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.getType(key)
      }
    }

  /**
    * sets the expire time (in sec.) for the specified key.
    */
  final def expire(
      key: Any,
      ttl: Int
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.expire(key, ttl)
      }
    }

  /**
    * sets the expire time (in milli sec.) for the specified key.
    */
  final def pexpire(
      key: Any,
      ttlInMillis: Int
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.pexpire(key, ttlInMillis)
      }
    }

  /**
    * sets the expire time for the specified key.
    */
  final def expireat(
      key: Any,
      timestamp: Long
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.expireat(key, timestamp)
      }
    }

  /**
    * sets the expire timestamp in millis for the specified key.
    */
  final def pexpireat(
      key: Any,
      timestampInMillis: Long
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.pexpireat(key, timestampInMillis)
      }
    }

  /**
    * returns the remaining time to live of a key that has a timeout
    */
  final def ttl(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.ttl(key)
      }
    }

  /**
    * returns the remaining time to live of a key that has a timeout in millis
    */
  final def pttl(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.pttl(key)
      }
    }

  /**
    * selects the DB to connect, defaults to 0 (zero).
    */
  final def select(index: Int): F[Resp[Boolean]] =
    conc.raiseError(new NotAllowedInClusterError(s"SELECT $index not allowed in cluster mode"))

  /**
    * removes all the DB data.
    */
  final def flushdb(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[List[Resp[Boolean]]] =
    onAllNodes(node =>
      node.managedClient(pool, node.uri).use {
        _.flushdb
      }
    )

  /**
    * removes data from all the DB's.
    */
  final def flushall(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[List[Resp[Boolean]]] =
    onAllNodes[Boolean](node => node.managedClient(pool, node.uri).use(_.flushall))

  /**
    * Move the specified key from the currently selected DB to the specified destination DB.
    */
  final def move(
      key: Any,
      db: Int
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.move(key, db)
      }
    }

  /**
    * exits the server.
    */
  final def quit(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Boolean]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.quit
      }
    )

  /**
    * auths with the server.
    */
  final def auth(
      secret: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.auth(secret)
      }
    )

  /**
    * Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
    * to persistent (a key that will never expire as no timeout is associated).
    */
  final def persist(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.persist(key)
      }
    }

  /**
    * Incrementally iterate the keys space (since 2.8)
    */
  final def scan[A](
      cursor: Int,
      pattern: Any = "*",
      count: Int = 10
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    conc.raiseError(new NotAllowedInClusterError(s"SCAN $cursor $pattern $count not allowed in cluster mode"))

  /**
    * ping
    */
  final def ping(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[List[Resp[String]]] =
    onAllNodes(node =>
      node.managedClient(pool, node.uri).use {
        _.ping
      }
    )

  protected val pong: Option[String] = Some("PONG")

  /**
    * Marks the given keys to be watched for conditional execution of a transaction.
    */
  final def watch(
      key: Any,
      keys: Any*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.watch(key, keys: _*)
      }
    }

  /**
    * Flushes all the previously watched keys for a transaction
    */
  final def unwatch(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[List[Resp[Boolean]]] =
    onAllNodes(node =>
      node.managedClient(pool, node.uri).use {
        _.unwatch()
      }
    )

  /**
    * CONFIG GET
    */
  final def getConfig(key: Any = "*")(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Option[Map[String, Option[String]]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.getConfig(key)
      }
    }

  /**
    * CONFIG SET
    */
  final def setConfig(
      key: Any,
      value: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.setConfig(key, value)
      }
    }

  final def echo(
      message: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[String]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.echo(message)
      }
    )

  // Set Operations

  /**
    * Add the specified members to the set value stored at key. (VARIADIC: >= 2.4)
    */
  final def sadd(key: Any, value: Any, values: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.sadd(key, value, values: _*)
      }
    }

  /**
    * Remove the specified members from the set value stored at key. (VARIADIC: >= 2.4)
    */
  final def srem(key: Any, value: Any, values: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.srem(key, value, values: _*)
      }
    }

  /**
    * Remove and return (pop) a random element from the Set value at key.
    */
  final def spop[A](
      key: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.spop(key)
      }
    }

  /**
    * Remove and return multiple random elements (pop) from the Set value at key since (3.2).
    */
  final def spop[A](key: Any, count: Int)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.spop(key, count)
      }
    }

  /**
    * Move the specified member from one Set to another atomically.
    */
  final def smove(sourceKey: Any, destKey: Any, value: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format
  ): F[Resp[Boolean]] =
    forKeys(sourceKey.toString, destKey.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.smove(sourceKey, destKey, value)
      }
    }

  /**
    * Return the number of elements (the cardinality) of the Set at key.
    */
  final def scard(
      key: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.scard(key)
      }
    }

  /**
    * Test if the specified value is a member of the Set at key.
    */
  final def sismember(
      key: Any,
      value: Any
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Boolean]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.sismember(key, value)
      }
    }

  /**
    * Return the intersection between the Sets stored at key1, key2, ..., keyN.
    */
  final def sinter[A](key: Any, keys: Any*)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Set[Option[A]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sinter[A](key, keys)
      }
    }

  final def sinterstore(
      key: Any,
      keys: Any*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sinterstore(key, keys)
      }
    }

  /**
    * Return the union between the Sets stored at key1, key2, ..., keyN.
    */
  final def sunion[A](key: Any, keys: Any*)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Set[Option[A]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sunion[A](key, keys)
      }
    }

  /**
    * Compute the union between the Sets stored at key1, key2, ..., keyN,
    * and store the resulting Set at dstkey.
    * SUNIONSTORE returns the size of the union, unlike what the documentation says
    * refer http://code.google.com/p/redis/issues/detail?id=121
    */
  final def sunionstore(
      key: Any,
      keys: Any*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sunionstore(key, keys)
      }
    }

  /**
    * Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
    */
  final def sdiff[A](key: Any, keys: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sdiff[A](key, keys)
      }
    }

  /**
    * Compute the difference between the Set key1 and all the Sets key2, ..., keyN,
    * and store the resulting Set at dstkey.
    */
  final def sdiffstore(
      key: Any,
      keys: Any*
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])], format: Format): F[Resp[Long]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.sdiffstore(key, keys)
      }
    }

  /**
    * Return all the members of the Set value at key.
    */
  final def smembers[A](key: Any)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.smembers(key)
      }
    }

  /**
    * Return a random element from a Set
    */
  final def srandmember[A](
      key: Any
  )(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.srandmember[A](key)
      }
    }

  /**
    * Return multiple random elements from a Set (since 2.6)
    */
  final def srandmember[A](key: Any, count: Int)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Set[Option[A]]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.srandmember[A](key, count)
      }
    }

  /**
    * Incrementally iterate Set elements (since 2.8)
    */
  final def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A],
      pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Option[(Int, List[Option[A]])]]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.sscan[A](key, cursor, pattern, count)
      }
    }

  // Hyperloglog operations

  /**
    * Add a value to the hyperloglog (>= 2.8.9)
    */
  final def pfadd(key: Any, value: Any, values: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Long]] =
    forKey(key.toString) { node =>
      node.managedClient(pool, node.uri).use {
        _.pfadd(key, value, values: _*)
      }
    }

  /**
    * Get the estimated cardinality from one or more keys (>= 2.8.9)
    */
  final def pfcount(key: Any, keys: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Long]] =
    forKeys(key.toString, keys.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.pfcount(keys)
      }
    }

  /**
    * Merge existing keys (>= 2.8.9)
    */
  final def pfmerge(destination: Any, sources: Any*)(
      implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]
  ): F[Resp[Boolean]] =
    forKeys(destination.toString, sources.map(_.toString): _*) { node =>
      node.managedClient(pool, node.uri).use {
        _.pfmerge(destination, sources: _*)
      }
    }

  def discard(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Boolean]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.discard
      }
    )

  def multi(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Boolean]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.multi
      }
    )

  def exec(
      hs: Seq[() => Any]
  )(implicit pool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])]): F[Resp[Option[List[Any]]]] =
    onANode(node =>
      node.managedClient(pool, node.uri).use {
        _.exec(hs)
      }
    )
}

class NotAllowedInClusterError(message: String) extends RuntimeException(message)
