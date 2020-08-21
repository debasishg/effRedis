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

import cats.effect._
import effredis.{ Log, Resp }
import effredis.codecs.{ Format, Parse }

class BaseOps[F[+_]: Concurrent: ContextShift: Log] extends RedisClusterOps[F] { self: RedisClusterClient[F] =>

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
    forKey(key.toString)(_.client.sort[A](key, limit, desc, alpha, by, get))

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
    forKey(key.toString)(_.client.sortNStore[A](key, limit, desc, alpha, by, get, storeAt))

  /**
    * returns all the keys matching the glob-style pattern.
    */
  def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]]

  /**
    * returns the current server time as a two items lists:
    * a Unix timestamp and the amount of microseconds already elapsed in the current second.
    */
  def time[A](implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]]

  /**
    * returns a randomly selected key from the currently selected DB.
    */
  def randomkey[A](implicit parse: Parse[A]): F[Resp[Option[A]]]

  /**
    * atomically renames the key oldkey to newkey.
    */
  def rename(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]]

  /**
    * rename oldkey into newkey but fails if the destination key newkey already exists.
    */
  def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]]

  /**
    * returns the size of the db.
    */
  def dbsize: F[Resp[Option[Long]]]

  /**
    * test if the specified key exists.
    */
  def exists(key: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.exists(key))

  /**
    * deletes the specified keys.
    */
  def del(key: Any, keys: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*)(_.client.del(key, keys: _*))

  /**
    * returns the type of the value stored at key in form of a string.
    */
  def getType(key: Any)(implicit format: Format): F[Resp[Option[String]]] =
    forKey(key.toString)(_.client.getType(key))

  /**
    * sets the expire time (in sec.) for the specified key.
    */
  def expire(key: Any, ttl: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.expire(key, ttl))

  /**
    * sets the expire time (in milli sec.) for the specified key.
    */
  def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.expire(key, ttlInMillis))

  /**
    * sets the expire time for the specified key.
    */
  def expireat(key: Any, timestamp: Long)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.expireat(key, timestamp))

  /**
    * sets the expire timestamp in millis for the specified key.
    */
  def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.expireat(key, timestampInMillis))

  /**
    * returns the remaining time to live of a key that has a timeout
    */
  def ttl(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.ttl(key))

  /**
    * returns the remaining time to live of a key that has a timeout in millis
    */
  def pttl(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.pttl(key))

  /**
    * selects the DB to connect, defaults to 0 (zero).
    */
  def select(index: Int): F[Resp[Boolean]] =
    F.raiseError(new NotAllowedInClusterError(s"SELECT $index not allowed in cluster mode"))

  /**
    * removes all the DB data.
    */
  def flushdb: F[Resp[Boolean]] =
    onANode(_.client.flushdb)

  /**
    * removes data from all the DB's.
    */
  def flushall: F[Resp[Boolean]] =
    onANode(_.client.flushdb)

  /**
    * Move the specified key from the currently selected DB to the specified destination DB.
    */
  def move(key: Any, db: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.move(key, db))

  /**
    * exits the server.
    */
  def quit: F[Resp[Boolean]] =
    onANode(_.client.quit)

  /**
    * auths with the server.
    */
  def auth(secret: Any)(implicit format: Format): F[Resp[Boolean]] =
    onANode(_.client.auth(secret))

  /**
    * Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
    * to persistent (a key that will never expire as no timeout is associated).
    */
  def persist(key: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.persist(key))

  /**
    * Incrementally iterate the keys space (since 2.8)
    */
  def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)
  : F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    F.raiseError(new NotAllowedInClusterError(s"SCAN $cursor $pattern $count not allowed in cluster mode"))

  /**
    * ping
    */
  def ping: F[Resp[Option[String]]] =
    onANode(_.client.ping)

  protected val pong: Option[String] = Some("PONG") 

  /**
    * Marks the given keys to be watched for conditional execution of a transaction.
    */
  def watch(key: Any, keys: Any*)(implicit format: Format): F[Resp[Boolean]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*)(_.client.watch(key, keys: _*))

  /**
    * Flushes all the previously watched keys for a transaction
    */
  def unwatch(): F[Resp[Boolean]] =
    onAllNodes(_.client.unwatch)

  /**
    * CONFIG GET
    */
  def getConfig(key: Any = "*")(implicit format: Format): F[Resp[Option[Map[String, Option[String]]]]] =
    forKey(key.toString)(_.client.getConfig(key))

  /**
    * CONFIG SET
    */
  def setConfig(key: Any, value: Any)(implicit format: Format): F[Resp[Option[String]]] =
    forKey(key.toString)(_.client.setConfig(key, value))

  def echo(message: Any)(implicit format: Format): F[Resp[Option[String]]] =
    onANode(_.client.echo(message))
}
