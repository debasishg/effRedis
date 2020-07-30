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

import cats.effect.{ Blocker, Concurrent, ContextShift }
import algebra.BaseApi
import serialization._

// class BaseOperations[F[_]: Concurrent: ContextShift](implicit blocker: Blocker) extends BaseApi[F] with Redis {
trait BaseOperations[F[_]] extends BaseApi[F] with Redis { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def sort[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil
  )(implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]] = {

    val commands: List[Any] = makeSortArgs(key, limit, desc, alpha, by, get)
    send("SORT", commands)(asList)
  }

  private def makeSortArgs(
      key: String,
      limit: Option[(Int, Int)],
      desc: Boolean,
      alpha: Boolean,
      by: Option[String],
      get: List[String]
  ): List[Any] =
    List(
      List(key),
      limit.map(l => List("LIMIT", l._1, l._2)).getOrElse(Nil),
      (if (desc) List("DESC") else Nil),
      (if (alpha) List("ALPHA") else Nil),
      by.map(b => List("BY", b)).getOrElse(Nil),
      get.flatMap(g => List("GET", g))
    ).flatten

  override def sortNStore[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil,
      storeAt: String
  )(implicit format: Format, parse: Parse[A]): F[Option[Long]] = {

    val commands = makeSortArgs(key, limit, desc, alpha, by, get) ::: List("STORE", storeAt)
    send("SORT", commands)(asLong)
  }

  override def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]] =
    send("KEYS", List(pattern))(asList)

  override def time[A](implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]] =
    send("TIME")(asList)

  @deprecated("use randomkey", "2.8")
  def randkey[A](implicit parse: Parse[A]): F[Option[A]] =
    send("RANDOMKEY")(asBulk)

  override def randomkey[A](implicit parse: Parse[A]): F[Option[A]] =
    send("RANDOMKEY")(asBulk)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): F[Boolean] =
    send("RENAME", List(oldkey, newkey))(asBoolean)

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): F[Boolean] =
    send("RENAMENX", List(oldkey, newkey))(asBoolean)

  override def dbsize: F[Option[Long]] =
    send("DBSIZE")(asLong)

  override def exists(key: Any)(implicit format: Format): F[Boolean] =
    send("EXISTS", List(key))(asBoolean)

  override def del(key: Any, keys: Any*)(implicit format: Format): F[Option[Long]] =
    send("DEL", key :: keys.toList)(asLong)

  override def getType(key: Any)(implicit format: Format): F[Option[String]] =
    send("TYPE", List(key))(asString)

  override def expire(key: Any, ttl: Int)(implicit format: Format): F[Boolean] =
    send("EXPIRE", List(key, ttl))(asBoolean)

  override def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): F[Boolean] =
    send("PEXPIRE", List(key, ttlInMillis))(asBoolean)

  override def expireat(key: Any, timestamp: Long)(implicit format: Format): F[Boolean] =
    send("EXPIREAT", List(key, timestamp))(asBoolean)

  override def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): F[Boolean] =
    send("PEXPIREAT", List(key, timestampInMillis))(asBoolean)

  override def ttl(key: Any)(implicit format: Format): F[Option[Long]] =
    send("TTL", List(key))(asLong)

  override def pttl(key: Any)(implicit format: Format): F[Option[Long]] =
    send("PTTL", List(key))(asLong)

  override def select(index: Int): F[Boolean] =
    send("SELECT", List(index))(if (asBoolean) {
      db = index
      true
    } else {
      false
    })

  override def flushdb: F[Boolean] =
    send("FLUSHDB")(asBoolean)

  override def flushall: F[Boolean] =
    send("FLUSHALL")(asBoolean)

  override def move(key: Any, db: Int)(implicit format: Format): F[Boolean] =
    send("MOVE", List(key, db))(asBoolean)

  override def quit: F[Boolean] =
    send("QUIT")(disconnect)

  override def auth(secret: Any)(implicit format: Format): F[Boolean] =
    send("AUTH", List(secret))(asBoolean)

  override def persist(key: Any)(implicit format: Format): F[Boolean] =
    send("PERSIST", List(key))(asBoolean)

  override def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[(Option[Int], Option[List[Option[A]]])]] =
    send(
      "SCAN",
      cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)

  override def ping: F[Option[String]] =
    send("PING")(asString)

  override def watch(key: Any, keys: Any*)(implicit format: Format): F[Boolean] =
    send("WATCH", key :: keys.toList)(asBoolean)

  override def unwatch(): F[Boolean] =
    send("UNWATCH")(asBoolean)

  override def getConfig(key: Any = "*")(implicit format: Format): F[Option[Map[String, Option[String]]]] = ???
//   override def getConfig(key: Any = "*")(implicit format: Format): F[Option[Map[String, Option[String]]]] =
//     send("CONFIG", List("GET", key))(asList).map { ls =>
//       ls.grouped(2).collect { case Some(k) :: v :: Nil => k -> v }.toMap
//     }

  override def setConfig(key: Any, value: Any)(implicit format: Format): F[Option[String]] =
    send("CONFIG", List("SET", key, value))(asString)
}
