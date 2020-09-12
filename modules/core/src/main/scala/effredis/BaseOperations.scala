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

import cats.effect.{ Concurrent, ContextShift }
import algebra.BaseApi
import codecs._

trait BaseOperations[F[+_]] extends BaseApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def sort[A](
      key: String,
      limit: Option[(Int, Int)] = None,
      desc: Boolean = false,
      alpha: Boolean = false,
      by: Option[String] = None,
      get: List[String] = Nil
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]] = {

    val commands: List[Any] = makeSortArgs(key, limit, desc, alpha, by, get)
    send("SORT", commands)(asFlatList)
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
  )(implicit format: Format, parse: Parse[A]): F[Resp[Long]] = {

    val commands = makeSortArgs(key, limit, desc, alpha, by, get) ::: List("STORE", storeAt)
    send("SORT", commands)(asInteger)
  }

  override def keys[A](
      pattern: Any = "*"
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]] =
    send("KEYS", List(pattern))(asFlatList)

  override def time: F[Resp[List[Option[Long]]]] =
    send("TIME")(asFlatList(Parse.Implicits.parseLong))

  @deprecated("use randomkey", "2.8")
  def randkey[A](implicit parse: Parse[A]): F[Resp[Option[A]]] =
    send("RANDOMKEY")(asBulkString)

  override def randomkey[A](implicit parse: Parse[A]): F[Resp[Option[A]]] =
    send("RANDOMKEY")(asBulkString)

  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("RENAME", List(oldkey, newkey))(asBoolean)

  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("RENAMENX", List(oldkey, newkey))(if (asInteger == 1) true else false)

  override def dbsize: F[Resp[Long]] =
    send("DBSIZE")(asInteger)

  override def exists(key: Any, keys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("EXISTS", List(key) ::: keys.toList)(asInteger)

  override def del(key: Any, keys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("DEL", List(key) ::: keys.toList)(asInteger)

  override def getType(key: Any)(implicit format: Format): F[Resp[String]] =
    send("TYPE", List(key))(asSimpleString)

  override def expire(key: Any, ttl: Int)(implicit format: Format): F[Resp[Boolean]] =
    send("EXPIRE", List(key, ttl))(if (asInteger == 1) true else false)

  override def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): F[Resp[Boolean]] =
    send("PEXPIRE", List(key, ttlInMillis))(if (asInteger == 1) true else false)

  override def expireat(key: Any, timestamp: Long)(implicit format: Format): F[Resp[Boolean]] =
    send("EXPIREAT", List(key, timestamp))(if (asInteger == 1) true else false)

  override def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): F[Resp[Boolean]] =
    send("PEXPIREAT", List(key, timestampInMillis))(if (asInteger == 1) true else false)

  override def ttl(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("TTL", List(key))(asInteger)

  override def pttl(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("PTTL", List(key))(asInteger)

  override def select(index: Int): F[Resp[Boolean]] =
    send("SELECT", List(index))(if (asSimpleString == "OK") {
      db = index
      true
    } else {
      false
    })

  override def flushdb: F[Resp[String]] =
    send("FLUSHDB")(asSimpleString)

  override def flushall: F[Resp[String]] =
    send("FLUSHALL")(asSimpleString)

  override def move(key: Any, db: Int)(implicit format: Format): F[Resp[String]] =
    send("MOVE", List(key, db))(asSimpleString)

  override def quit: F[Resp[Boolean]] =
    send("QUIT")(disconnect)

  override def auth(secret: Any)(implicit format: Format): F[Resp[String]] =
    send("AUTH", List(secret))(asSimpleString)

  override def persist(key: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("PERSIST", List(key))(if (asInteger == 1) true else false)

  override def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[Option[A]])]]] =
    send(
      "SCAN",
      cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)

  override def ping: F[Resp[String]] =
    send("PING")(asSimpleString)

  override def watch(key: Any, keys: Any*)(implicit format: Format): F[Resp[String]] =
    send("WATCH", key :: keys.toList)(asSimpleString)

  override def unwatch(): F[Resp[String]] =
    send("UNWATCH")(asSimpleString)

  override def getConfig(key: Any = "*")(
      implicit format: Format
  ): F[Resp[Option[Map[String, Option[String]]]]] = ??? // {
//     val fa = send("CONFIG", List("GET", key))(asFlatList)
//     val ev = implicitly[Concurrent[F]]
//     ev.fmap(fa) { ls =>
//       ls.grouped(2).collect { case Some(k) :: v :: Nil => k -> v }.toMap
//     }
//   }

  override def setConfig(key: Any, value: Any)(implicit format: Format): F[Resp[String]] =
    send("CONFIG", List("SET", key, value))(asSimpleString)

  override def echo(message: Any)(implicit format: Format): F[Resp[String]] =
    send("ECHO", List(message))(asSimpleString)
}
