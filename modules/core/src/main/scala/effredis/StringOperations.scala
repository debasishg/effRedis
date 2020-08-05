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

import cats.effect._

import codecs._
import algebra.StringApi
import StringApi._

import scala.concurrent.duration.Duration

trait StringOperations[F[+_]] extends StringApi[F] { self: Redis[F] =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  def set(
      key: Any,
      value: Any,
      whenSet: SetBehaviour = Always,
      expire: Duration = null
  )(implicit format: Format): F[RedisResponse[Boolean]] = {

    val expireCmd = if (expire != null) {
      List("PX", expire.toMillis.toString)
    } else {
      List.empty
    }
    val cmd = List(key, value) ::: expireCmd ::: whenSet.command
    send("SET", cmd)(asBoolean)
  }

  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("GET", List(key))(asBulk)

  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("GETSET", List(key, value))(asBulk)

  override def setnx(key: Any, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("SETNX", List(key, value))(asBoolean)

  override def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("SETEX", List(key, expiry, value))(asBoolean)

  override def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("PSETEX", List(key, expiryInMillis, value))(asBoolean)

  override def incr(key: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("INCR", List(key))(asLong)

  override def incrby(key: Any, increment: Long)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("INCRBY", List(key, increment))(asLong)

  override def incrbyfloat(key: Any, increment: Float)(implicit format: Format): F[RedisResponse[Option[Float]]] =
    send("INCRBYFLOAT", List(key, increment))(asBulk.map(_.toFloat))

  override def decr(key: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("DECR", List(key))(asLong)

  override def decrby(key: Any, increment: Long)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("DECRBY", List(key, increment))(asLong)

  override def mget[A](
      key: Any,
      keys: Any*
  )(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[List[Option[A]]]]] =
    send("MGET", key :: keys.toList)(asList)

  override def mset(kvs: (Any, Any)*)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("MSET", kvs.foldRight(List[Any]()) { case ((k, v), l) => k :: v :: l })(asBoolean)

  override def msetnx(kvs: (Any, Any)*)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("MSETNX", kvs.foldRight(List[Any]()) { case ((k, v), l) => k :: v :: l })(asBoolean)

  override def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("SETRANGE", List(key, offset, value))(asLong)

  override def getrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[RedisResponse[Option[A]]] =
    send("GETRANGE", List(key, start, end))(asBulk)

  override def strlen(key: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("STRLEN", List(key))(asLong)

  override def append(key: Any, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("APPEND", List(key, value))(asLong)

  override def getbit(key: Any, offset: Int)(implicit format: Format): F[RedisResponse[Option[Int]]] =
    send("GETBIT", List(key, offset))(asInt)

  override def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): F[RedisResponse[Option[Int]]] =
    send("SETBIT", List(key, offset, value))(asInt)

  override def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): F[RedisResponse[Option[Int]]] =
    send("BITOP", op :: destKey :: srcKeys.toList)(asInt)

  override def bitcount(key: Any, range: Option[(Int, Int)] = None)(
      implicit format: Format
  ): F[RedisResponse[Option[Int]]] =
    send("BITCOUNT", List[Any](key) ++ (range.map { r =>
          List[Any](r._1, r._2)
        } getOrElse List[Any]()))(asInt)

}
