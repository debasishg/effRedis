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

trait StringOperations[F[+_]] extends StringApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  def set(
      key: Any,
      value: Any,
      whenSet: SetBehaviour = Always,
      expire: Duration = null,
      keepTTL: Boolean = false
  )(implicit format: Format): F[Resp[String]] = {

    val expireCmd = if (expire != null) {
      List("PX", expire.toMillis.toString)
    } else {
      List.empty
    }
    val cmd = List(key, value) ::: expireCmd ::: whenSet.command ::: (if (keepTTL) List("KEEPTTL") else List.empty)
    send("SET", cmd)(asSimpleString)
  }

  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    send("GET", List(key))(asBulkString)

  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    send("GETSET", List(key, value))(asBulkString)

  override def setnx(key: Any, value: Any)(implicit format: Format): F[Resp[Long]] =
    send("SETNX", List(key, value))(asInteger)

  override def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): F[Resp[String]] =
    send("SETEX", List(key, expiry, value))(asSimpleString)

  override def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): F[Resp[String]] =
    send("PSETEX", List(key, expiryInMillis, value))(asSimpleString)

  override def incr(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("INCR", List(key))(asInteger)

  override def incrby(key: Any, increment: Long)(implicit format: Format): F[Resp[Long]] =
    send("INCRBY", List(key, increment))(asInteger)

  override def incrbyfloat(key: Any, increment: Float)(implicit format: Format): F[Resp[Option[Float]]] =
    send("INCRBYFLOAT", List(key, increment))(asBulkString.map(_.toFloat))

  override def decr(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("DECR", List(key))(asInteger)

  override def decrby(key: Any, increment: Long)(implicit format: Format): F[Resp[Long]] =
    send("DECRBY", List(key, increment))(asInteger)

  override def mget[A](
      key: Any,
      keys: Any*
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[A]]] =
    send("MGET", key :: keys.toList)(asFlatList)

  override def mset(kvs: (Any, Any)*)(implicit format: Format): F[Resp[String]] =
    send("MSET", kvs.foldRight(List[Any]()) { case ((k, v), l) => k :: v :: l })(asSimpleString)

  override def msetnx(kvs: (Any, Any)*)(implicit format: Format): F[Resp[Long]] =
    send("MSETNX", kvs.foldRight(List[Any]()) { case ((k, v), l) => k :: v :: l })(asInteger)

  override def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): F[Resp[Long]] =
    send("SETRANGE", List(key, offset, value))(asInteger)

  override def getrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    send("GETRANGE", List(key, start, end))(asBulkString)

  override def strlen(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("STRLEN", List(key))(asInteger)

  override def append(key: Any, value: Any)(implicit format: Format): F[Resp[Long]] =
    send("APPEND", List(key, value))(asInteger)

  override def getbit(key: Any, offset: Int)(implicit format: Format): F[Resp[Long]] =
    send("GETBIT", List(key, offset))(asInteger)

  override def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): F[Resp[Long]] =
    send("SETBIT", List(key, offset, value))(asInteger)

  override def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("BITOP", op :: destKey :: srcKeys.toList)(asInteger)

  override def bitcount(key: Any, range: Option[(Int, Int)] = None)(
      implicit format: Format
  ): F[Resp[Long]] =
    send("BITCOUNT", List[Any](key) ++ (range.map { r =>
          List[Any](r._1, r._2)
        } getOrElse List[Any]()))(asInteger)

}
