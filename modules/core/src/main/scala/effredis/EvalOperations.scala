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
import algebra.EvalApi
import codecs._

trait EvalOperations[F[+_]] extends EvalApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def evalMultiBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asFlatList[A])

  override def evalBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asBulkString)

  override def evalInt(luaCode: String, keys: List[Any], args: List[Any]): F[Resp[Long]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asInteger)

  override def evalMultiSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asFlatList[A])

  override def evalSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asBulkString)

  override def evalSHABulk[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asBulkString)

  override def scriptLoad(luaCode: String): F[Resp[Option[String]]] =
    send("SCRIPT", List("LOAD", luaCode))(asBulkString)

  override def scriptExists(shas: String*): F[Resp[List[Option[Int]]]] =
    send("SCRIPT", List("EXISTS") ::: shas.toList)(asFlatList[Int](Parse.Implicits.parseInt))

  override def scriptFlush: F[Resp[String]] =
    send("SCRIPT", List("FLUSH"))(asSimpleString)

  private def argsForEval(luaCode: String, keys: List[Any], args: List[Any]): List[Any] =
    luaCode :: keys.length :: keys ::: args
}
