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

trait EvalOperations[F[_]] extends EvalApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def evalMultiBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[List[Option[A]]]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asList[A])

  override def evalBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[A]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asBulk)

  override def evalInt(luaCode: String, keys: List[Any], args: List[Any]): F[Option[Int]] =
    send("EVAL", argsForEval(luaCode, keys, args))(asInt)

  override def evalMultiSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[List[Option[A]]]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asList[A])

  override def evalSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[A]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asAny.asInstanceOf[Option[A]])

  override def evalSHABulk[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Option[A]] =
    send("EVALSHA", argsForEval(shahash, keys, args))(asBulk)

  override def scriptLoad(luaCode: String): F[Option[String]] =
    send("SCRIPT", List("LOAD", luaCode))(asBulk)

  override def scriptExists(shahash: String): F[Option[Int]] = {
    val fa = send("SCRIPT", List("EXISTS", shahash))(asList[String])
    val ev = implicitly[Concurrent[F]]
    ev.fmap(fa) {
      case Some(list) => {
        if (list.size > 0 && list(0).isDefined) {
          Some(list(0).get.toInt)
        } else {
          None
        }
      }
      case None => None
    }
  }

  override def scriptFlush: F[Option[String]] =
    send("SCRIPT", List("FLUSH"))(asString)

  private def argsForEval(luaCode: String, keys: List[Any], args: List[Any]): List[Any] =
    luaCode :: keys.length :: keys ::: args
}
