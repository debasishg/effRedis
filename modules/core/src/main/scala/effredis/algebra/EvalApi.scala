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
package algebra

import codecs.{ Format, Parse }

trait EvalApi[F[+_]] {

  /**
    * evaluates lua code on the server.
    */
  def evalMultiBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]]

  def evalBulk[A](luaCode: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]]

  def evalInt(luaCode: String, keys: List[Any], args: List[Any]): F[Resp[Long]]

  def evalMultiSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]]

  def evalSHA[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]]

  def evalSHABulk[A](shahash: String, keys: List[Any], args: List[Any])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]]

  def scriptLoad(luaCode: String): F[Resp[Option[String]]]

  def scriptExists(shas: String*): F[Resp[List[Option[Int]]]]

  def scriptFlush: F[Resp[String]]
}
