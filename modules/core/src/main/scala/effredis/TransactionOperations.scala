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
import algebra.TransactionApi

trait TransactionOperations[F[+_]] extends TransactionApi[F] { self: Redis[F, _] =>
  implicit def blocker: Blocker

  override def discard: F[Resp[Boolean]] =
    send("DISCARD")(asBoolean)

  override def multi: F[Resp[Option[String]]] =
    send("MULTI")(asString)

  override def exec(hs: Seq[() => Any]): F[Resp[Option[List[Any]]]] =
    send("EXEC")(asExec(hs))
}
