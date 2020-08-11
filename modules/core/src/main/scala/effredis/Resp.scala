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

sealed trait Resp[+A]
case class Value[A](value: A) extends Resp[A]
case object RedisQueued extends Resp[Nothing]
case object Bufferred extends Resp[Nothing]
case class RedisError(cause: String) extends Resp[Nothing]

sealed trait TransactionState
case class TxnDiscarded(contents: Vector[(String, () => Any)]) extends TransactionState
case class TxnError(message: String) extends TransactionState
