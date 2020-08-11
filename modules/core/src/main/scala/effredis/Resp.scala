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

// generates a value A
case class Value[A](value: A) extends Resp[A]

// queued for transaction
case object Queued extends Resp[Nothing]

// bufferred for pipeline
case object Buffered extends Resp[Nothing]

// error
case class Error(cause: String) extends Resp[Nothing]

// transaction discarded
case class TxnDiscarded(contents: Vector[(String, () => Any)]) extends Resp[Nothing]
