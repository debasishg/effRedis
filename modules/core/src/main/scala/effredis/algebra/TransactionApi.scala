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

trait TransactionApi[F[+_]] {

  /**
    * discard transaction
    */
  def discard: F[Resp[Boolean]]

  /**
    * multi to start the transaction
    */
  def multi: F[Resp[Boolean]]

  /**
    * exec to complete the transaction
    */
  def exec(hs: Seq[() => Any]): F[Resp[Option[List[Any]]]]
}