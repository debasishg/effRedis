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

trait NodeApi[F[+_]] {

  /**
    * save the DB on disk now.
    */
  def save: F[Resp[Boolean]]

  /**
    * save the DB in the background.
    */
  def bgsave: F[Resp[Boolean]]

  /**
    * return the UNIX TIME of the last DB SAVE executed with success.
    */
  def lastsave: F[Resp[Option[Long]]]

  /**
    * Stop all the clients, save the DB, then quit the server.
    */
  def shutdown: F[Resp[Boolean]]

  def bgrewriteaof: F[Resp[Boolean]]

  /**
    * The info command returns different information and statistics about the server.
    */
  def info: F[Resp[Option[String]]]

  /**
    * is a debugging command that outputs the whole sequence of commands received by the Redis server.
    */
  def monitor: F[Resp[Boolean]]

  /**
    * The SLAVEOF command can change the replication settings of a slave on the fly.
    */
  def slaveof(options: Any): F[Resp[Boolean]]
}
