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

import java.net.URI
import javax.net.ssl.SSLContext

import shapeless.HList

import cats.data.NonEmptyList
import cats.effect._
import cats.syntax.all._

object RedisClient {
  sealed trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  sealed trait Mode
  case object SINGLE extends Mode
  case object TRANSACT extends Mode
  case object PIPE extends Mode

  private[effredis] def extractDatabaseNumber(connectionUri: java.net.URI): Int =
    Option(connectionUri.getPath)
      .map(path =>
        if (path.isEmpty) 0
        else Integer.parseInt(path.tail)
      )
      .getOrElse(0)

  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log, M <: Mode](
      uri: URI,
      blocker: Blocker,
      mode: M
  ): Resource[F, RedisClient[F, M]] = {

    val acquire: F[RedisClient[F, M]] = {
      F.debug(s"Acquiring client for uri $uri $blocker") *>
        blocker.blockOn {
          F.delay(new RedisClient[F, M](uri, mode))
        }
    }
    val release: RedisClient[F, M] => F[Unit] = { c =>
      F.debug(s"Releasing client for uri $uri") *> {
        F.delay(c.close())
      }
    }

    Resource.make(acquire)(release)
  }

  def make[F[+_]: ContextShift: Concurrent: Log, M <: Mode](
      uri: URI,
      mode: M = SINGLE
  ): Resource[F, RedisClient[F, M]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker, mode)
    } yield client

  /**
    * Make a single normal connection
    *
    * @param uri the client URI
    * @return the single client
    */
  def single[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClient[F, SINGLE.type]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker, SINGLE)
    } yield client

  /**
    * Creates a single connection out of the first working one from the
    * list supplied. This is used to find out one of the seed connections
    * in a Redis Cluster
    *
    * @param uris the list of uris from which at least one needs to work
    * @return a resource for a working client in F
    */
  def single[F[+_]: ContextShift: Concurrent: Log](
      uris: NonEmptyList[URI]
  ): F[Resource[F, RedisClient[F, SINGLE.type]]] =
    firstWorking(uris.toList).flatMap {
      case Some(uri) => single(uri).pure[F]
      case _ =>
        F.raiseError(new IllegalArgumentException(s"None of the supplied URIs $uris could connect to the cluster"))
    }

  private def clientPings[F[+_]: ContextShift: Concurrent: Log](r: RedisClient[F, SINGLE.type]): F[Boolean] =
    r.ping.flatMap {
      case Value(_) => true.pure[F]
      case _        => false.pure[F]
    }

  private def firstWorking[F[+_]: ContextShift: Concurrent: Log](uris: List[URI]): F[Option[URI]] = {
    def firstWorkingRec(uris: List[URI], workingURI: Option[URI]): F[Option[URI]] =
      if (uris.isEmpty) None.pure[F]
      else if (workingURI.isDefined) workingURI.pure[F]
      else {
        single(uris.head).use { client =>
          F.info(s"Trying ${uris.head} ..") *>
            clientPings(client).flatMap {
              case true => {
                F.info(s"Worked ${uris.head}!") *>
                  Some(uris.head).pure[F]
              }
              case false => firstWorkingRec(uris.tail, None)
            }
        }
      }
    firstWorkingRec(uris, None)
  }

  /**
    * Make a connection for transaction
    *
    * @param uri the client URI
    * @return the transaction client
    */
  def transact[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClient[F, TRANSACT.type]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker, TRANSACT)
    } yield client

  /**
    * Make a connection for pipelines
    *
    * @param uri the client URI
    * @return the pipeline client
    */
  def pipe[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClient[F, PIPE.type]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker, PIPE)
    } yield client

  /**
    * API for pipeline instructions
    *
    * @param client the pipeline client
    * @param f the pipeline of functions
    * @return response from server
    */
  def pipeline[F[+_]: Concurrent: ContextShift: Log, A](
      client: RedisClient[F, PIPE.type]
  )(f: RedisClient[F, PIPE.type] => F[A]): F[Resp[Option[List[Any]]]] =
    try {
      f(client).flatMap { _ =>
        client
          .send(client.commandBuffer.toString, true)(Some(client.handlers.map(_._2).map(_()).toList))
      }
    } catch {
      case e: Exception => Error(e.getMessage).pure[F]
    }

  /**
    * API for pipeline instructions. Allows HList to be used for setting up
    * the pipeline
    *
    * @param client the pipeline client
    * @param f the pipeline of functions
    * @return response from server
    */
  def hpipeline[F[+_]: Concurrent: ContextShift: Log, In <: HList](
      client: RedisClient[F, PIPE.type]
  )(commands: () => F[In]): F[Resp[Option[List[Any]]]] =
    try {
      commands().flatMap { _ =>
        client
          .send(client.commandBuffer.toString, true)(Option(client.handlers.map(_._2).map(_()).toList))
          .flatTap { r =>
            client.handlers = Vector.empty
            client.commandBuffer = new StringBuffer
            F.delay(r)
          }
      }
    } catch {
      case e: Exception => F.delay(Error(e.getMessage))
    }
}

class RedisClient[F[+_]: Concurrent: ContextShift: Log, M <: RedisClient.Mode] private (
    override val host: String,
    override val port: Int,
    override val database: Int = 0,
    override val secret: Option[Any] = None,
    override val timeout: Int = 0,
    override val sslContext: Option[SSLContext] = None,
    val mode: M
) extends RedisCommand[F, M](mode) {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]

  def this(m: M) = this("localhost", 6379, mode = m)
  def this(connectionUri: java.net.URI, m: M) = this(
    host = connectionUri.getHost,
    port = connectionUri.getPort,
    database = RedisClient.extractDatabaseNumber(connectionUri),
    secret = Option(connectionUri.getUserInfo)
      .flatMap(_.split(':') match {
        case Array(_, password, _*) => Some(password)
        case _                      => None
      }),
    mode = m
  )
  override def toString: String = host + ":" + String.valueOf(port) + "/" + database
  override def close(): Unit    = { disconnect; () }
}
