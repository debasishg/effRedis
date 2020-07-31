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

import java.net.{ SocketException, URI }
import javax.net.ssl.SSLContext

import serialization.Format

import cats.effect._
import cats.implicits._

object RedisClient {
  sealed trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder

  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  private def extractDatabaseNumber(connectionUri: java.net.URI): Int =
    Option(connectionUri.getPath)
      .map(path =>
        if (path.isEmpty) 0
        else Integer.parseInt(path.tail)
      )
      .getOrElse(0)

  private[effredis] def acquireAndRelease[F[_]: Concurrent: ContextShift](
      uri: URI,
      blocker: Blocker
  ): Resource[F, RedisClient[F]] = {

    val acquire: F[RedisClient[F]]         = blocker.blockOn((new RedisClient[F](uri, blocker)).pure[F])
    val release: RedisClient[F] => F[Unit] = c => { c.disconnect; ().pure[F] }
    Resource.make(acquire)(release)
  }

  def makeWithURI[F[_]: ContextShift: Concurrent](
      uri: URI
  ): Resource[F, RedisClient[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker)
    } yield client
}

trait Redis extends RedisIO with Protocol {

  def send[F[_]: Concurrent: ContextShift, A](command: String, args: Seq[Any])(
      result: => A
  )(implicit format: Format, blocker: Blocker): F[A] = blocker.blockOn {

    val ev = implicitly[Concurrent[F]]
    try {
      write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
      result.pure[F]
    } catch {
      case e: RedisConnectionException =>
        if (disconnect) send(command, args)(result)
        else ev.raiseError(e)
      case e: SocketException =>
        if (disconnect) send(command, args)(result)
        else ev.raiseError(e)
    }
  }

  def send[F[_]: Concurrent: ContextShift, A](command: String)(result: => A)(implicit blocker: Blocker): F[A] =
    blocker.blockOn {
      val ev = implicitly[Concurrent[F]]
      try {
        write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
        result.pure[F]
      } catch {
        case e: RedisConnectionException =>
          if (disconnect) send(command)(result)
          else ev.raiseError(e)
        case e: SocketException =>
          if (disconnect) send(command)(result)
          else ev.raiseError(e)
      }
    }

  def cmd(args: Seq[Array[Byte]]): Array[Byte] = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[Product2[Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList

}

trait RedisCommand[F[_]]
    extends Redis
    with StringOperations[F]
    with BaseOperations[F]
    with ListOperations[F]
    with SetOperations[F]
    with HashOperations[F]
    with SortedSetOperations[F]
    with NodeOperations[F]
//     with GeoOperations
//     with EvalOperations
//     with PubOperations
//     with HyperLogLogOperations
    with AutoCloseable {

  val database: Int       = 0
  val secret: Option[Any] = None

  override def onConnect: Unit = {
    secret.foreach(s => auth(s))
    selectDatabase()
  }

  private def selectDatabase(): Unit = {
    val _ = if (database != 0) select(database)
    ()
  }
}

class RedisClient[F[_]: Concurrent: ContextShift](
    override val host: String,
    override val port: Int,
    override val database: Int = 0,
    override val secret: Option[Any] = None,
    override val timeout: Int = 0,
    override val sslContext: Option[SSLContext] = None,
    val blocker: Blocker
) extends RedisCommand[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]

  def this(b: Blocker) = this("localhost", 6379, blocker = b)
  def this(connectionUri: java.net.URI, b: Blocker) = this(
    host = connectionUri.getHost,
    port = connectionUri.getPort,
    database = RedisClient.extractDatabaseNumber(connectionUri),
    secret = Option(connectionUri.getUserInfo)
      .flatMap(_.split(':') match {
        case Array(_, password, _*) => Some(password)
        case _                      => None
      }),
    blocker = b
  )
  override def toString: String = host + ":" + String.valueOf(port) + "/" + database
  override def close(): Unit    = { disconnect; () }
}
