package effredis.cluster

import java.net.URI
import java.util.concurrent._

import cats.implicits._
import cats.effect._

import effredis.{ RedisClient, RedisBlocker, Log }

object ClusterClient {
  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log](
      blocker: Blocker,
      uris: URI*,
  ): Resource[F, ClusterClient[F]] = {

    val acquire: F[ClusterClient[F]] = {
      val clients = uris.map { uri =>
        new RedisClient(uri, 
          Blocker.liftExecutorService(Executors.newFixedThreadPool(1)))
      }.toList
      blocker.blockOn((new ClusterClient[F](clients: _*)).pure[F])
    }

    val release: ClusterClient[F] => F[Unit] = { c =>
      c.clients.map(_.disconnect)
      ().pure[F]
    }

    Resource.make(acquire)(release)
  }

  def make[F[+_]: ContextShift: Concurrent: Log](
      uris: URI*
  ): Resource[F, ClusterClient[F]] = {
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(blocker, uris:_*)
    } yield client
  }
}

sealed case class ClusterClient[F[+_]: Concurrent: ContextShift: Log] private (
    clients: RedisClient[F]*
)
