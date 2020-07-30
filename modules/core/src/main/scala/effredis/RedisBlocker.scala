package effredis

import cats.effect._
import java.util.concurrent._

trait RedisBlocker {
  def ec: Blocker
}

object RedisBlocker {
  def apply(blocker: Blocker): RedisBlocker =
    new RedisBlocker {
      def ec: Blocker = blocker
    }

  private[effredis] def make[F[_]: Sync]: Resource[F, Blocker] =
    Blocker.fromExecutorService(F.delay(Executors.newFixedThreadPool(1)))
}