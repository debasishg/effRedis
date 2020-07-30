package effredis

import cats.effect._

object Main extends IOApp {
  def blockingOp: IO[RedisClient] = IO(new RedisClient("localhost", 6379))

  def run(args: List[String]): IO[ExitCode] = {
    val _ = Blocker[IO].use { blocker =>
      for {
        r <- blocker.blockOn(blockingOp) 
      } yield (r)
    }

    IO(ExitCode.Success)
  }
  
}
