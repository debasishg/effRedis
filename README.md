# effRedis
Non-blocking, effectful Scala client for Redis implemented using [cats](https://github.com/typelevel/cats) and [cats-effect](https://github.com/typelevel/cats-effect). It uses the existing [scala-redis](https://github.com/debasishg/scala-redis) client as the underlying implementation. I have plans of making a few improvements in the underlying implementation as well, but things will take time. I can only afford a couple of hours per week on this.

**This is an exercise that has just started and it will remain WIP for quite some time now. Feedback welcome.**


# Sample Usage

```scala
import java.net.URI
import cats.effect._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.makeWithURI[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      val result = for {

        _ <- set("key1", "debasish ghosh")
        _ <- set("key2", 100)
        _ <- set("key3", true)
        d <- get("key1")
        p <- incrby("key2", 12)
        a <- mget("key1", "key2", "key3")
        l <- lpush("list1", "debasish", "paramita", "aarush")

      } yield (d, p, a, l)

      println(result.unsafeRunSync())
      
      IO(ExitCode.Success)
    }
}
```