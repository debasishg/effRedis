package effredis.algebra

trait NodeApi[F[_]] {

  /**
   * save the DB on disk now.
   */
  def save: F[Boolean]

  /**
   * save the DB in the background.
   */
  def bgsave: F[Boolean]

  /**
   * return the UNIX TIME of the last DB SAVE executed with success.
   */
  def lastsave: F[Option[Long]]

  /**
   * Stop all the clients, save the DB, then quit the server.
   */
  def shutdown: F[Boolean]

  def bgrewriteaof: F[Boolean]

  /**
   * The info command returns different information and statistics about the server.
   */
  def info: F[Option[String]]

  /**
   * is a debugging command that outputs the whole sequence of commands received by the Redis server.
   */
  def monitor: F[Boolean]

  /**
   * The SLAVEOF command can change the replication settings of a slave on the fly.
   */
  def slaveof(options: Any): F[Boolean]

}