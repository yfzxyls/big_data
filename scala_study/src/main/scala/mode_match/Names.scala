package mode_match

/**
  * Created by soap on 2018/3/28.
  */
object Names {
  def unapplySeq(str: String): Option[Seq[String]] = {
    if (str.contains(",")) Some(str.split(","))
    else None
  }
}
