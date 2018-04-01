package scala_implicit

import scala.io.Source
import java.io.File

/**
  * Created by soap on 2018/3/31.
  */
class RichFile(val from: File) {
  def read = Source.fromFile(from.getPath).mkString
}