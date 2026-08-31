package com.example
package commit

import org.apache.hadoop.fs.{FileSystem, Path}

class OutputCommitter(fs: FileSystem) {
  def commit(temp: Path, target: Path): Unit = {
    val backup = new Path(target.toString + ".backup")
    if (fs.exists(backup)) fs.delete(backup, true)
    if (fs.exists(target) && !fs.rename(target, backup)) {
      throw new IllegalStateException(s"Cannot move current output [$target] to backup")
    }
    try {
      if (!fs.rename(temp, target)) throw new IllegalStateException(s"Cannot commit [$temp] to [$target]")
      if (fs.exists(backup)) fs.delete(backup, true)
    } catch {
      case e: Throwable =>
        if (fs.exists(target)) fs.delete(target, true)
        if (fs.exists(backup)) fs.rename(backup, target)
        throw e
    }
  }
}
