package com.antwerkz.gridfs

import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

class GridFSFileAttributeView(private val path: GridFSPath) : BasicFileAttributeView {
    override fun readAttributes(): BasicFileAttributes {
        return GridsFSFileAttributes(path)
    }

    override fun setTimes(lastModifiedTime: FileTime?, lastAccessTime: FileTime?, createTime: FileTime?) {
    }

    override fun name(): String? {
        return "gridfs"
    }
}