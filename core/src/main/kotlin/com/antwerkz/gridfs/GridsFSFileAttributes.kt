package com.antwerkz.gridfs

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

class GridsFSFileAttributes(private val path: GridFSPath) : BasicFileAttributes {
    override fun isRegularFile(): Boolean {
        return true
    }

    override fun lastAccessTime(): FileTime? {
        path.file?.metadata
        throw UnsupportedOperationException()
    }

    override fun isOther(): Boolean {
        return false
    }

    override fun isDirectory(): Boolean {
        return false
    }

    override fun isSymbolicLink(): Boolean {
        return false
    }

    override fun creationTime(): FileTime {
        return FileTime.from(getFile().uploadDate.toInstant())
    }

    private fun getFile() = path.file ?: throw RuntimeException("TODO")

    override fun size(): Long {
        return path.size()
    }

    override fun fileKey(): Any? {
        return path.path
    }

    override fun lastModifiedTime(): FileTime? {
        return creationTime()
    }
}