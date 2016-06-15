package com.antwerkz.gridfs

import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.Document
import java.io.File
import java.net.URI
import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

class GridFSPath(val fileSystem: GridFSFileSystem, val path: String) : Path {

    constructor(fileSystem: GridFSFileSystem, elements: List<String>) : this(fileSystem, elements.joinToString(fileSystem.separator))

    fun toGridFSFile() : GridFSFile? {
        val bucket = fileSystem.bucket
        return bucket.find(Document("filename", path)).firstOrNull()
    }

    override fun toFile(): File {
        throw UnsupportedOperationException()
    }

    override fun isAbsolute(): Boolean {
        return path.startsWith("/")
    }

    override fun getFileName(): Path {
        return GridFSPath(fileSystem, path.split('/').last())
    }

    override fun getName(index: Int): Path {
        val split = path.split('/')
        return if (index < split.size) GridFSPath(fileSystem, split[index]) else throw IllegalArgumentException()
    }

    override fun subpath(beginIndex: Int, endIndex: Int): Path {
        val split: List<String> = path.split('/')
        if (beginIndex > split.size || endIndex > split.size) throw IllegalArgumentException()
        val strings: List<String> = split.subList(beginIndex, endIndex)
        return GridFSPath(fileSystem, strings)
    }

    override fun endsWith(other: Path): Boolean {
        var that = other.iterator().asSequence().toList()
        var me = iterator().asSequence().toList()
        var endsWith = true
        while (endsWith && me.size > 0 && that.size > 0) {
            endsWith = me.last().equals(that.last())
            me = me.dropLast(1)
            that = that.dropLast(1)
        }
        return endsWith
    }

    override fun endsWith(other: String): Boolean {
        return endsWith(GridFSPath(fileSystem, other))
    }

    override fun register(watcher: WatchService?, events: Array<out WatchEvent.Kind<*>>?, vararg modifiers: WatchEvent.Modifier?): WatchKey? {
        throw UnsupportedOperationException()
    }

    override fun register(watcher: WatchService?, vararg events: WatchEvent.Kind<*>?): WatchKey? {
        throw UnsupportedOperationException()
    }

    override fun relativize(other: Path?): Path? {
        throw UnsupportedOperationException()
    }

    override fun iterator(): MutableIterator<Path> {
        return split()
                .map { GridFSPath(fileSystem, it) }
                .toMutableList()
                .iterator()
    }

    override fun toUri(): URI {
        throw UnsupportedOperationException()
    }

    override fun toRealPath(vararg options: LinkOption?): Path {
        throw UnsupportedOperationException()
    }

    override fun normalize(): GridFSPath {
        val parts = split()
                .filter { it != "." }
                .toMutableList()
        var result = mutableListOf<String>()
        for (part in parts) {
            if(part == "..") {
                if(result.isEmpty()) {
                    throw IllegalStateException("Trying to normalize() an invalid path: ${path}")
                }
                result = result.dropLast(1) as MutableList<String>
            } else {
                result.add(part)
            }

        }
        return GridFSPath(fileSystem, result)
    }

    override fun getParent(): Path? {
        return if (path == "/") null else GridFSPath(fileSystem, path.split(fileSystem.separator).dropLast(1))
    }

    override fun getNameCount(): Int {
        return split().size
    }

    override fun compareTo(other: Path): Int {
        throw UnsupportedOperationException()
    }

    override fun startsWith(other: Path): Boolean {
        var that = other.iterator()
        var me = iterator()
        var starts = true
        while (starts && me.hasNext() && that.hasNext()) {
            starts = me.next().equals(that.next())
        }
        return starts
    }

    override fun startsWith(other: String): Boolean {
        return startsWith(GridFSPath(fileSystem, other))
    }

    override fun getFileSystem(): FileSystem {
        return fileSystem
    }

    override fun getRoot(): Path? {
        return fileSystem.getPath("/")
    }

    override fun resolveSibling(other: Path?): Path? {
        return fileSystem.getPath(path, other?.toString() ?: "")
    }

    override fun resolveSibling(other: String?): Path {
        return fileSystem.getPath(path, other ?: "")
    }

    override fun resolve(other: Path?): Path {
        return fileSystem.getPath(path + "/" + toGridFSPath(other).path)
    }

    override fun resolve(other: String?): Path {
        if (other == null) {
            throw IllegalArgumentException("The provided path can not be null")
        }
        return fileSystem.getPath(path + "/" + other)
    }

    private fun toGridFSPath(other: Path?): GridFSPath {
        if (other == null) {
            throw IllegalArgumentException("The provided path can not be null")
        } else if (other !is GridFSPath) {
            throw ProviderMismatchException("The provided path is not a GridFSPath")
        } else {
            return other
        }
    }

    override fun toAbsolutePath(): Path {
        if (path.startsWith("/")) {
            return this
        }
        return GridFSPath(fileSystem, fileSystem.separator + path)
    }

    override fun toString(): String {
        return fileSystem.provider().scheme + "://${fileSystem.hosts}/${fileSystem.database.name}.${fileSystem.bucketName}/${path}"
    }

    private fun split() = path.split(fileSystem.separator).dropLastWhile { it == "" }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other?.javaClass != javaClass) return false

        other as GridFSPath

        if (fileSystem != other.fileSystem) return false
        if (path != other.path) return false

        return true
    }

    override fun hashCode(): Int {
        var result = fileSystem.hashCode()
        result += 31 * result + path.hashCode()
        return result
    }


}
