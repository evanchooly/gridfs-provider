package com.antwerkz.gridfs

import com.mongodb.MongoClientURI
import com.mongodb.client.gridfs.GridFSUploadStream
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import com.mongodb.client.model.Filters
import java.io.FileNotFoundException
import java.io.InputStream
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.DirectoryStream.Filter
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.InvalidPathException
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.nio.file.spi.FileSystemProvider

class GridFSFileSystemProvider : FileSystemProvider() {
    override fun getScheme(): String {
        return "gridfs"
    }

    override fun newFileSystem(uri: URI, env: Map<String, *>?): FileSystem {
        return GridFSFileSystem(MongoClientURI(uri.toString().replace("gridfs://", "mongodb://")), this)
    }

    override fun getFileSystem(uri: URI): FileSystem {
        return newFileSystem(uri, null)
    }

    override fun getPath(uri: URI): Path {
//        val host = uri.host
//        val port = if (uri.port == -1) 27017 else uri.port
//        var split = uri.path.split('/')
//        if (split[0] == "") {
//            split = split.drop(1)
//        }
//        val db = split[0]
//        val collection = split[1]
        val split = uri.path.split("/")
        val ns = split[1]
        val path = split.drop(2).joinToString("/", "/")
        val mongoUri = uri.toString().replace(uri.path, "") + "/" + ns
        val fileSystem = getFileSystem(URI(mongoUri))
        return fileSystem.getPath(path)

    }

    override fun newInputStream(path: Path, vararg options: OpenOption): InputStream {
        val fileSystem = path.fileSystem
        if (fileSystem is GridFSFileSystem) {
            path as GridFSPath

            return fileSystem.bucket.openDownloadStreamByName(path.path, GridFSDownloadByNameOptions())
        }
        throw InvalidPathException(path.toString(), "Only gridfs paths are allowed: $path")
    }

    override fun newOutputStream(path: Path, vararg options: OpenOption): GridFSUploadStream {
        val fileSystem = path.fileSystem
        if (fileSystem is GridFSFileSystem) {
            path as GridFSPath

            return fileSystem.bucket.openUploadStream(path.path, GridFSUploadOptions())
        }
        throw InvalidPathException(path.toString(), "Only gridfs paths are allowed: $path")
    }

    override fun newByteChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): SeekableByteChannel {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun newDirectoryStream(dir: Path, filter: Filter<in Path>): DirectoryStream<Path> {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun createDirectory(dir: Path, vararg attrs: FileAttribute<*>) {
    }

    override fun delete(path: Path) {
        path as GridFSPath

        val bucket = path.fileSystem.bucket
        path.toGridFSFile()?.let { file ->
            bucket.delete(file.objectId)
        }
    }

    override fun copy(source: Path, target: Path, vararg options: CopyOption) {
        if (!Files.isSameFile(source, target)) {
            newOutputStream(target).use { outputStream ->
                newInputStream(source).use { inputStream ->
                    inputStream.copyTo(outputStream)
                }
            }
        }
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        source as GridFSPath
        target as GridFSPath
        if (!Files.isSameFile(source, target)) {
            if (source.fileSystem.equals(target.fileSystem)) {
                source.toGridFSFile()?.let { file ->
                    source.fileSystem.bucket.rename(file.objectId, target.path)
                }
            } else {
                copy(source, target, *options)
                delete(source)
            }
        }
    }

    override fun isSameFile(path: Path, path2: Path): Boolean {
        path as GridFSPath
        path2 as GridFSPath

        return path.fileSystem.equals(path2.fileSystem)
                && path.normalize().equals(path2.normalize())
    }

    override fun isHidden(path: Path): Boolean {
        return false;
    }

    override fun getFileStore(path: Path): FileStore {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        path as GridFSPath
        val fileSystem = path.fileSystem
        val find = fileSystem.bucket.find(Filters.eq("filename", path.path))
        if (find.firstOrNull() == null) {
            throw FileNotFoundException("$path was not found in the '${fileSystem.bucketName}' bucket")
        }
    }

    override fun <V : FileAttributeView> getFileAttributeView(path: Path, type: Class<V>, vararg options: LinkOption): V? {
        if (type == BasicFileAttributeView::class.java) {
            return GridFSFileAttributeView(path as GridFSPath) as V
        }
        return null
    }

    override fun <A : BasicFileAttributes> readAttributes(path: Path, type: Class<A>, vararg options: LinkOption): A {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun readAttributes(path: Path, attributes: String, vararg options: LinkOption): Map<String, Any> {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun setAttribute(path: Path, attribute: String, value: Any, vararg options: LinkOption) {
        throw UnsupportedOperationException("Not implemented")
    }
}

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

class GridsFSFileAttributes(private val path: GridFSPath) : BasicFileAttributes {
    override fun isRegularFile(): Boolean {
        return true
    }

    override fun lastAccessTime(): FileTime? {
        path.toGridFSFile()?.metadata
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

    private fun getFile() = path.toGridFSFile() ?: throw RuntimeException("TODO")

    override fun size(): Long {
        return getFile().length
    }

    override fun fileKey(): Any? {
        return path.path
    }

    override fun lastModifiedTime(): FileTime? {
        return creationTime()
    }
}