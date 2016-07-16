package com.antwerkz.gridfs

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.mongodb.MongoClientURI
import com.mongodb.client.gridfs.GridFSUploadStream
import com.mongodb.client.model.Filters
import java.io.FileNotFoundException
import java.io.InputStream
import java.net.MalformedURLException
import java.net.URI
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.DirectoryStream.Filter
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

class GridFSFileSystemProvider : FileSystemProvider() {
    val cache: LoadingCache<URI, GridFSFileSystem>

    init {
        cache = CacheBuilder
                .newBuilder()
                .maximumSize(10)
                .expireAfterWrite(10, SECONDS)
                .removalListener(object : RemovalListener<URI, GridFSFileSystem> {
                    override fun onRemoval(notification: RemovalNotification<URI, GridFSFileSystem>) {
                        println("notification = ${notification}")
                        println("pruning cache")
                        notification.value?.client?.close()
                    }

                })
                .build(
                        object : CacheLoader<URI, GridFSFileSystem>() {
                            override fun load(uri: URI): GridFSFileSystem {
                                return GridFSFileSystem(MongoClientURI(uri.toString().replace("gridfs://", "mongodb://")),
                                        this@GridFSFileSystemProvider)
                            }
                        })

    }

    override fun getScheme(): String {
        return "gridfs"
    }

    override fun newFileSystem(uri: URI, env: Map<String, *>?): GridFSFileSystem {
        return cache.get(uri)
    }

    override fun getFileSystem(uri: URI): FileSystem {
        return cache.get(uri)
    }

    override fun getPath(uri: URI): Path {
        val path = uri.path.dropWhile { it == '/' }
        val ns = path.substringBefore("/")
        if (!ns.contains('.')) {
            throw MalformedURLException("GridFS URIs must be in the form 'gridfs://<host>[:<port>]/<database>.<collection>'")
        }
        val fileName = path.substringAfter("/")
        val mongoUri = uri.toString().replace(path, "") + ns
        val fileSystem = getFileSystem(URI(mongoUri))
        return fileSystem.getPath("/" + fileName)
    }

    override fun newInputStream(path: Path, vararg options: OpenOption): InputStream {
        return checkPath(path).newInputStream()
    }

    override fun newOutputStream(path: Path, vararg options: OpenOption): GridFSUploadStream {
        return checkPath(path).newOutputStream()
    }

    override fun newByteChannel(path: Path?, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): SeekableByteChannel {
        return GridFSByteChannel(checkPath(path))
    }

    internal fun checkPath(path: Path?): GridFSPath {
        if (path == null) {
            throw NullPointerException()
        } else if (path !is GridFSPath) {
            throw ProviderMismatchException()
        } else {
            return path
        }
    }

    override fun newDirectoryStream(dir: Path, filter: Filter<in Path>): DirectoryStream<Path> {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun createDirectory(dir: Path, vararg attrs: FileAttribute<*>) {
    }

    override fun delete(path: Path) {
        path as GridFSPath

        val bucket = path.fileSystem.bucket
        path.file?.let { file ->
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
                source.file?.let { file ->
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

