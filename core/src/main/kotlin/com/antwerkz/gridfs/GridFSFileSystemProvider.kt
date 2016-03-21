package com.antwerkz.gridfs

import com.mongodb.MongoClientURI
import com.mongodb.client.gridfs.GridFSBuckets
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
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
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
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun newInputStream(path: Path, vararg options: OpenOption): InputStream {
        val fileSystem = path.fileSystem
        if ( fileSystem is GridFSFileSystem ) {
            val gridFsPath = path as GridFSPath

            val bucket = GridFSBuckets.create(fileSystem.database, gridFsPath.bucketName)
            return bucket.openDownloadStreamByName(path.file, GridFSDownloadByNameOptions())
        }
        throw InvalidPathException(path.toString(), "Only gridfs paths are allowed: $path")
    }

    override fun newOutputStream(path: Path, vararg options: OpenOption): GridFSUploadStream {
        val fileSystem = path.fileSystem
        if ( fileSystem is GridFSFileSystem ) {
            val gridFsPath = path as GridFSPath

            val bucket = GridFSBuckets.create(fileSystem.database, gridFsPath.bucketName)
            return bucket.openUploadStream(path.file, GridFSUploadOptions())
        }
        throw InvalidPathException(path.toString(), "Only gridfs paths are allowed: $path")
    }

    override fun newByteChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): SeekableByteChannel {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun newDirectoryStream(dir: Path, filter: Filter<in Path>): DirectoryStream<Path> {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun createDirectory(dir: Path, vararg attrs: FileAttribute<*>) {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun delete(path: Path) {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun copy(source: Path, target: Path, vararg options: CopyOption) {
        if(!Files.isSameFile(source, target)) {

        }
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun move(source: Path, target: Path, vararg options: CopyOption) {
        copy(source, target, *options)
        delete(source)
    }

    override fun isSameFile(path: Path, path2: Path): Boolean {
        return path.normalize().equals(path2.normalize())
    }

    override fun isHidden(path: Path): Boolean {
        return false;
    }

    override fun getFileStore(path: Path): FileStore {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun checkAccess(path: Path, vararg modes: AccessMode) {
        path as GridFSPath
        val fileSystem = path.fileSystem
        val bucket = GridFSBuckets.create(fileSystem.database, path.bucketName)
        val find = bucket.find(Filters.eq("filename", path.file))
        if(find.firstOrNull() == null) {
            throw FileNotFoundException("$path was not found in the '${path.bucketName}' bucket")
        }
    }

    override fun <V : FileAttributeView> getFileAttributeView(path: Path, type: Class<V>, vararg options: LinkOption): V {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun <A : BasicFileAttributes> readAttributes(path: Path, type: Class<A>, vararg options: LinkOption): A {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun readAttributes(path: Path, attributes: String, vararg options: LinkOption): Map<String, Any> {
        throw UnsupportedOperationException("Not implemented yet!")
    }

    override fun setAttribute(path: Path, attribute: String, value: Any, vararg options: LinkOption) {
        throw UnsupportedOperationException("Not implemented yet!")
    }
}