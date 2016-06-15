package com.antwerkz.gridfs

import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.MongoDatabase
import com.mongodb.client.gridfs.GridFSBucket
import com.mongodb.client.gridfs.GridFSBuckets
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

class GridFSFileSystem(uri: MongoClientURI, private val provider: GridFSFileSystemProvider) : FileSystem() {
    private var open = true
    val database: MongoDatabase
    val client: MongoClient
    val bucketName: String
    val bucket: GridFSBucket
    val hosts: String

    init {
        client = MongoClient(uri);
        hosts = uri.hosts.joinToString(",")
        database = client.getDatabase(uri.database)
        bucketName = uri.collection ?: "gridfs"
        bucket = GridFSBuckets.create(database, bucketName)
    }

    override fun getSeparator(): String {
        return "/"
    }

    override fun newWatchService(): WatchService? {
        throw UnsupportedOperationException()
    }

    override fun supportedFileAttributeViews(): MutableSet<String>? {
        throw UnsupportedOperationException()
    }

    override fun isReadOnly(): Boolean {
        return false
    }

    override fun getFileStores(): MutableIterable<FileStore>? {
        throw UnsupportedOperationException()
    }

    override fun getPath(first: String, vararg more: String): GridFSPath {
        return if (more.size == 0) GridFSPath(this, first) else GridFSPath(this, listOf(first) + more.asList());
    }

    override fun provider(): FileSystemProvider {
        return provider
    }

    override fun isOpen(): Boolean {
        return open
    }

    override fun getUserPrincipalLookupService(): UserPrincipalLookupService? {
        throw UnsupportedOperationException()
    }

    override fun close() {
        open = false
        client.close()
    }

    override fun getPathMatcher(syntaxAndPattern: String?): PathMatcher? {
        throw UnsupportedOperationException()
    }

    override fun getRootDirectories(): MutableIterable<Path>? {
        throw UnsupportedOperationException()
    }

    override fun equals(other: Any?): Boolean{
        if (this === other) return true
        if (other?.javaClass != javaClass) return false

        other as GridFSFileSystem

        if (database != other.database) return false
        if (bucketName != other.bucketName) return false

        return true
    }

    override fun hashCode(): Int{
        var result = database.hashCode()
        result += 31 * result + bucketName.hashCode()
        return result
    }

}