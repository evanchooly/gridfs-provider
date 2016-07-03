package com.antwerkz.gridfs

import com.mongodb.MongoClient
import com.mongodb.client.gridfs.GridFSBuckets
import org.testng.Assert.assertEquals
import org.testng.Assert.assertNotNull
import org.testng.annotations.Test
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths

class GridFSFileSystemProviderTest {
    @Test
    fun testByteChannelReads() {
        val url = "gridfs://localhost:27017/gridfs.archives/some/path/to/file"
        val path = GridFSFileSystemProvider().getPath(URI(url))
        assertNotNull(path)

        val mongoClient = MongoClient("localhost")
        val database = mongoClient.getDatabase("gridfs")
        database.drop();
        val gridFSBucket = GridFSBuckets.create(database, "archives");

        gridFSBucket.uploadFromStream("/some/path/to/file", "hello world".byteInputStream());

        assertEquals(String(Files.readAllBytes(path)), "hello world")
    }

    @Test
    fun testByteChannelWrites() {
        val url = "gridfs://localhost:27017/gridfs.archives/some/path/to/file"
        val path = GridFSFileSystemProvider().getPath(URI(url))
        assertNotNull(path)

        val mongoClient = MongoClient("localhost")
        val database = mongoClient.getDatabase("gridfs")
        database.drop();
        val gridFSBucket = GridFSBuckets.create(database, "archives");

        val channel = Files.newByteChannel(path)

        channel.write(ByteBuffer.wrap("hello world".toByteArray()))
        val bytes = ByteArrayOutputStream()
        gridFSBucket.downloadToStreamByName("/some/path/to/file", bytes);
        assertEquals(bytes.toString(), "hello world")
    }

    @Test
    fun testCache() {
        val provider = GridFSFileSystemProvider()
        val path1 = provider.getPath(URI("gridfs://localhost:27017/gridfs.archives/some/path/to/file"))
        val path2 = provider.getPath(URI("gridfs://localhost:27017/gridfs.archives/some/path/to/file"))
        val path3 = provider.getPath(URI("gridfs://localhost:27017/gridfs.archives/some/other/path/to/file"))

        assertEquals(path1.fileSystem, path2.fileSystem);
        assertEquals(path1.fileSystem, path3.fileSystem);
    }
}