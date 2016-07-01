package com.antwerkz.gridfs

import com.mongodb.MongoClient
import com.mongodb.client.gridfs.GridFSBuckets
import org.testng.Assert
import org.testng.annotations.Test
import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Files

class GridFSFileSystemProviderTest {
    @Test
    fun testByteChannelReads() {
        val url = "gridfs://localhost:27017/gridfs.archives/some/path/to/file"
        val path = GridFSFileSystemProvider().getPath(URI(url))
        Assert.assertNotNull(path)

        val mongoClient = MongoClient("localhost")
        val database = mongoClient.getDatabase("gridfs")
        database.drop();
        val gridFSBucket = GridFSBuckets.create(database, "archives");

        gridFSBucket.uploadFromStream("/some/path/to/file", "hello world".byteInputStream());

        Assert.assertEquals(String(Files.readAllBytes(path)), "hello world")
    }

    @Test
    fun testByteChannelWrites() {
        val url = "gridfs://localhost:27017/gridfs.archives/some/path/to/file"
        val path = GridFSFileSystemProvider().getPath(URI(url))
        Assert.assertNotNull(path)

        val mongoClient = MongoClient("localhost")
        val database = mongoClient.getDatabase("gridfs")
        database.drop();
        val gridFSBucket = GridFSBuckets.create(database, "archives");

        val channel = Files.newByteChannel(path)

        channel.write(ByteBuffer.wrap("hello world".toByteArray()))
        val bytes = ByteArrayOutputStream()
        gridFSBucket.downloadToStreamByName("/some/path/to/file", bytes);
        Assert.assertEquals(bytes.toString(), "hello world")
    }
}