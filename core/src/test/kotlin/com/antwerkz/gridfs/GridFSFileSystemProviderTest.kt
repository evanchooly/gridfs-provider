package com.antwerkz.gridfs

import com.mongodb.MongoClient
import com.mongodb.client.gridfs.GridFSBuckets
import com.mongodb.client.model.Filters
import org.testng.Assert
import org.testng.annotations.Test
import java.net.URI
import java.nio.file.Files

class GridFSFileSystemProviderTest {
    @Test
    fun testGetPath() {
        val url = "gridfs://localhost:27017/gridfs.archives/some/path/to/file"
        val path = GridFSFileSystemProvider().getPath(URI(url))
        Assert.assertNotNull(path)

        val mongoClient = MongoClient("localhost")
        val database = mongoClient.getDatabase("gridfs")
        database.drop();
        val gridFSBucket = GridFSBuckets.create(database, "archives");

        gridFSBucket.uploadFromStream("/some/path/to/file", "hello world".byteInputStream());

        val bytes = Files.readAllBytes(path)
        val toString = String(bytes)
        Assert.assertEquals(toString, "hello world")
    }
}