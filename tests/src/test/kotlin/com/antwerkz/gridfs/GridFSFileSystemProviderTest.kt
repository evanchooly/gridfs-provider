package com.antwerkz.gridfs

import com.mongodb.MongoClient
import com.mongodb.client.gridfs.GridFSBuckets
import org.testng.Assert
import org.testng.annotations.AfterTest
import org.testng.annotations.Test
import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Files

class GridFSFileSystemProviderTest {
    val client = MongoClient("localhost")

    @AfterTest
    fun close() {
        client.close()
    }

    @Test
    fun loadFS() {
        loadData()
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem
        Assert.assertNotNull(fileSystem)
        Assert.assertTrue(fileSystem.isOpen)
        Assert.assertFalse(fileSystem.isReadOnly)

        val path = fileSystem.getPath("/Users/jimmy/hello")

        val writer = Files.newOutputStream(path).writer()
        writer.write("hello world")
        writer.flush();
        writer.close();

        val inputStream = Files.newInputStream(path)
        try {
            val readText = inputStream.reader().readText()
            Assert.assertEquals(readText, "hello world");
        } finally {
            inputStream.close();
        }
    }

    private fun loadData() {
        val database = client.getDatabase("gridfs")
        database.drop()
        val gridfs = GridFSBuckets.create(database, "/tmp")
        val uploadStream = gridfs.openUploadStream("some.file")
        val content = "I'm just a text file"
        val writer = uploadStream.writer()
        writer.write(content)
        writer.flush()
        uploadStream.close()

        val stream = gridfs.openDownloadStreamByName("some.file")
        try {
            Assert.assertEquals(stream.reader().readText(), content)
        } finally {
            stream.close()
        }
    }
}