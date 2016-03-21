package com.antwerkz.gridfs

import com.mongodb.MongoClient
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
    fun roundTrip() {
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

    @Test
    fun normalize() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem

        val path1 = fileSystem.getPath("/path/to/something/thats/../here")
        val path2 = fileSystem.getPath("/path/to/something/thats/not/../../here")

        Assert.assertEquals(path1.normalize().path, fileSystem.getPath("/path/to/something/here").path)
        Assert.assertEquals(path2.normalize().path, fileSystem.getPath("/path/to/something/here").path)
    }

    @Test
    fun isSamePath() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem

        val path1 = fileSystem.getPath("/path/to/something/thats/../here")
        val path2 = fileSystem.getPath("/path/to/something/thats/not/../../here")

        Assert.assertTrue(Files.isSameFile(path1, path2))
    }

    @Test
    fun exists() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem
        val path = fileSystem.getPath("/path/to/nowhere")
        Assert.assertFalse(Files.exists(path));

        val writer = Files.newOutputStream(path).writer()
        writer.write("hello world")
        writer.flush();
        writer.close();

        Assert.assertTrue(Files.exists(path));
    }

    @Test
    fun moves() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem

        val path = fileSystem.getPath("/Users/jimmy/hello")

        val writer = Files.newOutputStream(path).writer()
        writer.write("hello world")
        writer.flush();
        writer.close();

        val path1 = fileSystem.getPath("/Users/bob/hello")
        Files.move(path, path1)

        val inputStream = Files.newInputStream(path1)
        try {
            val readText = inputStream.reader().readText()
            Assert.assertEquals(readText, "hello world");
        } finally {
            inputStream.close();
        }
    }
}