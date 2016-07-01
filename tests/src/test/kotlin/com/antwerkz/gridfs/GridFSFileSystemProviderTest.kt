package com.antwerkz.gridfs

import com.mongodb.MongoClient
import org.testng.Assert
import org.testng.annotations.AfterTest
import org.testng.annotations.BeforeTest
import org.testng.annotations.Test
import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Files

class GridFSFileSystemProviderTest {
    lateinit var client: MongoClient

    @BeforeTest
    fun open() {
        client = MongoClient("localhost")
        client.getDatabase("gridfs").drop()
        client.getDatabase("gridfs1").drop()
        client.getDatabase("gridfs2").drop()
    }

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

        Files.newInputStream(path).reader().use  {
            Assert.assertEquals(it.readText(), "hello world");
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

        var path1 = fileSystem.getPath("/path/to/something/thats/../here")
        var path2 = fileSystem.getPath("/path/to/something/thats/not/../../here")

        Assert.assertEquals(path1.fileSystem, path2.fileSystem)
        Assert.assertTrue(Files.isSameFile(path1, path2))

        val fileSystem1 = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs1")) as GridFSFileSystem
        val fileSystem2 = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs2")) as GridFSFileSystem

        path1 = fileSystem1.getPath("/path/to/something/thats/../here")
        path2 = fileSystem2.getPath("/path/to/something/thats/not/../../here")

        Assert.assertFalse(Files.isSameFile(path1, path2))
        Assert.assertNotEquals(path1.fileSystem, path2.fileSystem)

    }

    @Test
    fun exists() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem
        val path = fileSystem.getPath("/path/to/nowhere")
        Assert.assertFalse(Files.exists(path));

        Files.newOutputStream(path).writer().use { writer ->
            writer.write("hello world")
            writer.flush();
        }

        Assert.assertTrue(Files.exists(path));
    }

    @Test
    fun moves() {
        val fileSystem = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs")) as GridFSFileSystem

        val path = fileSystem.getPath("/Users/jimmy/hello")

        Files.newOutputStream(path).writer().use { writer ->
            writer.write("hello world")
            writer.flush();
        }

        val hello = fileSystem.getPath("/Users/bob/hello")
        Files.move(path, hello)

        Files.newInputStream(hello).reader().use { reader ->
            Assert.assertEquals(reader.readText(), "hello world");
        }

        Assert.assertFalse(Files.exists(path))
    }

    @Test
    fun moveAcrossFileSystems() {
        val fileSystem1 = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs1")) as GridFSFileSystem
        val fileSystem2 = FileSystems.getFileSystem(URI("gridfs://localhost/gridfs2")) as GridFSFileSystem

        val path = fileSystem1.getPath("/Users/jimmy/hello")

        Files.newOutputStream(path).writer().use { writer ->
            writer.write("hello world")
            writer.flush();
        }

        val path1 = fileSystem2.getPath("/Users/bob/hello")
        Files.move(path, path1)

        Files.newInputStream(path1).reader().use { reader ->
            Assert.assertEquals(reader.readText(), "hello world");
        }

        Assert.assertFalse(Files.exists(path))
    }
}