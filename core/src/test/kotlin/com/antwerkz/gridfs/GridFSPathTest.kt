package com.antwerkz.gridfs

import com.mongodb.MongoClient
import org.testng.Assert
import org.testng.annotations.AfterTest
import org.testng.annotations.Test
import java.net.URI

class GridFSPathTest {
    val client = MongoClient("localhost")
    val fileSystem = GridFSFileSystemProvider(client)
            .getFileSystem(URI("gridfs://localhost/bob")) as GridFSFileSystem

    @AfterTest
    fun close() {
        client.close()
    }

    @Test
    fun testFileName() {
        val path = fileSystem.getPath("/tmp/some.file")
        Assert.assertEquals(path.fileSystem, fileSystem)
        Assert.assertEquals(path.fileName, GridFSPath(fileSystem, "some.file"))
    }

    @Test
    fun testEndsWithPath() {
        val path = fileSystem.getPath("/tmp/some.file")
        Assert.assertTrue(path.endsWith(GridFSPath(fileSystem, "some.file")))
        Assert.assertTrue(path.endsWith(GridFSPath(fileSystem, "/tmp/some.file")))
        Assert.assertFalse(path.endsWith(GridFSPath(fileSystem, "/tmp/")))
        Assert.assertFalse(path.endsWith(GridFSPath(fileSystem, "some-other.file")))
    }

    @Test
    fun testEndsWithString() {
        val path = fileSystem.getPath("/tmp/some.file")
        Assert.assertTrue(path.endsWith("some.file"))
        Assert.assertTrue(path.endsWith("/tmp/some.file"))
        Assert.assertFalse(path.endsWith("/tmp/"))
        Assert.assertFalse(path.endsWith("some-other.file"))
    }

   @Test
    fun testStartsWithPath() {
        val path = fileSystem.getPath("/tmp/some.file")
        Assert.assertTrue(path.startsWith(GridFSPath(fileSystem, "/tmp")))
        Assert.assertFalse(path.startsWith(GridFSPath(fileSystem, "tmp/some.file")))
        Assert.assertTrue(path.startsWith(GridFSPath(fileSystem, "/tmp/")))
        Assert.assertFalse(path.startsWith(GridFSPath(fileSystem, "some-other.file")))
    }

    @Test
    fun testStartsWithString() {
        val path = fileSystem.getPath("/tmp/some.file")
        Assert.assertFalse(path.startsWith("some.file"))
        Assert.assertTrue(path.startsWith("/tmp/some.file"))
        Assert.assertTrue(path.startsWith("/tmp/"))
        Assert.assertFalse(path.startsWith("some-other.file"))

    }

    @Test
    fun testAbsolute() {
        Assert.assertFalse(fileSystem.getPath("tmp/some.file").isAbsolute)
        Assert.assertTrue(fileSystem.getPath("/tmp/some.file").isAbsolute)
        Assert.assertTrue(fileSystem.getPath("/tmp").isAbsolute)
        Assert.assertTrue(fileSystem.getPath("/").isAbsolute)
        Assert.assertEquals(fileSystem.getPath("tmp/some.file").toAbsolutePath(), GridFSPath(fileSystem, "/tmp/some.file"))
        Assert.assertEquals(fileSystem.getPath("/tmp/some.file").toAbsolutePath(), GridFSPath(fileSystem, "/tmp/some.file"))
    }

    @Test
    fun testGetName() {
        val path = fileSystem.getPath("/path/to/some/file/file.txt")
        Assert.assertEquals(path.getName(0), GridFSPath(fileSystem, ""))
        Assert.assertEquals(path.getName(4), GridFSPath(fileSystem, "file"))
        Assert.expectThrows(IllegalArgumentException::class.java, { path.getName(40) })
    }

    @Test
    fun testGetNameCount() {
        Assert.assertEquals(GridFSPath(fileSystem, "/path/to/some/file/file.txt").nameCount, 6)
        Assert.assertEquals(GridFSPath(fileSystem, "file/file.txt").nameCount, 2)
    }

    @Test
    fun testGetParent() {
        Assert.assertEquals(GridFSPath(fileSystem, "/path/to/some/file/file.txt").parent, GridFSPath(fileSystem, "/path/to/some/file"))
        Assert.assertEquals(GridFSPath(fileSystem, "/path").parent, GridFSPath(fileSystem, "/"))
        Assert.assertNull(GridFSPath(fileSystem, "/").parent)
    }
}