package com.antwerkz.gridfs

import org.testng.Assert
import org.testng.annotations.Test
import java.net.URI

class GridFSFileSystemProviderTest {
    @Test
    fun testGetPath() {
        val path = GridFSFileSystemProvider().getPath(URI("gridfs://localhost:27017/gridfs.files/some/path/to/file"))
        Assert.assertNotNull(path)
    }
}