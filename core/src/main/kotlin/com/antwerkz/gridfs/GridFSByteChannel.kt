package com.antwerkz.gridfs

import java.io.InputStream
import java.io.OutputStream
import java.lang.Math.min
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

class GridFSByteChannel(val path: GridFSPath) : SeekableByteChannel {

    private var inputStream: InputStream? = null
    private var outputStream: OutputStream? = null

    private var position = 0

    override fun write(src: ByteBuffer): Int {
        val bytes = ByteArray(8192)
        var written = 0
        if (inputStream == null) {
            if (outputStream == null) {
                outputStream = path.newOutputStream()
            }
            outputStream?.use {
                while (src.remaining() > 0) {
                    val count = min(src.remaining(), bytes.size)
                    src.get(bytes, position, count)
                    written += count
                    outputStream?.write(bytes, position, count)
                }
            }
        } else {
            throw IllegalStateException("Path already open for reading.  Can not be opened for writing.")
        }
        return written
    }

    override fun truncate(size: Long): SeekableByteChannel? {
        throw UnsupportedOperationException()
    }

    override fun position(): Long {
        return position.toLong()
    }

    override fun position(newPosition: Long): SeekableByteChannel? {
        throw UnsupportedOperationException()
    }

    override fun size(): Long {
        return path.size()
    }

    override fun read(dst: ByteBuffer): Int {
        val bytes = ByteArray(8192)
        var read = 0
        if (outputStream == null) {
            if (inputStream == null) {
                inputStream = path.newInputStream()
                position = 0
            }
            inputStream?.let { stream ->
                var count = 0
                try {
                    while (dst.remaining() > position && count != -1) {
                        count = stream.read(bytes, position, min(dst.remaining(), bytes.size))
                        if (count != -1) {
                            dst.put(bytes, position, count)
                            position += count
                            read += count
                        }
                    }
                } finally {
                    if (count == -1) {
                        stream.close()
                        position = -1
                    }
                }
            }
        } else {
            throw IllegalStateException("Path already open for writing.  Can not be opened for reading.")
        }
        return read
    }

    override fun isOpen(): Boolean {
        return inputStream != null || outputStream != null
    }

    override fun close() {
        inputStream?.close()
        outputStream?.close()
    }

}