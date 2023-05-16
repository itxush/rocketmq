package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * MappedFile是RocketMQ内存映射文件的具体实现。
 * 将消息字节写入Page Cache缓冲区中(commit方法)，或者将消息刷入磁盘(flush)。
 * CommitLog consumerQueue、index三类文件磁盘的读写都是通过MappedFile。
 */
public class MappedFile extends ReferenceResource {
    // 常量，内存页大小，4KB
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 当前JVM实例中MappedFile的虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    // 当前JVM实例中
    // MappedFile对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 当前文件的写指针，从0开始(内存映射文件中的写指针)
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 当前文件的提交指针，如果开启transientStore-PoolEnable，则数据会存储在TransientStorePool中，然后提交到内存映射ByteBuffer中，再写入磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 将该指针之前的数据持久化存储到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 文件大小
    protected int fileSize;
    // 该MappedFile文件对应的channel
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 如果启用了TransientStorePool，则writeBuffer为从暂时存储池中借用的buffer，
     * 此时存储对象(比如消息等)会先写入该writeBuffer，然后commit到fileChannel，最后对fileChannel进行flush刷盘
     *
     * 这个buf包含整个文件大小(commitLog  = 1G(1073741824K))
     *
     */
    protected ByteBuffer writeBuffer = null;
    // 一个内存ByteBuffer池实现，如果如果启用了TransientStorePool则不为空
    protected TransientStorePool transientStorePool = null;
    // 文件名，其实就是该文件内容默认起始位置
    private String fileName;
    // 该文件中内容相对于整个文件的偏移，其实和文件名相同
    private long fileFromOffset;
    // 该MappedFile对应的实际文件
    private File file;
    /**
     * 通过fileChannel.map得到的可读写的内存映射buffer，如果没有启用TransientStorePool则写数据时会写到该缓冲中，刷盘时直接调用该映射buffer的force函数，而不需要进行commit操作
     * <p>
     * 修改MappedByteBuffer实际会将数据写入文件对应的Page Cache中，而TransientStorePool方案下写入的则为纯粹的内存。
     * 因此在消息写入操作上会更快，因此能更少的占用CommitLog.putMessageLock锁，从而能够提升消息处理量。使用TransientStorePool方案的缺陷主要在于在异常崩溃的情况下会丢失更多的消息。
     */
    private MappedByteBuffer mappedByteBuffer;
    // 文件最后一次写入内容的时间
    private volatile long storeTimestamp = 0;
    // 是否是MappedFileQueue队列中第一个文件。
    private boolean firstCreateInQueue = false;

    /**
     * 根据transientStorePoolEnable是否为true调用不同的构造方法。
     * <p>
     * transientStorePoolEnable=true(只在异步刷盘情况下生效)表示将内容先保存在堆外内存中。
     * TransientStorePool会通过ByteBuffer.allocateDirect调用直接申请堆外内存，消息数据在写入内存的时候是写入预申请的内存中
     * 通过Commit线程将数据提交到FileChannel中
     * 在异步刷盘的时候，再由刷盘线程（Flush线程）将数据持久化到磁盘文件。
     */
    public MappedFile() {
    }

    /**
     * 如果设置transientStorePoolEnable为false则调用此方法，参见
     * {@link AllocateMappedFileService#mmapOperation()}
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 如果设置transientStorePoolEnable为true则调用此方法，参见
     * {@link org.apache.rocketmq.store.config.MessageStoreConfig#isTransientStorePoolEnable()}
     * {@link org.apache.rocketmq.store.AllocateMappedFileService#mmapOperation()}
     */
    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 如果transientStorePoolEnable为true，则初始化MappedFile的writeBuffer，该buffer从transientStorePool中获取
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // 通过文件名获取起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 确保父目录存在
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            /*
             * FileChannel提供了map()方法把文件映射到虚拟内存，通常情况可以映射整个文件，如果文件比较大，可以进行分段映射，RocketMQ这里映射大小为(0,fileSize)。
             * 当通过map()方法建立映射关系之后，就不依赖于用于创建映射的FileChannel。特别是，关闭通道(Channel)对映射的有效性没有影响。
             * MappedFile的初始化(init)方法，初始化MappedByteBuffer，模式为MapMode.READ_WRITE(读/写)，此模式对缓冲区的更改最终将写入文件；
             * 但该更改对映射到同一文件的其他程序不一定是可见的。
             */
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb, PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
                                              PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
                                                   PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        // 获取MappedFile当前文件写指针
        int currentPos = this.wrotePosition.get();
        // 如果currentPos小于文件大小
        if (currentPos < this.fileSize) {
            /*
             * RocketMQ提供两种数据落盘的方式:
             * 1. 直接将数据写到mappedByteBuffer, 然后flush;
             * 2. 先写到writeBuffer, 再从writeBuffer提交到fileChannel, 最后flush.
             */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            /*
             * 设置下一个要被读或写的位置, 每次读写缓冲区都会改变该值，为下次读写作准备
             */
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 单个消息
            if (messageExt instanceof MessageExtBrokerInner) {
                // 追加消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBatch) { // 批量消息
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        // 如果currentPos大于或等于文件大小，表明文件已写满，抛出异常
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * commit()方法:内存映射的提交，commit操作主要针对异步刷盘模式
     *
     * @param commitLeastPages 为本次提交最小的页面，默认4页(4*4KB),可参见
     *                         {@link org.apache.rocketmq.store.CommitLog.CommitRealTimeService#run()}
     * @return
     */
    public int commit(final int commitLeastPages) {
        /*
         * 1.writeBuffer 为空就不提交，而writeBuffer只有开启
         * transientStorePoolEnable为true并且是异步刷盘模式才会不为空
         * 所以commit是针对异步刷盘使用的
         */
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            // 清理工作，归还到堆外内存池中，并且释放当前writeBuffer
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                // 创建writeBuffer的共享缓存区，slice共享内存，其实就是切片
                // 但是position、mark、limit单独维护
                // 新缓冲区的position=0，其capacity和limit将是缓冲区中剩余的字节数，其mark=undefined
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 上一次的提交指针作为position
                byteBuffer.position(lastCommittedPosition);
                // 当前最大的写指针作为limit
                byteBuffer.limit(writePos);
                // 把commitedPosition到wrotePosition的写入FileChannel中
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                // 更新提交指针
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够flush
     * 1. 文件已经写满
     * 2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     * 3. flushLeastPages==0&&有新写入的部分
     *
     * @param flushLeastPages flush最小分页
     *                        mmap映射后的内存一般是内存页大小的倍数，而内存页大小一般为4K，所以写入到映射内存的数据大小可以以4K进行分页，
     *                        而flushLeastPages这个参数只是指示写了多少页后才可以强制将映射内存区域的数据强行写入到磁盘文件
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();
        // 如果文件满了（文件大小与写入位置一样），则返回true
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            // 总共写入的页大小-已经提交的页大小>=最少一次写入的页大小，OS_PAGE_SIZE默认4kb
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
