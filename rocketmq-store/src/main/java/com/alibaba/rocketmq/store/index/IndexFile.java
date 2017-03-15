/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.MapedFile;


/**
 * 存储具体消息索引信息的文件
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static int HASH_SLOT_SIZE = 4;
    private static int INDEX_SIZE = 20;
    private static int INVALID_INDEX = 0;
    private final int hashSlotNum; // 默认500W
    private final int indexNum; // 默认2000W
    private final MapedFile mapedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;


    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * HASH_SLOT_SIZE) + (indexNum * INDEX_SIZE);
        this.mapedFile = new MapedFile(fileName, fileTotalSize);
        this.fileChannel = this.mapedFile.getFileChannel();
        this.mappedByteBuffer = this.mapedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }


    public String getFileName() {
        return this.mapedFile.getFileName();
    }


    public void load() {
        this.indexHeader.load();
    }


    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mapedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mapedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }


    /**
     * 当前索引文件是否写满
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }


    public boolean destroy(final long intervalForcibly) {
        return this.mapedFile.destroy(intervalForcibly);
    }


    /**
     * 如果返回false，表示需要创建新的索引文件
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;

            FileLock fileLock = null;

            try {
                // TODO 是否是读写锁
                // fileLock = this.fileChannel.lock(absSlotPos, HASH_SLOT_SIZE, false);
                // slotValue = 索引个数, 这个数值指的是处理完上一个消息后的索引个数, 但并没有加上此时处理的消息
                // 因为索引的开始位置是0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= INVALID_INDEX || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = INVALID_INDEX; // 0
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                // 时间差存储单位由毫秒改为秒
                timeDiff = timeDiff / 1000;

                // 25000天后溢出
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 索引的绝度物理偏移量, 根据indexCount递增
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * HASH_SLOT_SIZE + this.indexHeader.getIndexCount() * INDEX_SIZE;

                // 写入真正索引
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 更新哈希槽
                // 这个值很重要, 根据它, 可以将Message Key的hashcode一样的消息,串成一个链条
                // 根据这个值,可以找到此hashcode下的链表,最新的一个消息索引的物理位置
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 第一次写入
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount(); // 写入索引槽的值和索引内存,才更新数量 + 1
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            log.warn("putKey index count " + this.indexHeader.getIndexCount() + " index max num " + this.indexNum);
        }

        return false;
    }


    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }


    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }


    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }


    /**
     * 时间区间是否匹配
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();

        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());

        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }


    // 返回值是大于0
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) keyHashPositive = 0;
        return keyHashPositive;
    }


    /**
     * 前提：入参时间区间在调用前已经匹配了当前索引文件的起始结束时间
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin, final long end, boolean lock) {
        if (this.mapedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum; // 获取槽位的position
            // 每个slot是4位的int类型数据
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * HASH_SLOT_SIZE;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // HASH_SLOT_SIZE, true);
                }

                // Slot存储的值为存储当前消息之前的索引个数,因为索引偏移量从0开始
                // 根据次值,可以获取此hashcode下的链表,最新索引的物理偏移量
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= INVALID_INDEX || slotValue > this.indexHeader.getIndexCount() || this.indexHeader.getIndexCount() <= 1) {
                    // TODO NOTFOUND
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * HASH_SLOT_SIZE + nextIndexToRead * INDEX_SIZE;

                        // 每个索引为20个字节
                        // 4 key hash
                        // 8 commit log offset
                        // 4 timestamp
                        // 4 next index offset
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // int转为long，避免下面计算时间差值时溢出
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        // 读到了未知数据
                        if (timeDiff < 0) {
                            break;
                        }

                        // 时间差存储的是秒，再还原为毫秒， long避免溢出
                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= INVALID_INDEX || prevIndexRead > this.indexHeader.getIndexCount() || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                this.mapedFile.release();
            }
        }
    }
}
