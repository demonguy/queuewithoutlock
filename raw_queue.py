#!/usr/bin/env python
import sys
import time
from multiprocessing import shared_memory
import struct

class RawQueue(object):
    """docstring for RawQueue"""
    def __init__(self, name, length):
        super(RawQueue, self).__init__()
        self.memory = shared_memory.SharedMemory(name, create=True, size=length)
        self.buffer = self.memory.buf
        self.blen = shared_memory.SharedMemory(name + "_len", create=True, size=8)
        self.bhead = shared_memory.SharedMemory(name + "_head", create=True, size=8)
        self.btail = shared_memory.SharedMemory(name + "_tail", create=True, size=8)
        self.bhas_data = shared_memory.SharedMemory(name + "_has_data", create=True, size=1)

        self.len = length
        self.head = 0
        self.tail = 0
        self.has_data = False

    @property
    def len(self):
        return struct.unpack("Q", self.blen.buf)[0]

    @len.setter
    def len(self, value):
        self.blen.buf[0:] = struct.pack("Q", value)

    @property
    def head(self):
        return struct.unpack("Q", self.bhead.buf)[0]

    @head.setter
    def head(self, value):
        self.bhead.buf[0:] = struct.pack("Q", value)

    @property
    def tail(self):
        return struct.unpack("Q", self.btail.buf)[0]

    @tail.setter
    def tail(self, value):
        self.btail.buf[0:] = struct.pack("Q", value)

    @property
    def has_data(self):
        return struct.unpack("?", self.bhas_data.buf)[0]

    @has_data.setter
    def has_data(self, value):
        self.bhas_data.buf[0:] = struct.pack("?", value)
    

    def put(self, byte_list):
        if self.has_data and self.head == self.tail:
            raise RuntimeError("queue is full")
        elif len(byte_list) > self.len:
            raise RuntimeError("queue size is too small")
        else:
            # happens when buffer is not a ring yet
            if self.tail <= self.head:
                #heppens when tail is before head, which means the buffer is not a ring, end before end of buffer we have enough space
                if len(byte_list) <= self.len - self.head:
                    self.buffer[self.head:self.head+len(byte_list)] = byte_list
                    self.head += len(byte_list)
                    self.head %= self.len
                    self.has_data = True
                # happens when buffer is not a ring, and not enough space before the end of buffer
                elif len(byte_list) <= self.len - self.head + self.tail:
                    self.buffer[self.head:] = byte_list[:self.len-self.head]
                    self.buffer[:len(byte_list)-(self.len-self.head)] = byte_list[self.len-self.head:]
                    self.head = len(byte_list) - (self.len-self.head)
                    self.has_data = True
                else:
                    raise RuntimeError("not enough space in queue")

            #happens when buffer is already a ring
            elif self.tail > self.head:
                if len(byte_list) <= self.tail - self.head:
                    self.buffer[self.head:self.head+len(byte_list)] = byte_list
                    self.head += len(byte_list)
                else:
                    raise RuntimeError("not enough space in queue")
            else:
                raise RuntimeError("not enough space in queue")

    def get(self, length):
        if self.has_data:
            if self.head - self.tail >= length:
                r = self.buffer[self.tail:self.tail+length]
                self.tail = self.tail + length
            elif self.head - self.tail < 0 and self.len - self.tail + self.head >= length:
                if self.len - self.tail >= length:
                    r1 = self.buffer[self.tail:self.tail+length]
                    self.tail = self.tail + length
                    r = r1
                else:
                    r1 = self.buffer[self.tail:]
                    r2 = self.buffer[:length - (self.len - self.tail)]
                    self.tail = length - (self.len - self.tail)
                    r = bytes(r2)+bytes(r1)
            else:
                raise RuntimeError("not enough data to read")

            if self.tail == self.head:
                self.has_data = False
            return bytes(r)
        else:
            raise RuntimeError("no data in queue")

    def __exit__(self):
        self.memory.close()
        self.memory.unlink()
        self.blen.close()
        self.blen.unlink()
        self.bhead.close()
        self.bhead.unlink()
        self.btail.close()
        self.btail.unlink()
        self.bhas_data.close()
        self.bhas_data.unlink()

    def __del__(self):
        self.memory.close()
        self.memory.unlink()
        self.blen.close()
        self.blen.unlink()
        self.bhead.close()
        self.bhead.unlink()
        self.btail.close()
        self.btail.unlink()
        self.bhas_data.close()
        self.bhas_data.unlink()