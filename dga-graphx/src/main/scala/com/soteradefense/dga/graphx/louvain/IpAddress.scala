package com.soteradefense.dga.graphx.louvain

import java.net.InetAddress
import java.nio.ByteBuffer

object IpAddress {
  
  def toString(address: Long) = {
    val byteBuffer = ByteBuffer.allocate(8)
    val addressBytes = byteBuffer.putLong(address)
    // The below is needed because we don't have an unsigned Long, and passing a byte array
    // with more than 4 bytes causes InetAddress to interpret it as a (bad) IPv6 address
    val tmp = new Array[Byte](4)
    Array.copy(addressBytes.array, 4, tmp, 0, 4)
    InetAddress.getByAddress(tmp).getHostAddress()
  }
  
  
  def toLong(_address: String): Long = {
    val address = try {
      InetAddress.getByName(_address)
    } catch {
      case e: Throwable => throw new IllegalArgumentException("Could not parse address: " + e.getMessage)
    }
    val addressBytes = address.getAddress
    val bb = ByteBuffer.allocate(8)
    addressBytes.length match {
      case 4 =>
        bb.put(Array[Byte](0,0,0,0)) // Need a filler
        bb.put(addressBytes)
      case n =>
        throw new IndexOutOfBoundsException("Expected 4 byte address, got " + n)
    }
    bb.getLong(0)
  }

}