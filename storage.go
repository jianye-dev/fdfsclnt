package fdfsclnt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

func storageUploadFile(addr string, storPathIndex int8, f *os.File) (groupid, filename string, err error) {
	var (
		extName [6]byte
		fi      os.FileInfo
	)

	if fi, err = f.Stat(); err != nil {
		return
	}

	return storageUploadFile2(addr, storPathIndex, extName[:], fi.Size(), f)
}

func storageUploadFile2(addr string, storPathIndex int8, extName []byte, size int64, r io.Reader) (groupid, filename string, err error) {
	var (
		conn net.Conn
		hdr  cmdhdr
		buf  []byte
		bufr *bytes.Reader
	)

	if conn, err = net.Dial("tcp", addr); err != nil {
		return
	}
	defer conn.Close()

	hdr.cmd = StorageProtoCmdUploadFile
	hdr.pkglen = int64(size + 15)

	if err = hdr.Write(conn); err != nil {
		return
	}
	if err = binary.Write(conn, binary.LittleEndian, storPathIndex); err != nil {
		return
	}
	if err = binary.Write(conn, binary.BigEndian, size); err != nil {
		return
	}

	if _, err = conn.Write(extName[:]); err != nil {
		return
	}

	_, err = io.CopyN(conn, r, size)
	if err != nil {
		return
	}

	if err = hdr.Read(conn); err != nil {
		return
	}

	if hdr.pkglen == 0 || hdr.status != 0 {
		err = fmt.Errorf("[RESP] storageUploadFile pkglen:%d cmd:%d status:%d", hdr.pkglen, hdr.cmd, hdr.status)
		return
	}

	buf = make([]byte, hdr.pkglen)

	if _, err = conn.Read(buf); err != nil {
		return
	}

	bufr = bytes.NewReader(buf)

	if groupid, err = readcstr(bufr, FDFSGroupNameMaxLen); err != nil {
		return
	}

	if filename, err = readcstr(bufr, int(hdr.pkglen-16)); err != nil {
		return
	}

	return
}

func storageDownloadFile(addr string, groupid, filename string, offset, size int64, w io.Writer) (err error) {
	var (
		conn     net.Conn
		hdr      cmdhdr
		_groupid [FDFSGroupNameMaxLen]byte
	)

	if len(groupid) > FDFSGroupNameMaxLen {
		err = ErrGroupIDTooLong
		return
	}

	copy(_groupid[:], groupid)

	if conn, err = net.Dial("tcp", addr); err != nil {
		return
	}
	defer conn.Close()

	hdr.cmd = StorageProtoCmdDownloadFile
	hdr.pkglen = int64(len(filename) + 32)

	if err = hdr.Write(conn); err != nil {
		return
	}

	if err = binary.Write(conn, binary.BigEndian, offset); err != nil {
		return
	}

	if err = binary.Write(conn, binary.BigEndian, size); err != nil {
		return
	}

	if _, err = conn.Write(_groupid[:]); err != nil {
		return
	}

	if _, err = conn.Write([]byte(filename)); err != nil {
		return
	}

	if err = hdr.Read(conn); err != nil {
		return
	}

	if hdr.pkglen == 0 || hdr.status != 0 {
		err = fmt.Errorf("[RESP] storageDownloadFile pkglen:%d cmd:%d status:%d", hdr.pkglen, hdr.cmd, hdr.status)
		return
	}

	if _, err = io.CopyN(w, conn, hdr.pkglen); err != nil {
		return
	}

	return
}

func storageDeleteFile(addr string, groupid, filename string) (err error) {
	var (
		conn     net.Conn
		hdr      cmdhdr
		_groupid [FDFSGroupNameMaxLen]byte
	)

	if len(groupid) > FDFSGroupNameMaxLen {
		err = ErrGroupIDTooLong
		return
	}

	copy(_groupid[:], groupid)

	if conn, err = net.Dial("tcp", addr); err != nil {
		return
	}
	defer conn.Close()

	hdr.cmd = StorageProtoCmdDeleteFile
	hdr.pkglen = int64(len(filename) + 16)

	if err = hdr.Write(conn); err != nil {
		return
	}

	if _, err = conn.Write(_groupid[:]); err != nil {
		return
	}

	if _, err = conn.Write([]byte(filename)); err != nil {
		return
	}

	if err = hdr.Read(conn); err != nil {
		return
	}

	if hdr.pkglen != 0 || hdr.status != 0 {
		err = fmt.Errorf("[RESP] storageDeleteFile pkglen:%d cmd:%d status:%d", hdr.pkglen, hdr.cmd, hdr.status)
		return
	}

	return
}
