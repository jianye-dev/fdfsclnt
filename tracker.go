package fdfsclnt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type storInfo struct {
	groupname string
	addr      string
	pathindex int8
}

func (i *storInfo) parse(s []byte) (err error) {
	var (
		bufr   *bytes.Reader
		ipaddr string
		port   int64
	)

	bufr = bytes.NewReader(s)

	if i.groupname, err = readcstr(bufr, 16); err != nil {
		return
	}

	if ipaddr, err = readcstr(bufr, 15); err != nil {
		return
	}

	if err = binary.Read(bufr, binary.BigEndian, &port); err != nil {
		return
	}

	i.addr = fmt.Sprintf("%s:%d", ipaddr, port)

	binary.Read(bufr, binary.LittleEndian, &i.pathindex)

	return
}

func trackerQueryStoreWithoutGroupOne(addr string) (info storInfo, err error) {
	var (
		conn net.Conn
		hdr  cmdhdr
		buf  []byte
	)

	if conn, err = net.Dial("tcp", addr); err != nil {
		return
	}
	defer conn.Close()

	hdr.cmd = TrackerProtoCmdServiceQueryStoreWithoutGroupOne

	if err = hdr.Write(conn); err != nil {
		return
	}

	hdr.cmd = 0

	if err = hdr.Read(conn); err != nil {
		return
	}

	if hdr.pkglen == 0 || hdr.status != 0 {
		err = fmt.Errorf("[RESP] trackerQueryStoreWithoutGroupOne pkglen:%d cmd:%d status:%d", hdr.pkglen, hdr.cmd, hdr.status)
		return
	}

	buf = make([]byte, hdr.pkglen)

	if _, err = conn.Read(buf); err != nil {
		return
	}

	if err = info.parse(buf); err != nil {
		return
	}

	return
}

func trackerQueryQueryFetchOne(addr, groupid, filename string) (info storInfo, err error) {
	var (
		conn     net.Conn
		hdr      cmdhdr
		_groupid [FDFSGroupNameMaxLen]byte
		buf      []byte
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

	hdr.pkglen = int64(FDFSGroupNameMaxLen + len(filename))
	hdr.cmd = TrackerProtoCmdServiceQueryFetchOne

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

	if hdr.pkglen == 0 || hdr.status != 0 {
		err = fmt.Errorf("[RESP] trackerQueryQueryFetchOne pkglen:%d cmd:%d status:%d", hdr.pkglen, hdr.cmd, hdr.status)
		return
	}

	buf = make([]byte, hdr.pkglen)

	if _, err = conn.Read(buf); err != nil {
		return
	}

	if err = info.parse(buf); err != nil {
		return
	}

	return
}
