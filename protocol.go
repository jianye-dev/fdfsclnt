package fdfsclnt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// FastDFS Proto Cmd
const (
	TrackerProtoCmdResp                             = 100
	TrackerProtoCmdServiceQueryStoreWithoutGroupOne = 101
	TrackerProtoCmdServiceQueryFetchOne             = 102

	StorageProtoCmdUploadFile   = 11
	StorageProtoCmdDeleteFile   = 12
	StorageProtoCmdDownloadFile = 14

	FDFSProtoCmdActiveTest = 111
)

// FastDFS const
const (
	FDFSGroupNameMaxLen = 16
)

var (
	ErrGroupIDTooLong = errors.New("Group ID Too Long.")
	ErrInvalidFileID  = errors.New("Invalid File ID.")
)

type cmdhdr struct {
	pkglen int64
	cmd    int8
	status int8
}

func (h *cmdhdr) Write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, h.pkglen); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, h.cmd); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, h.status); err != nil {
		return
	}
	return
}

func (h *cmdhdr) Read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &h.pkglen); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &h.cmd); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &h.status); err != nil {
		return
	}
	return
}

func readcstr(r io.Reader, n int) (str string, err error) {
	var (
		b  [128]byte
		bs []byte
	)
	if n > len(b) {
		bs = make([]byte, n)
	} else {
		bs = b[:n]
	}

	if n, err = r.Read(bs); err != nil {
		return
	}

	if idx := bytes.IndexByte(bs, 0); idx < 0 {
		str = string(bs)
	} else {
		str = string(bs[:idx])
	}

	return
}
