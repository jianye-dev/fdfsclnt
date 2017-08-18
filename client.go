package fdfsclnt

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Client FastDFS
type Client struct {
	TrackerAddr string
}

// Upload FastDFS Client Method
func (c Client) Upload(f *os.File) (fdfsid string, err error) {
	groupid, filename, err := c.Upload2(f)
	if err != nil {
		return
	}
	fdfsid = fmt.Sprintf("%s/%s", groupid, filename)
	return
}

// Upload2 FastDFS Client Method
func (c Client) Upload2(f *os.File) (groupid, filename string, err error) {
	var info storInfo
	if info, err = trackerQueryStoreWithoutGroupOne(c.TrackerAddr); err != nil {
		return
	}
	return storageUploadFile(info.addr, info.pathindex, f)
}

func (c Client) UploadStream(size int64, r io.Reader) (fdfsid string, err error) {
	var groupid, filename string

	if groupid, filename, err = c.UploadStream2(size, r); err != nil {
		return
	}

	fdfsid = fmt.Sprintf("%s/%s", groupid, filename)
	return
}

func (c Client) UploadStream2(size int64, r io.Reader) (groupid, filename string, err error) {
	var (
		info    storInfo
		extName [6]byte
	)
	if info, err = trackerQueryStoreWithoutGroupOne(c.TrackerAddr); err != nil {
		return
	}
	return storageUploadFile2(info.addr, info.pathindex, extName[:], size, r)
}

func (c Client) UploadStreamStorage(addr string, size int64, r io.Reader) (fdfsid string, err error) {
	var groupid, filename string

	if groupid, filename, err = c.UploadStream2Storage(addr, size, r); err != nil {
		return
	}

	fdfsid = fmt.Sprintf("%s/%s", groupid, filename)
	return
}

func (c Client) UploadStream2Storage(addr string, size int64, r io.Reader) (groupid, filename string, err error) {
	var (
		extName [6]byte
	)

	return storageUploadFile2(addr, 0, extName[:], size, r)
}

// Download FastDFS Client Method
func (c Client) Download(fdfsid string, w io.Writer) (err error) {
	strs := strings.SplitN(fdfsid, "/", 2)
	if len(strs) != 2 {
		return ErrInvalidFileID
	}
	return c.Download2(strs[0], strs[1], w)
}

// Download2 FastDFS Client Method
func (c Client) Download2(groupid, filename string, w io.Writer) (err error) {
	var info storInfo
	if info, err = trackerQueryQueryFetchOne(c.TrackerAddr, groupid, filename); err != nil {
		return
	}
	return storageDownloadFile(info.addr, groupid, filename, 0, 0, w)
}

// Delete FastDFS Client Method
func (c Client) Delete(fdfsid string) (err error) {
	strs := strings.SplitN(fdfsid, "/", 2)
	if len(strs) != 2 {
		return ErrInvalidFileID
	}
	return c.Delete2(strs[0], strs[1])
}

// Delete2 FastDFS Client Method
func (c Client) Delete2(groupid, filename string) (err error) {
	var info storInfo
	if info, err = trackerQueryQueryFetchOne(c.TrackerAddr, groupid, filename); err != nil {
		return
	}
	return storageDeleteFile(info.addr, groupid, filename)
}
