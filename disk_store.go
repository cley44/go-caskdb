package caskdb

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	file   *os.File
	keyDir map[string]KeyEntry
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	var f *os.File
	var err error
	if isFileExists(fileName) {
		f, err = os.Open(fileName)
		if err != nil {
			return nil, err
		}
		keydir := map[string]KeyEntry{}
		fileOffset := int64(0)
		for {
			b := make([]byte, headerSize)
			f.Seek(fileOffset, 0)
			fileOffset += headerSize
			n, err := f.Read(b)
			if err == io.EOF {
				// Reached end of file
				break
			}
			if err != nil {
				return nil, err
			}
			timestamp, keySize, valueSize := decodeHeader(b[:n])
			key := make([]byte, keySize)
			// value := make([]byte, valueSize)
			f.Seek(fileOffset, 0)
			n, err = f.Read(key)
			if err == io.EOF {
				// Reached end of file
				break
			}
			if err != nil {
				return nil, err
			}
			fileOffset += int64(keySize)
			f.Seek(fileOffset, 0)
			// f.Read(value)
			fileOffset += int64(valueSize)

			keydir[string(key[:n])] = KeyEntry{uint32(fileOffset) - valueSize - keySize - headerSize, headerSize + keySize + valueSize, timestamp}
		}

		return &DiskStore{file: f, keyDir: keydir}, nil
	} else {
		f, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
		return &DiskStore{file: f, keyDir: map[string]KeyEntry{}}, nil
	}
}

func (d *DiskStore) Get(key string) string {
	keyEntry, ok := d.keyDir[key]
	if !ok {
		return ""
	}
	b := make([]byte, keyEntry.totalSize)
	d.file.Seek(int64(keyEntry.position), 0)
	n, err := d.file.Read(b)
	if err != nil {
		panic(err)
	}
	_, key, value := decodeKV(b[:n])
	return value
}

func (d *DiskStore) Set(key string, value string) {
	currentTime := time.Now()
	timestamp := uint32(currentTime.Unix())
	n, byte_array := encodeKV(timestamp, key, value)
	stats, err := d.file.Stat()
	if err != nil {
		panic(err)
	}
	sizeOfFile := stats.Size()
	keyEntry := KeyEntry{uint32(sizeOfFile), uint32(n), timestamp}
	d.keyDir[key] = keyEntry
	d.file.Seek(0, 2)
	_, err = d.file.Write(byte_array)
	if err != nil {
		panic(err)
	}
}

func (d *DiskStore) Close() bool {
	err := d.file.Close()
	if err != nil {
		return false
	}
	return true
}
