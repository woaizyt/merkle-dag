package merkledag

import (
	"encoding/json"
	"fmt"
	"hash"
	"math"
)

// Link结构体表示MerkleDag节点中的链接节点
// 包含了链接的名称Name、哈希值Hash和大小Size。
type Link struct {
	Name string
	Hash []byte
	Size int
}

// Object结构体表示MerkleDag节点中的对象节点
// 包含了链接对象切片Links和数据对象Data
type Object struct {
	Links []Link
	Data  []byte
}

// 定义了几个常量，分片大小常量CHUNK_SIZE（256K）
// 每层最大链接数常量MAX_LISTLINE
// 单位大小常量K、M
// 三种节点类型常量BLOB、LIST和TREE。
const (
	CHUNK_SIZE   = 256 * K
	MAX_LISTLINE = 4096
	K            = 1 << 10
	M            = K << 10
	BLOB         = "blob"
	LIST         = "link"
	TREE         = "tree"
)

// Add函数用于向MerkleDag中添加节点，并返回Merkle Root的字节表示。
// 根据节点的类型，调用相应的处理函数进行处理，并生成对象节点obj。
// 然后将对象节点转换为JSON格式，并计算其哈希值，最后返回哈希值的字节表示。
func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	obj := &Object{}
	switch node.Type() {
	case FILE:
		obj = handleFile(node, store, h)
		break
	case DIR:
		obj = handleDir(node, store, h)
		break
	}
	jsonObj, _ := json.Marshal(obj)
	return computeHash(jsonObj, h)

}

// 用于计算给定数据的哈希值
func computeHash(data []byte, h hash.Hash) []byte {
	h.Reset()
	h.Write(data)
	return h.Sum(nil)
}

// 用于将对象节点存储在KVStore中
func putObjInStore(obj *Object, store KVStore, h hash.Hash, objType string) {
	//将对象节点存储在KVStore中
	value, err := json.Marshal(obj)
	//判断KVStore中是否已经存在相同的哈希值，如果存在则不进行存储
	if err != nil {
		fmt.Println("json.Marshal err:", err)
		return
	}

	hash := computeHash(value, h)
	flag, _ := store.Has(hash)
	//如果不存在则将对象节点存储在KVStore中
	if flag {
		return
	}
	if objType == BLOB {
		store.Put(hash, obj.Data)
	} else {
		store.Put(hash, value)
	}

}

// 处理文件
func handleFile(node Node, store KVStore, h hash.Hash) *Object {
	//首先创建一个空的Object对象。如果文件大小超过分片大小，则进行文件分片处理，并生成多层链接节点，将链接节点存储在对象节点中。
	obj := &Object{}
	FileNode, _ := node.(File)
	if FileNode.Size() > CHUNK_SIZE {
		numChunks := math.Ceil(float64(FileNode.Size()) / float64(CHUNK_SIZE))
		height := 0
		tmp := numChunks
		// 计算分层的层数
		for {
			height++
			tmp /= MAX_LISTLINE
			if tmp == 0 {
				break
			}
		}
		obj, _ = dfshandleFile(height, FileNode, store, 0, h)
	} else { //如果文件大小未超过分片大小，则将文件的数据存储在对象节点的Data字段中，然后将对象节点存储在KVStore中。
		obj.Data = FileNode.Bytes()
		putObjInStore(obj, store, h, BLOB)
	}
	return obj

}

// 处理大文件的方法 递归调用，返回当前生成的obj已经处理了多少数据
func dfshandleFile(height int, node File, store KVStore, start int, h hash.Hash) (*Object, int) {
	obj := &Object{}
	lendata := 0
	// 如果只分一层
	if height == 1 {
		if len(node.Bytes())-start < CHUNK_SIZE {
			data := node.Bytes()[start:]
			obj.Data = append(obj.Data, data...)
			lendata = len(data)
			putObjInStore(obj, store, h, BLOB)
			return obj, lendata
		} else {
			for i := 1; i <= MAX_LISTLINE; i++ {
				end := start + CHUNK_SIZE
				// 确保不越界
				if end > len(node.Bytes()) {
					end = len(node.Bytes())
				}
				data := node.Bytes()[start:end]
				blobObj := Object{
					Links: nil,
					Data:  data,
				}
				putObjInStore(&blobObj, store, h, BLOB)
				jsonMarshal, _ := json.Marshal(blobObj)
				obj.Links = append(obj.Links, Link{
					Hash: computeHash(jsonMarshal, h),
					Size: int(len(data)),
				})
				obj.Data = append(obj.Data, []byte(BLOB)...)
				lendata += len(data)
				start += CHUNK_SIZE
				if start >= len(node.Bytes()) {
					break
				}
			}
			putObjInStore(obj, store, h, LIST)
			return obj, lendata
		}
	} else {
		// 如果不只有一层
		for i := 1; i <= MAX_LISTLINE; i++ {
			if start >= len(node.Bytes()) {
				break
			}
			tmpObj, tmpLendata := dfshandleFile(height-1, node, store, start, h)
			lendata += tmpLendata
			jsonMarshal, _ := json.Marshal(tmpObj)
			obj.Links = append(obj.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: tmpLendata,
			})
			if tmpObj.Links == nil {
				obj.Data = append(obj.Data, []byte(BLOB)...)
			} else {
				obj.Data = append(obj.Data, []byte(LIST)...)
			}
			start += tmpLendata
		}
		putObjInStore(obj, store, h, LIST)
		return obj, lendata
	}
}

// 处理文件夹,获取目录的迭代器，并遍历目录的子节点。对于每个子节点，根据其类型调用相应的处理函数进行处理
func handleDir(node Node, store KVStore, h hash.Hash) *Object {
	dirNode, _ := node.(Dir)
	iter := dirNode.It()
	treeObject := &Object{}
	for iter.Next() {
		node := iter.Node()
		switch node.Type() {
		case FILE: //对于文件类型的子节点，调用handleFile函数生成文件的对象节点，并将对象节点存储在链接节点中。
			file := node.(File)
			tmp := handleFile(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			if tmp.Links == nil {
				treeObject.Data = append(treeObject.Data, []byte(BLOB)...)
			} else {
				treeObject.Data = append(treeObject.Data, []byte(LIST)...)
			}

			break
		case DIR: //对于目录类型的子节点，调用handleDir函数生成目录的对象节点，并将对象节点存储在链接节点中。
			dir := node.(Dir)
			tmp := handleDir(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: computeHash(jsonMarshal, h),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			treeObject.Data = append(treeObject.Data, []byte(TREE)...)
			break
		}
	} //最后，将链接节点和数据类型存储在对象节点中，并将对象节点存储在KVStore中。
	putObjInStore(treeObject, store, h, LIST)
	return treeObject
}
