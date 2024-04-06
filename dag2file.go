package merkledag

import (
	"encoding/json"
	"strings"
)

// Hash to file

func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型是tree
	flag, _ := store.Has(hash) //检查给定哈希值是否存在于KVStore中，如果存在则继续执行
	if flag {
		objBinary, _ := store.Get(hash)
		var obj Object
		json.Unmarshal(objBinary, &obj)
		pathArr := strings.Split(path, "/")
		cur := 1
		return getFileByDir(obj, pathArr, cur, store)
	}
	return nil
}

// 接收一个Object对象和一个KVStore存储实例作为参数，并返回一个字节切片
func getFileByList(obj Object, store KVStore) []byte {
	ans := make([]byte, 0)
	index := 0
	for i := range obj.Links {
		curObjType := string(obj.Data[index : index+4])
		index += 4
		curObjLink := obj.Links[i]
		curObjBinary, _ := store.Get(curObjLink.Hash)
		var curObj Object
		json.Unmarshal(curObjBinary, &curObj)
		//如果是BLOB类型，即表示找到了文件内容，将其添加到结果切片中；
		//如果是List类型，即表示需要进一步递归处理链接，并将处理结果添加到结果切片中。
		//最后返回结果切片。
		if curObjType == BLOB {
			ans = append(ans, curObjBinary...)
		} else { //List
			tmp := getFileByList(curObj, store)
			ans = append(ans, tmp...)
		}
	}
	return ans
}

// 接收一个Object对象、一个路径切片、当前路径的索引和一个KVStore存储实例作为参数，并返回一个字节切片
func getFileByDir(obj Object, pathArr []string, cur int, store KVStore) []byte {
	if cur >= len(pathArr) {
		return nil
	}
	index := 0
	for i := range obj.Links {
		objType := string(obj.Data[index : index+4])
		index += 4
		objInfo := obj.Links[i]
		if objInfo.Name != pathArr[cur] {
			continue
		}
		switch objType {
		//如果链接的类型是TREE，即表示需要进一步递归处理目录
		case TREE:
			objDirBinary, _ := store.Get(objInfo.Hash)
			var objDir Object
			json.Unmarshal(objDirBinary, &objDir)
			ans := getFileByDir(objDir, pathArr, cur+1, store)
			if ans != nil {
				return ans
			}
		//链接的类型是BLOB，即表示找到了文件内容，返回该内容
		case BLOB:
			ans, _ := store.Get(objInfo.Hash)
			return ans
		//链接的类型是LIST，即表示需要进一步递归处理列表，并返回处理结果
		case LIST:
			objLinkBinary, _ := store.Get(objInfo.Hash)
			var objLink Object
			json.Unmarshal(objLinkBinary, &objLink)
			ans := getFileByList(objLink, store)
			return ans
		}
	}
	return nil
}
