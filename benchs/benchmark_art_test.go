package main

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"testing"

	goart "github.com/Clement-Jean/go-art"
	radix "github.com/armon/go-radix"
	iradix "github.com/hashicorp/go-immutable-radix/v2"
	art "github.com/plar/go-adaptive-radix-tree/v2"
)

type Leaf struct {
	Key      string
	ParentId []int32
	Id       []int32
	FullPath []string
}
type TreeOps interface {
	Insert(key string, parentId []int32, id []int32, fullPath []string)
}

type Indexes struct {
	goart       goart.Tree[string, *Leaf]
	radix       *radix.Tree
	art         art.Tree
	iradix      *iradix.Tree[*Leaf]
	parentIndex []string
}

func NewIndexes() *Indexes {
	return &Indexes{
		goart:  goart.NewAlphaSortedTree[string, *Leaf](),
		radix:  radix.New(),
		art:    art.New(),
		iradix: iradix.New[*Leaf](),
	}
}

func (i *Indexes) Insert(key string, parentId int32, id int32) {

	if parentId >= id {
		return
	}
	// to build full path we look up the parent id in the parentIndex
	if len(i.parentIndex) <= int(id) {
		grow := make([]string, int(id)+1-len(i.parentIndex))
		i.parentIndex = append(i.parentIndex, grow...)
	}

	if parentId == 0 {
		i.parentIndex[int(id)] = key
	}
	if parentId > 0 {
		fp := i.parentIndex[int(parentId)]
		fpKey := fp + ">" + key
		i.parentIndex[int(id)] = fpKey
	}

	newLeaf := &Leaf{Key: key, ParentId: []int32{}, Id: []int32{}, FullPath: []string{}}

	if leaf, ok := i.radix.Get(key); ok {
		newLeaf = leaf.(*Leaf)
	}
	newLeaf.ParentId = append(newLeaf.ParentId, parentId)
	newLeaf.Id = append(newLeaf.Id, id)
	newLeaf.FullPath = append(newLeaf.FullPath, i.parentIndex[int(id)])

	i.radix.Insert(key, newLeaf)
	i.goart.Insert(key, newLeaf)
	i.art.Insert(art.Key(key), newLeaf)
	i.iradix, _, _ = i.iradix.Insert([]byte(key), newLeaf)
}

func setupIndexes() *Indexes {
	indexes := NewIndexes()
	fp, err := os.Open("neigborhood.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	reader := csv.NewReader(fp)
	reader.Comma = ';'
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Fatal(err)
		}
		parentId, err := strconv.Atoi(record[2])
		if err != nil {
			log.Fatal(err)
		}
		indexes.Insert(record[1], int32(parentId), int32(id))
	}
	return indexes
}

func BenchmarkIndexes(b *testing.B) {
	indexes := setupIndexes()
	prefix := "ABDİ"
	benchmarks := []struct {
		name   string
		fn     func(b *testing.B)
		prefix string
	}{
		{name: "radix", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				indexes.radix.WalkPrefix(prefix, func(key string, value any) bool {
					found = append(found, value.(*Leaf))
					return len(found) >= cap(found)
				})
				//b.Log(len(found))
			}
		}},
		{name: "goart", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				iter := indexes.goart.Prefix(prefix)
				for _, value := range iter {
					found = append(found, value)
				}
				//b.Log(len(found))

			}
		}},
		{name: "art", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				indexes.art.ForEachPrefix(art.Key(prefix), func(node art.Node) bool {
					found = append(found, node.Value().(*Leaf))
					return len(found) <= cap(found)
				})
				//b.Log(len(found))
			}
		}},
		{name: "iradix", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				root := indexes.iradix.Root()
				found := make([]*Leaf, 0, 100)
				root.WalkPrefix([]byte(prefix), func(key []byte, value *Leaf) bool {
					found = append(found, value)
					return len(found) >= cap(found)
				})
			}
		}},
	}
	b.ResetTimer()
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, benchmark.fn)
	}
}

func BenchmarkIndexesParallel(b *testing.B) {
	indexes := setupIndexes()
	b.ResetTimer()
	prefix := "ABDİ"
	benchmarks := []struct {
		name   string
		fn     func(b *testing.B)
		prefix string
	}{
		{name: "radix", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				indexes.radix.WalkPrefix(prefix, func(key string, value any) bool {
					found = append(found, value.(*Leaf))
					return len(found) >= cap(found)
				})
				//b.Log(len(found))
			}
		}},
		{name: "goart", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				iter := indexes.goart.Prefix(prefix)
				for _, value := range iter {
					found = append(found, value)
				}
				//b.Log(len(found))

			}
		}},
		{name: "art", fn: func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				found := make([]*Leaf, 0, 100)
				indexes.art.ForEachPrefix(art.Key(prefix), func(node art.Node) bool {
					found = append(found, node.Value().(*Leaf))
					return len(found) <= cap(found)
				})
				//b.Log(len(found))
			}
		}},
	}
	b.ResetTimer()
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchmark.fn(b)
				}
			})
		})
	}
}

func TestRadixIndexes(t *testing.T) {
	t.Skip("debug helper: use BenchmarkIndexes or run radix.WalkPrefix manually")
}

func TestIRadixIndexes(t *testing.T) {
	t.Skip("debug helper: use BenchmarkIndexes or run iradix.WalkPrefix manually")
}
