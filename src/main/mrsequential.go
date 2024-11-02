package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{} // 产生一个空的中间kv结构体
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))      //通过map得到 中间结果
		intermediate = append(intermediate, kva...) // 聚集所有的中间值和中间v
	}

	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate)) //按照中间kv 排序
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[], 对于每个key 调用reduce 函数
	// and print the result to mr-out-0. 打印结果到 结果文件中
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1

		// 根据当前的key 从这里开始遍历 往后找相同的 key , 将中间结果加起来

		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++ // 由于以前已经排好序了,  所以 i到j 即所有的 相同的key 的 中间值们
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value) // 把中级那只 结合起来, 放进values数组中
		}
		// 把这个key和对应的 结合起来的value 数组交给reduce函数, 由reduce函数进行处理@ values list
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j // 从下一个 key开始 !
		// 脑中有一个流程图在这里 !
	}

	ofile.Close() // 关闭文件
}

// load the application Map and Reduce functions
// 即加载 map和reduce 的func , 从一个文件中加载 方法!
// 😯教会了我如何从文件中加载插
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
