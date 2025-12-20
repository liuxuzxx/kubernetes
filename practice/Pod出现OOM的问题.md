# 1. 概述

OOM Kill的出现.

目前发现南沙生产有一个服务cpaas-msg-hub在每隔一段时间之后,就会出现OOM Kill,Pod的内存从3G调整到3200Mi,又调整到3500Mi,但是还是无法阻止出现OOM Kill.
唯一的改变点就是:延迟了OOM Kill出现的时间,就是给的Pod内存越大,服务能够维持的时间也就越长.

# 2. 观察到的客观事实

1. 服务启动之后,看到container_memory_working_set_bytes这个指标会逐步的增长
2. container_memory_working_set_bytes这个指标的增长速度大概是:110Mb/天
3. 当内存增长到3.5GB之后,会维持大概25天-35天左右的时候,然后可能会在一个时间点触发OOM Kill,重启
4. 在整个服务启动期间,因为是Java服务,看到Java的堆一直正常的进行GC,从未出现过堆的OOM

# 3. 基本的推理

1. 这起OOM Kill的原因,大概率或者是说肯定是堆外内存的问题
2. 这个堆外内存不仅仅是Java的堆外面,也可能是container_memory_working_set_bytes组成的所有部分都有可能
3. 所以我们就开始逐一的调查和摸排container_memory_working_set_bytes的所有组成部分的变化

# 4. container_memory_working_set_bytes在K8S源码中的探究

1. 首先,我们先找到这个指标的生成代码:

```go
# prometheus.go
{
	name:      "container_memory_working_set_bytes",
	help:      "Current working set in bytes.",
	valueType: prometheus.GaugeValue,
	getValues: func(s *info.ContainerStats) metricValues {
		return metricValues{{value: float64(s.Memory.WorkingSet), timestamp: s.Timestamp}}
	},
}

# 从这段代码不难看出,container_memory_working_set_bytes是从ContainerStats.Memory.WorkingSet中获取的
# 那么很自然的就是去追踪ContainerStats.Memory.WorkingSet的生成过程了
```

2. 先不着急,先看下这几个结构体

```go
type ContainerStats struct {
    ...无关的属性省略
	Memory    MemoryStats             `json:"memory,omitempty"`
    ...无关的属性省略
}


type MemoryStats struct {

	// The amount of working set memory, this includes recently accessed memory,
	// dirty memory, and kernel memory. Working set is <= "usage".
	// Units: Bytes.
	WorkingSet uint64 `json:"working_set"`

    #大致总结和翻译下:这个WorkingSet包含: 最近访问的内存,脏内存,系统内核内存 <= usage这个数据

    ...无关的属性省略
}

# 下一步就很简单了,就是找出来给WorkingSet赋值的代码就行了
# 但是Golang允许struct在赋值的时候,可以不用指定属性名字,这个就没有找到,rust也是支持这种操作,但是属性名字必须一致才行
# 那就只能是继续往上面的类型找了,那就找ContainerStats,看下哪个地方有返回生成了ContainerStats
```

3. ContainerStats的生成

```go
# vender/github.com/google/cadvisor/info/v1/container.go
func (ci *ContainerInfo) StatsAfter(ref time.Time) []*ContainerStats {
	n := len(ci.Stats) + 1
	for i, s := range ci.Stats {
		if s.Timestamp.After(ref) {
			n = i
			break
		}
	}
	if n > len(ci.Stats) {
		return nil
	}
	return ci.Stats[n:]
}

# 我们先看下这个逻辑阿
# 首先是堆ci.Stats进行遍历,找到第一个大于ref(参数时间)的就break,然后返回 ci.Stats[n:]是一个数组,就是后面的全要了
```

4. 看下ContainerInfo这个结构体

```go
type ContainerInfo struct {
    ...无关的属性省略
	// Historical statistics gathered from the container.
	Stats []*ContainerStats `json:"stats,omitempty"`
}

# 那么问题就清晰了,看下Stats这个属性怎么生成的,查找了下,发现还是需要查找ContainerInfo是怎么设置和生成的才行
```

5. 找寻了一大堆的代码,就在代码和Interface来回穿梭的过程中,找到了真实执行的逻辑的地方

```go
# vender/github.com/google/cadvisor/manager/manager.go
func (m *manager) GetRequestedContainersInfo(containerName string, options v2.RequestOptions) (map[string]*info.ContainerInfo, error) {
	containers, err := m.getRequestedContainers(containerName, options)
	if err != nil {
		return nil, err
	}
	var errs partialFailure
	containersMap := make(map[string]*info.ContainerInfo)
	query := info.ContainerInfoRequest{
		NumStats: options.Count,
	}
	for name, data := range containers {
		info, err := m.containerDataToContainerInfo(data, &query)
		if err != nil {
			if err == memory.ErrDataNotFound {
				klog.V(4).Infof("Error getting data for container %s because of race condition", name)
				continue
			}
			errs.append(name, "containerDataToContainerInfo", err)
		}
		containersMap[name] = info
	}
	return containersMap, errs.OrNil()
}

# 就是去查找信息,重点是containerDataToContainerInfo这个方法
```

6. 插句话,之所以找不到ContainerInfo如何赋值的,原因如下:

```go
type TimedStore struct {
	buffer   timedStoreDataSlice
	age      time.Duration
	maxItems int
}

type timedStoreData struct {
	timestamp time.Time
	data      interface{}
}

#使用了这种数据结构来存储,类似于Java的Object
#但是数据来源肯定有地方,那就赵这个TimedStore数据来源,看到下面的方法:

func (c *containerCache) AddStats(stats *info.ContainerStats) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Add the stat to storage.
	c.recentStats.Add(stats.Timestamp, stats)
	return nil
}

#执行了AddStats方法,就是添加数据的意思,那就顺藤摸瓜去朝上找了
```

7. 如下方法

```go
func (cd *containerData) updateStats() error {
	stats, statsErr := cd.handler.GetStats()

    ...省略无关代码逻辑
    # 这地地方是关键点,关键就是这个地方执行了AddStats方法
	err = cd.memoryCache.AddStats(&cInfo, stats)
	...省略无关代码逻辑
}
```

8. 捕捉到获取的地方

```go
#注意这个方法GetStats()有三个实现,分别是container,raw,crio的实现,我们选择container的实现
// Get cgroup and networking stats of the specified container
func (h *Handler) GetStats() (*info.ContainerStats, error) {
	...省略无关的代码

	cgroupStats, err := h.cgroupManager.GetStats()
    ...省略
	stats := newContainerStats(cgroupStats, h.includedMetrics)
    ...省略
	return stats, nil
}

#查看newContainerStats方法
func newContainerStats(cgroupStats *cgroups.Stats, includedMetrics container.MetricSet) *info.ContainerStats {
	...

	if s := cgroupStats; s != nil {
		...
		setMemoryStats(s, ret)
		...
	}
	return ret
}


#继续看看WorkingSet怎么来的

func setMemoryStats(s *cgroups.Stats, ret *info.ContainerStats) {
	ret.Memory.Usage = s.MemoryStats.Usage.Usage
    ...
    # 因为我们是cgroup2模式,所以这里 inactiveFileKeyName = "inactive_file"
	workingSet := ret.Memory.Usage
	if v, ok := s.MemoryStats.Stats[inactiveFileKeyName]; ok {
		ret.Memory.TotalInactiveFile = v
		if workingSet < v {
			workingSet = 0
		} else {
			workingSet -= v
		}
	}
	ret.Memory.WorkingSet = workingSet
}

#计算逻辑也不复杂:简单来说就是: Usage - InactiveFile就是 WorkingSet的大小
#下面就要看看这个cgroup.Stats.MemoryStats.Usage.Usage是怎么来的
```

9. Usage怎么计算的

```go
func (m *Manager) GetStats() (*cgroups.Stats, error) {
	st := cgroups.NewStats()
    ...
	// memory (since kernel 4.5)
	if err := statMemory(m.dirPath, st); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
    ...
}

#继续查看statMemory是怎么来的

func statMemory(dirPath string, stats *cgroups.Stats) error {
	const file = "memory.stat"
	...

	memoryUsage, err := getMemoryDataV2(dirPath, "")
	if err != nil {
		if errors.Is(err, unix.ENOENT) && dirPath == UnifiedMountpoint {
			// The root cgroup does not have memory.{current,max,peak}
			// so emulate those using data from /proc/meminfo and
			// /sys/fs/cgroup/memory.stat
			return rootStatsFromMeminfo(stats)
		}
		return err
	}
	stats.MemoryStats.Usage = memoryUsage
	...
}

#继续查看getMemoryDataV2方法的实现

func getMemoryDataV2(path, name string) (cgroups.MemoryData, error) {
	...
	usage := moduleName + ".current"

    ...
	value, err := fscommon.GetCgroupParamUint(path, usage)
	memoryData.Usage = value

    ......
}


#举个例子,就是如下的文件的数据

/sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod17470c02_2435_410d_824b_5e7f833ec65a.slice/memory.current

#但是问题来了,memory.current这个是怎么计算的?
```