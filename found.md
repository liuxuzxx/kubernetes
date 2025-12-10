# 1. 概述

用来记录阅读 kubernetes 源码过程中的理解.

# 2. kubelet 的源码阅读

## 2.1 kubelet 的定位

1. Node agent,所有 k8s 在 Node 上的动作都由 kubelet 完成
2. kubelet 包含:Pod 的管理、和容器运行时打交道、Prober 的检测等等

## 2.2 prober 的源码阅读

prober 支持四种类型的探针:tcp,http,exec,grpc(为什么不支持 udp,因为 udp 没有回复,所以没法支持)

四种探针的实现,都实现了如下的 interface:

```golang
func (p grpcProber) Probe(host, service string, port int, timeout time.Duration) (probe.Result, string, error) {
    //探针自己的实现逻辑
    //1. tcp其实就是连一下,如果连接成功，则成功，否则失败，然后关闭连接
    //2. http就是发送一个GET请求，只有GET请求，认为http状态码在:[200,400)这个区间内的就是成功，否则是失败
    //3. exec就是执行执行命令,如果执行结果返回0,则认为成功，否则认为失败
    //4. grpc是发送一个grpc接口请求health接口，这个我目前不是很清楚grpc的health接口是否有个固定的地址
}

probe.Result: 成功/失败/未知等结果
string: 是结果内容，http是response,exec是执行结果,grpc是返回的response,tcp是空字符串
error: 就是返回的错误对象
```

目前的疑惑是:这些 probe 都已经定义了，应该如何使用，或者是用来干嘛的?这个是我们下面的阅读需求点

目前追踪到会通过一个 SyncPod 的方法定时的轮询处理这些 pod 的信息
下面开始追踪 Pod 的生命周期的处理，基本上可以从下面的顺序开始:

1. Delete Pod
2. Update Pod
3. Create Pod

## 2.3 磁盘压力的跟踪(DiskPressure)

1. 首先是确定了下面的代码是入口点

```go
#kubelet_node_status.go文件

setters = append(setters,
	nodestatus.MemoryPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderMemoryPressure, kl.recordNodeStatusEvent),
	nodestatus.DiskPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderDiskPressure, kl.recordNodeStatusEvent),
	nodestatus.PIDPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderPIDPressure, kl.recordNodeStatusEvent),
	nodestatus.ReadyCondition(kl.clock.Now, kl.runtimeState.runtimeErrors, kl.runtimeState.networkErrors, kl.runtimeState.storageErrors,
		kl.containerManager.Status, kl.shutdownManager.ShutdownStatus, kl.recordNodeStatusEvent, kl.supportLocalStorageCapacityIsolation()),
	nodestatus.VolumesInUse(kl.volumeManager.ReconcilerStatesHasBeenSynced, kl.volumeManager.GetVolumesInUse),
	// TODO(mtaufen): I decided not to move this setter for now, since all it does is send an event
	// and record state back to the Kubelet runtime object. In the future, I'd like to isolate
	// these side-effects by decoupling the decisions to send events and partial status recording
	// from the Node setters.
	kl.recordNodeSchedulableEvent,
)
```

2. 深入分析下 DiskPressureCondition 函数的作用

```go
#其实逻辑很简单,就是根据pressureFunc() 的返回值来判断,如果是true了,那么更新节点的磁盘压力状态为:true(意思就是磁盘有压力了，下一步可能是驱逐Pod了)
# 然后发送一个event出去
#如果是false,证明节点的磁盘使用正常，一切OK,那就正常的上报OK的状态就行了
# 所以很关键的就是去追踪pressureFunc() 这个函数
func DiskPressureCondition(nowFunc func() time.Time,
	pressureFunc func() bool,
	recordEventFunc func(eventType, event string),
) Setter {
	return func(ctx context.Context, node *v1.Node) error {
		currentTime := metav1.NewTime(nowFunc())
		var condition *v1.NodeCondition
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == v1.NodeDiskPressure {
				condition = &node.Status.Conditions[i]
			}
		}

		newCondition := false
		if condition == nil {
			condition = &v1.NodeCondition{
				Type:   v1.NodeDiskPressure,
				Status: v1.ConditionUnknown,
			}
			newCondition = true
		}
		condition.LastHeartbeatTime = currentTime
		// 下面这段处理磁盘压力的逻辑其实很简单:
		// 1. 执行函数pressureFunc()的结果作为判断的条件,如果存在磁盘压力，然后condition的status不是True(代表存在压力的意思)
		//    则需要修改状态以及Reason和Message这些信息，发送下recordEvent事件
		// 2. 如果不存在磁盘压力,则需要修改转台为False,并且发送下recordEvent事件
		// 3. 上面两个操作都需要在需要执行反转操作的时候才需要执行.就是true变为false或者是false变成true的时候才需要执行
		if pressureFunc() {
			if condition.Status != v1.ConditionTrue {
				condition.Status = v1.ConditionTrue
				condition.Reason = "KubeletHasDiskPressure"
				condition.Message = "kubelet has disk pressure"
				condition.LastTransitionTime = currentTime
				recordEventFunc(v1.EventTypeNormal, "NodeHasDiskPressure")
			}
		} else if condition.Status != v1.ConditionFalse {
			condition.Status = v1.ConditionFalse
			condition.Reason = "KubeletHasNoDiskPressure"
			condition.Message = "kubelet has no disk pressure"
			condition.LastTransitionTime = currentTime
			recordEventFunc(v1.EventTypeNormal, "NodeHasNoDiskPressure")
		}

		//如果是新的Condition,则需要追加到node的status.conditions数组中
		if newCondition {
			node.Status.Conditions = append(node.Status.Conditions, *condition)
		}
		return nil
	}
}
```

3. 追踪对应的 processFunc()函数，其实是下面的一个实现

```go
#pkg/kubelet/eviction/types.go文件中
# 这个地方有点奇怪啊,命名是属于一些统计和监控判断类型的东西，为什么放在了eviction中了?

type Manager interface {
	// Start starts the control loop to monitor eviction thresholds at specified interval.
	Start(ctx context.Context, diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc, podCleanedUpFunc PodCleanedUpFunc, monitoringInterval time.Duration)

	// IsUnderMemoryPressure returns true if the node is under memory pressure.
	IsUnderMemoryPressure() bool

	//就是这个函数
	IsUnderDiskPressure() bool

	// IsUnderPIDPressure returns true if the node is under PID pressure.
	IsUnderPIDPressure() bool
}
```

4. 看下具体的实现

```go
# 在 pkg/kubelet/eviction/eviction_manager.go 中

func (m *managerImpl) IsUnderDiskPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodeDiskPressure)
}

func hasNodeCondition(inputs []v1.NodeConditionType, item v1.NodeConditionType) bool {
	for _, input := range inputs {
		if input == item {
			return true
		}
	}
	return false
}

#总结下上面的实现,就是说磁盘是否有压力,就是看 m.nodeConditions中是否含有v1.NodeDiskPressure这个值
#非常朴素的算法和实现,就是说只要是资源类型有压力了,那么就放到nodeConditions这个数组中就行了
#使用的时候也很简单,只需要进行比对下是否含有就行了
# 那么下面的重点就是: nodeConditions的数值是怎么来的，谁给装填进去的?
```

5. 继续看 nodeConditions 这个数组的数值来自何方

```go
# pkg/kubelet/eviction/eviction_manager.go

func (m *managerImpl) synchronize(ctx context.Context, diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc) ([]*v1.Pod, error) {
	logger := klog.FromContext(ctx)
	// if we have nothing to do, just return
	thresholds := m.config.Thresholds
	if len(thresholds) == 0 && !m.localStorageCapacityIsolation {
		return nil, nil
	}

	logger.V(3).Info("Eviction manager: synchronize housekeeping")
	// build the ranking functions (if not yet known)
	// TODO: have a function in cadvisor that lets us know if global housekeeping has completed
	if m.dedicatedImageFs == nil {
		hasImageFs, imageFsErr := diskInfoProvider.HasDedicatedImageFs(ctx)
		if imageFsErr != nil {
			// TODO: This should be refactored to log an error and retry the HasDedicatedImageFs
			// If we have a transient error this will never be retried and we will not set eviction signals
			logger.Error(imageFsErr, "Eviction manager: failed to get HasDedicatedImageFs")
			return nil, fmt.Errorf("eviction manager: failed to get HasDedicatedImageFs: %w", imageFsErr)
		}
		m.dedicatedImageFs = &hasImageFs
		splitContainerImageFs, splitErr := diskInfoProvider.HasDedicatedContainerFs(ctx)
		if splitErr != nil {
			// A common error case is when there is no split filesystem
			// there is an error finding the split filesystem label and we want to ignore these errors
			logger.Error(splitErr, "eviction manager: failed to check if we have separate container filesystem. Ignoring.")
		}

		// If we are a split filesystem but the feature is turned off
		// we should return an error.
		// This is a bad state.
		if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletSeparateDiskGC) && splitContainerImageFs {
			splitDiskError := fmt.Errorf("KubeletSeparateDiskGC is turned off but we still have a split filesystem")
			return nil, splitDiskError
		}
		thresholds, err := UpdateContainerFsThresholds(m.config.Thresholds, hasImageFs, splitContainerImageFs)
		m.config.Thresholds = thresholds
		if err != nil {
			logger.Error(err, "eviction manager: found conflicting containerfs eviction. Ignoring.")
		}
		m.splitContainerImageFs = &splitContainerImageFs
		m.signalToRankFunc = buildSignalToRankFunc(hasImageFs, splitContainerImageFs)
		m.signalToNodeReclaimFuncs = buildSignalToNodeReclaimFuncs(m.imageGC, m.containerGC, hasImageFs, splitContainerImageFs)
	}

	logger.V(3).Info("FileSystem detection", "DedicatedImageFs", m.dedicatedImageFs, "SplitImageFs", m.splitContainerImageFs)
	activePods := podFunc()
	updateStats := true
	summary, err := m.summaryProvider.Get(ctx, updateStats)
	if err != nil {
		logger.Error(err, "Eviction manager: failed to get summary stats")
		return nil, nil
	}

	if m.clock.Since(m.thresholdsLastUpdated) > notifierRefreshInterval {
		m.thresholdsLastUpdated = m.clock.Now()
		for _, notifier := range m.thresholdNotifiers {
			if err := notifier.UpdateThreshold(ctx, summary); err != nil {
				logger.Info("Eviction manager: failed to update notifier", "notifier", notifier.Description(), "err", err)
			}
		}
	}

	// make observations and get a function to derive pod usage stats relative to those observations.
	observations, statsFunc := makeSignalObservations(logger, summary)
	debugLogObservations(logger, "observations", observations)

	// determine the set of thresholds met independent of grace period
	thresholds = thresholdsMet(logger, thresholds, observations, false)
	debugLogThresholdsWithObservation(logger, "thresholds - ignoring grace period", thresholds, observations)

	// determine the set of thresholds previously met that have not yet satisfied the associated min-reclaim
	if len(m.thresholdsMet) > 0 {
		thresholdsNotYetResolved := thresholdsMet(logger, m.thresholdsMet, observations, true)
		thresholds = mergeThresholds(thresholds, thresholdsNotYetResolved)
	}
	debugLogThresholdsWithObservation(logger, "thresholds - reclaim not satisfied", thresholds, observations)

	// track when a threshold was first observed
	now := m.clock.Now()
	thresholdsFirstObservedAt := thresholdsFirstObservedAt(thresholds, m.thresholdsFirstObservedAt, now)

	// 关键的就是这个步骤了
	nodeConditions := nodeConditions(thresholds)

    ...下面主要是nodeConditions进行采集周期内的时间条件的过滤和更新,不会更新什么
    ...省略无关的代码
	// update internal state
	m.Lock()
	m.nodeConditions = nodeConditions // 看到能操作nodeConditions的只有这么一行代码了
    ...省略设置其他的属性和状态的代码
    m.Unlock()
    ...省略无关的代码
}


#对应的nodeConditions方法也很简单
#解释下: 就是循环thresholds,如果能在signalToNodeCondition这个全局的Map中能找到,就放到结果中返回
# 例如: nodefs.available可能具有: Ready/DiskPressure两种情况
func nodeConditions(thresholds []evictionapi.Threshold) []v1.NodeConditionType {
	results := []v1.NodeConditionType{}
	for _, threshold := range thresholds {
		if nodeCondition, found := signalToNodeCondition[threshold.Signal]; found {
			if !hasNodeCondition(results, nodeCondition) {
				results = append(results, nodeCondition)
			}
		}
	}
	return results
}


# 综上可知，需要去追踪thresholds这个是怎么来的,这个才是真实的资源使用情况统计的一个阈值.
# 其实从synchronize这个方法的解释来看,这个方法是获取需要驱逐的Pod的信息的,然后上面的上报给master的磁盘压力状态只不过是
# 顺手做了的事情,就是说顺手做了统计了的事情.属于搭便车
```

6. 通过修改 kubelet 的输出日志的级别为:8,看出来了一些信息

```go
// 这行代码其实是根据summary获取到了可观测数据,也就是observations
observations, statsFunc := makeSignalObservations(logger, summary)

//接着根据下面的方法，根据不同的阈值返回不同的判断结果
thresholds := thresholdsMet(logger, thresholds, observations, false)

#总体评价下kubernetes这段代码逻辑写的不是那么的优雅,很绕圈子,也不是很好理解
#尤其是后面这个方法thresholdsMet，传入一个thresholds，返回的也是一个thresholds,容易混淆
```

7. 追踪 summary 是怎么来的

```go
# 注意记住，下面这几种类型都是被算作磁盘压力的
//	signalToNodeCondition[evictionapi.SignalImageFsAvailable] = v1.NodeDiskPressure
//	signalToNodeCondition[evictionapi.SignalContainerFsAvailable] = v1.NodeDiskPressure
//	signalToNodeCondition[evictionapi.SignalNodeFsAvailable] = v1.NodeDiskPressure
//	signalToNodeCondition[evictionapi.SignalImageFsInodesFree] = v1.NodeDiskPressure
//	signalToNodeCondition[evictionapi.SignalNodeFsInodesFree] = v1.NodeDiskPressure
//	signalToNodeCondition[evictionapi.SignalContainerFsInodesFree] = v1.NodeDiskPressure
// 其实就是:主要分为磁盘存储以及磁盘系统的inodes是否够用,然后又有一个维度: Image,Container,Node的，所以计算起来就是: 2 x 3 =6种类型
type SummaryProvider interface {
	// 这个是获取summary的方法
	Get(ctx context.Context, updateStats bool) (*statsapi.Summary, error)
	GetCPUAndMemoryStats(ctx context.Context) (*statsapi.Summary, error)
}
```

8. 继续查看实现

```go
#1. 首先是需要查看下具体从summary如何转换成observations的

func makeSignalObservations(logger klog.Logger, summary *statsapi.Summary) (signalObservations, statsFunc) {

	...省略不相关的代码逻辑
	# 下面这段代码是统计node的磁盘使用情况的
	if nodeFs := summary.Node.Fs; nodeFs != nil {
		if nodeFs.AvailableBytes != nil && nodeFs.CapacityBytes != nil {
			result[evictionapi.SignalNodeFsAvailable] = signalObservation{
				available: resource.NewQuantity(int64(*nodeFs.AvailableBytes), resource.BinarySI),
				capacity:  resource.NewQuantity(int64(*nodeFs.CapacityBytes), resource.BinarySI),
				time:      nodeFs.Time,
			}
		}
		if nodeFs.InodesFree != nil && nodeFs.Inodes != nil {
			result[evictionapi.SignalNodeFsInodesFree] = signalObservation{
				available: resource.NewQuantity(int64(*nodeFs.InodesFree), resource.DecimalSI),
				capacity:  resource.NewQuantity(int64(*nodeFs.Inodes), resource.DecimalSI),
				time:      nodeFs.Time,
			}
		}
	}

	#下面这段代码是获取Image和Container的磁盘的使用以及Inode的使用的情况
	if summary.Node.Runtime != nil {
		if imageFs := summary.Node.Runtime.ImageFs; imageFs != nil {
			if imageFs.AvailableBytes != nil && imageFs.CapacityBytes != nil {
				result[evictionapi.SignalImageFsAvailable] = signalObservation{
					available: resource.NewQuantity(int64(*imageFs.AvailableBytes), resource.BinarySI),
					capacity:  resource.NewQuantity(int64(*imageFs.CapacityBytes), resource.BinarySI),
					time:      imageFs.Time,
				}
			}
			if imageFs.InodesFree != nil && imageFs.Inodes != nil {
				result[evictionapi.SignalImageFsInodesFree] = signalObservation{
					available: resource.NewQuantity(int64(*imageFs.InodesFree), resource.DecimalSI),
					capacity:  resource.NewQuantity(int64(*imageFs.Inodes), resource.DecimalSI),
					time:      imageFs.Time,
				}
			}
		}
		if containerFs := summary.Node.Runtime.ContainerFs; containerFs != nil {
			if containerFs.AvailableBytes != nil && containerFs.CapacityBytes != nil {
				result[evictionapi.SignalContainerFsAvailable] = signalObservation{
					available: resource.NewQuantity(int64(*containerFs.AvailableBytes), resource.BinarySI),
					capacity:  resource.NewQuantity(int64(*containerFs.CapacityBytes), resource.BinarySI),
					time:      containerFs.Time,
				}
			}
			if containerFs.InodesFree != nil && containerFs.Inodes != nil {
				result[evictionapi.SignalContainerFsInodesFree] = signalObservation{
					available: resource.NewQuantity(int64(*containerFs.InodesFree), resource.DecimalSI),
					capacity:  resource.NewQuantity(int64(*containerFs.Inodes), resource.DecimalSI),
					time:      containerFs.Time,
				}
			}
		}
	}
	...省略不相关代码逻辑
	return result, statsFunc
}

#2. 接着看summary如何获取磁盘相关的信息,这样子我们有的放矢,就是只需要关注磁盘相关的数据信息获取就行了
func (sp *summaryProviderImpl) Get(ctx context.Context, updateStats bool) (*statsapi.Summary, error) {
	...省略无用的代码
	# 磁盘的统计信息获取
	rootFsStats, err := sp.provider.RootFsStats()
	#image和container的使用统计信息获取
	imageFsStats, containerFsStats, err := sp.provider.ImageFsStats(ctx)
	...省略无用代码

	nodeStats := statsapi.NodeStats{
		...省略其他无用属性
		Fs:               rootFsStats,
		Runtime:          &statsapi.RuntimeStats{ContainerFs: containerFsStats, ImageFs: imageFsStats},
	}
	...省略
}
```

9. 追踪 rootFsStats 如何获取的

```go
#pkg/kubelet/kubelet.go
#这个其实调用很奇怪的,kubelet作为一个顶层的逻辑纵览协调者,为什么会被下层的具体的summary.go的handler.go这种业务逻辑调用
# 难道不应该把这些做统计的单独出来一个文件或者是一个文件夹来处理吗?
func (kl *Kubelet) RootFsStats() (*statsapi.FsStats, error) {
	return kl.StatsProvider.RootFsStats()
}

# 继续查看StatsProvider.ROotFsStats()的实现

// RootFsStats returns the stats of the node root filesystem.
func (p *Provider) RootFsStats() (*statsapi.FsStats, error) {
	rootFsInfo, err := p.cadvisor.RootFsInfo()
    ...省略

	return &statsapi.FsStats{
		...省略不相关的属性设置
		AvailableBytes: &rootFsInfo.Available,
		CapacityBytes:  &rootFsInfo.Capacity,
		InodesFree:     rootFsInfo.InodesFree,
		Inodes:         rootFsInfo.Inodes,
	}, nil
}

//继续追踪cadvisor.RootFsInfo() 方法的获取,其实最终还是cadvisor承担下了所有的数据信息
//下面的代码其实很简单，就是获取:cc.rootPath的磁盘使用信息(默认是: /var/lib/kubelet)
func (cc *cadvisorClient) RootFsInfo() (cadvisorapiv2.FsInfo, error) {
	return cc.GetDirFsInfo(cc.rootPath)
}

#换句话来说，就是如果: /var/lib/kubelet这个目录你给写满了,或者是挂载的同一块磁盘写满了,就会出现DiskPressure的告警状态
// 下面这个方法的实现,印证了上面的说法,首先是根据dir获取所在的device,然后根据device获取FsInfo也就是磁盘和inode的使用信息
func (m *manager) GetDirFsInfo(dir string) (v2.FsInfo, error) {
	device, err := m.fsInfo.GetDirFsDevice(dir)
	if err != nil {
		return v2.FsInfo{}, fmt.Errorf("failed to get device for dir %q: %v", dir, err)
	}
	return m.getFsInfoByDeviceName(device.Device)
}
```

10. 继续查看如何获取到 ImageFs 的信息的

```go
# 1. 首先是需要看到在kubelet.go代码中的如下初始化的代码
    // kubelet有两种方式获取Image和Container的统计信息，一种是cadvisor,另外一种是CRI接口(就是类似于Docker/Containerd/Kata这种自己直接提供接口提供)
	if kubeDeps.useLegacyCadvisorStats {
		klet.StatsProvider = cadvisorStatsProvider
	} else {
		klet.StatsProvider = stats.NewCRIStatsProvider(
			klet.cadvisor,
			klet.resourceAnalyzer,
			klet.podManager,
			kubeDeps.RemoteRuntimeService,
			kubeDeps.RemoteImageService,
			hostStatsProvider,
			utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI),
			cadvisorStatsProvider,
		)
	}

	//具体useLegacyCadvisorStats是怎么确定的，就是通过方法UsingLegacyCadvisorStats
	kubeDeps.useLegacyCadvisorStats = cadvisor.UsingLegacyCadvisorStats(kubeCfg.ContainerRuntimeEndpoint)

	//查看具体的逻辑
	// 1. 如果开启了PodAndContainerStatsFromCRI特性，则返回false,那么上面对应也就是使用CRI接口
	// 2. 如果没有开启PodAndContainerStatsFromCRI特性,那么就看下runtimeEndpoint是否以: run/crio/crio.sock结尾，其实就是判断是不是cri-o这个运行时
	func UsingLegacyCadvisorStats(runtimeEndpoint string) bool {
    	// If PodAndContainerStatsFromCRI feature is enabled, then assume the user
    	// wants to use CRI stats, as the aforementioned workaround isn't needed
    	// when this feature is enabled.
    	if utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI) {
    		return false
    	}
    	return strings.HasSuffix(runtimeEndpoint, CrioSocketSuffix)
    }

# 2. 综上所述，可以得出结论: containerd使用的是CRI接口获取Image和Container的Fs以及其他的统计信息的

//就是直接和CRI的实现进行通信,看了下是通过GRPC进行通信的
func (r *remoteImageService) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	return r.imageFsInfoV1(ctx)
}

// 这个方法直接一次性就返回了imageFs和containerFs的stats，并且是同一个，不区分的
// 我在想，我们是否也可以使用CRI接口和containerd进行通信?如果这样子就基本能够验证我们的正确性了
func (p *criStatsProvider) ImageFsStats(ctx context.Context) (imageFsRet *statsapi.FsStats, containerFsRet *statsapi.FsStats, errRet error) {
	resp, err := p.imageService.ImageFsInfo(ctx)
	if err != nil {
		return nil, nil, err
	}

	if len(resp.GetImageFilesystems()) == 0 {
		return nil, nil, fmt.Errorf("imageFs information is unavailable")
	}
	fs := resp.GetImageFilesystems()[0]
	imageFsRet = &statsapi.FsStats{
		Time:      metav1.NewTime(time.Unix(0, fs.Timestamp)),
		UsedBytes: &fs.UsedBytes.Value,
	}
	if fs.InodesUsed != nil {
		imageFsRet.InodesUsed = &fs.InodesUsed.Value
	}
	imageFsInfo, err := p.getFsInfo(klog.FromContext(ctx), fs.GetFsId())
	if err != nil {
		return nil, nil, fmt.Errorf("get filesystem info: %w", err)
	}
	if imageFsInfo != nil {
		// The image filesystem id is unknown to the local node or there's
		// an error on retrieving the stats. In these cases, we omit those
		// stats and return the best-effort partial result. See
		// https://github.com/kubernetes/heapster/issues/1793.
		imageFsRet.AvailableBytes = &imageFsInfo.Available
		imageFsRet.CapacityBytes = &imageFsInfo.Capacity
		imageFsRet.InodesFree = imageFsInfo.InodesFree
		imageFsRet.Inodes = imageFsInfo.Inodes
	}
	// TODO: For CRI Stats Provider we don't support separate disks yet.
	return imageFsRet, imageFsRet, nil
}
```
