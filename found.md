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
