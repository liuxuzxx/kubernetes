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
