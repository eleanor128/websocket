# Direct forwarding 与实时演示界面设计

依据当前 proposal 与 `cmd/loadgen` 的四 gateway 接口。本文是设计方案，尚未实现 Direct 服务端或演示页面。

## 1. 明确概念与验收目标

用户已确认：两个独立聊天视图分别连接不同 gateway，但进入同一个逻辑 room。一个视图发送，另一个通过服务器转发实时收到。

- gateway：部署中的一个服务进程，G0/G1/G2/G3。
- room_id：逻辑聊天室，同一个 room 可以有分布在四个 gateway 的成员。
- 聊天视图：浏览器中的客户端，每个视图有一条真实 WebSocket 连接。
- 不同 room_id 默认隔离。如果确实需要两个不同房间互相转发，应另行设计房间桥接规则；这不是 proposal 当前的全房间广播实验。

最小验收：左侧 G0 / demo-room 发送 hello，右侧 G1 / demo-room 出现相同 message_id 的 hello；切换右侧到 other-room 后，不应收到该消息。四视图版本应支持任意一个 gateway 作为源，其余三个全部收到且不重复。

## 2. Direct 的服务结构

四个 gateway 在应用层全互连。每个无序节点对维持一条持久、双向 WebSocket 连接，总计六条 overlay 连接。约定 ID 较小的一端负责主动拨号，避免建立重复连接；建连方不限制消息流向。

连接对：G0–G1、G0–G2、G0–G3、G1–G2、G1–G3、G2–G3。

处理客户端发布：
1. 校验消息格式，将连接身份绑定为 sender_id、room_id，将本节点写入 origin_gateway。
2. 向本地同 room 的客户端交付，包括发送者。
3. 向另外三个 gateway 分别排入一份 overlay 消息。

处理 peer 发布：
1. 校验 peer 身份与 origin_gateway；Direct 中它应来自源 gateway。
2. 只交付给本地同 room 的客户端。
3. 不再向其他 gateway 转发。

一条消息产生三次业务 overlay 发送，每个远端客户端仅经过一次 overlay 跳转。六条连接是拓扑连接数，三份副本是每次发布的业务发送量，两者不同。

不使用浏览器替服务器转发，也不在本地 UI 中复制一条消息到其他面板来模拟成功。

## 3. 数据与模块

建议新增独立 `cmd/gateway`，继续保留现有 `examples/chat` 作为单机示例。将可复用部分放在 `internal/chat` 中，后续拓扑共用本地广播、连接管理、协议、队列与指标，仅替换路由策略。

```text
cmd/gateway/           参数解析、启动客户端及 overlay 监听器
internal/chat/         房间成员、本地广播、客户端读写
internal/overlay/      peer 握手、六条持久连接、有界出口队列
internal/routing/      Direct 路由；后续 Star/Tree 共用接口
web/direct-demo/       四个真实 WebSocket 客户端的演示页面
configs/direct/       gateway 拓扑与演示端点配置
cmd/loadgen/           已有四 gateway 回放工具
```

客户端兼容现有 load generator：继续接受 run_id、message_id、sender_id、payload。通过 `/ws?name=...&room=...` 指定连接身份和房间，未提供 room 时默认 `room-all`。负载生成器不需要了解 overlay 地址。

内部统一消息建议：

```json
{
  "run_id": "demo-session-or-loadgen-run",
  "message_id": "unique-within-run",
  "sender_id": "Alice",
  "payload": "hello",
  "room_id": "demo-room",
  "origin_gateway": "G0"
}
```

现有 load generator 的 JSON 解码允许额外字段，但必须保留其 run_id、message_id、sender_id、payload 值和业务消息边界。payload 不添加用户名、不修改空白。

每条连接一个读协程、一个写协程，客户端与 peer 出口均有界。慢客户端不能阻塞整个房间；满队列、断线、写失败记录为显式指标。转发不持有全局房间锁等待网络写入。

## 4. 连接与异常策略

- 启动后先完成 peer 握手；相同 peer 的重复连接被拒绝，避免一个消息发两次。
- peer 身份应由实验配置与握手验证，客户端接口和 peer 接口分开。
- readiness 表示三个 peer 都连接好；演示页面另显示浏览器客户端连接状态，不能混为一谈。
- 演示模式可以重连 peer，但不自动补发已失败的业务消息。断开期间发布的未交付消息明确记为失败。
- 正式实验要求开始前 overlay ready；运行中 peer 断线标记 run 异常，不能把故障恢复影响混入正常拓扑容量比较。
- 去重若启用，使用有界的 `(run_id,message_id)` 记录，声明容量及有效期，并保留重复计数。不能宣称断线下具有 exactly-once 保证。
- 初版不增加全局消息排序：不同发送者并发发布时，不同 gateway 的到达顺序允许不同。界面按各自真实接收顺序展示。

## 5. 地址与部署

建议使用独立客户端端口与 overlay 端口，便于 proposal 中的流量分类。

| Gateway | 同机客户端端口 | 同机 overlay 端口 |
| --- | --- | --- |
| G0 | 8081 | 9081 |
| G1 | 8082 | 9082 |
| G2 | 8083 | 9083 |
| G3 | 8084 | 9084 |

初版在一台机器上运行四进程即可验证逻辑；不是四物理服务器的性能结论。多 EC2 部署时，每台可以复用相同端口，overlay 配置使用实例间可达的私网地址，浏览器及本地 load generator 使用可达的客户端地址。

客户端与 peer 都使用 WebSocket 传输，便于统一实现。实验流量整形需覆盖实际 overlay 路径；尤其同机连接不能绕过预期限制。带宽限制不由演示 UI 实现。

一个页面连接四个不同端口涉及跨 origin WebSocket。明确配置允许的演示页面 origin，不能依赖通配允许。HTTPS 页面对应使用 wss。peer 监听器仅允许实验节点访问，不向普通客户端暴露。

## 6. 实时演示界面

一页四个聊天面板，最先实现 G0/G1 双面板，之后扩展四面板。

```text
Direct 广播演示                       Room: demo-room
G0 ●──G1 ●     每次发送：本地交付 + 三个远端
│ ╲  ╱ │       拓扑连线表示连接状态，不代表可靠交付
│ ╱  ╲ │
G2 ●──G3 ●

┌ G0 · Alice · 已连接 ──────┐ ┌ G1 · Bob · 已连接 ─────────┐
│ hello                     │ │ hello                      │
│ 来源 G0 · 本地收到         │ │ 来源 G0 → G1 · 远端收到    │
│ ID abc · 接收时间          │ │ ID abc · 接收时间          │
│ [输入消息……] [发送]       │ │ [输入消息……] [发送]        │
└───────────────────────────┘ └────────────────────────────┘
┌ G2 · Carol ────────────────┐ ┌ G3 · Dave ────────────────┐
│ 独立接收日志               │ │ 独立接收日志               │
└───────────────────────────┘ └────────────────────────────┘
```

每个面板具备 gateway/endpoint、room、用户名、连接状态、发送框、接收日志和累计接收数量。状态明确区分连接中、已连接、已断开；断开时禁用发送并允许手动重连。

消息记录显示原始文本、message_id、源 gateway、本面板 gateway、接收时间。发送操作可以显示“已发出”，只有对应 WebSocket 真正收到回显后才显示“已收到”。消息内容通过 textContent 渲染，避免将用户文本作为 HTML 执行。

全局“切换房间”会重新建立四个面板的 room 连接；单面板也可切到 other-room 用来验证隔离。切换时应正确关闭旧 socket，避免残留订阅。消息量大时仅保留最近固定数量记录，防止浏览器 DOM 持续增长。

界面可用连线短暂高亮显示已观测到的发送/接收；不能凭前端定时器伪造网络状态。浏览器时间差只作演示，同一页面可用共同单调时钟计算已知消息的到达差；独立浏览器/机器间不宣称可直接得到准确单向延迟。正式指标以 load generator 为准。

演示客户端使用独立 demo-room，不加入正式测量的 room-all；正式实验时关闭演示，避免额外交付消耗资源并改变预期集合。

## 7. 测试清单与验收门槛

| 测试 | 预期 |
| --- | --- |
| 逐一由 G0/G1/G2/G3 发布 | 同 room 所有客户端各收到一次 |
| 一条消息的 overlay 计数 | 源发送三份；远端再转发零份 |
| 四 gateway 各两个客户端，四条消息 | 32 份唯一交付，12 次业务 overlay 发送 |
| 同时多源发布 | ID、payload 保留，无重复；不要求全局一致顺序 |
| 两个 room，成员分布跨 gateway | 只交付同 room，无串房 |
| peer 断线 | readiness/失败计数变化，其他连接不死锁；不伪报全部成功 |
| 重复 peer 握手或重连 | 不出现双连接重复广播 |
| 慢客户端/peer 队列满 | 有界内存、显式丢弃或断线记录、其他客户端继续工作 |
| 长文本、中文、换行、HTML 字符 | 内容完整保留，UI 不执行 HTML |
| UI 双窗口 | 一边发送，另一边收到相同 ID，显示各自真实接收记录 |
| UI 切换 room/连接失败/重连 | 状态真实，旧订阅清理，无伪成功 |
| 既有 200-client trace | 同一份 608-message trace，理想 121600 份唯一交付 |

先做路由单元测试，再做四个实际 gateway 进程的集成测试，最后接入既有 load generator。现有 `TestFourGatewayReplay` 使用模拟广播 fabric，只验证生成器，不能代替真实 Direct 实现的测试。

## 8. 实现顺序

1. 固定消息协议与 room 语义，实现可运行的单 gateway JSON 广播。
2. 建立 G0/G1 的持久 peer 链路，完成双向发布、只本地交付和房间隔离。
3. 扩展六条连接、三个 peer 扇出和 readiness/错误统计，完成四节点集成测试。
4. 将既有 load generator 接到真实四节点上，核对业务 overlay 与客户端交付计数。
5. 实现双面板演示，确保消息跨真实 socket 到达，再扩展为四面板与状态图。
6. 部署 EC2，检查客户端与 peer 可达性，最后开展资源校准和性能实验。

完成条件：服务端实际跨 gateway 转发；自动化测试与 load generator 对账通过；浏览器双面板可以实时互发、看到相同消息 ID；所有演示与性能结论明确区分。
