# 黄金双边自动交易 · 本机配对执行版

与「黄金差价监控」完全独立，仅监听 127.0.0.1:8766。

## 给使用者

目标运行环境：Windows 64 位，本机安装经纪商的桌面 MT5。
本应用内置 MetaTrader5 官方 Python 连接组件，分发 Windows 构建包后，无需用户安装 Python、挂 EA、安装独立桥接器或填写桥接端口。

## 快速开始（纸面交易）

### 1. 启动服务
```bash
# macOS 开发环境
python3 launcher.py

# Windows 构建包
双击 GoldPairLocal.exe
```

### 2. 配置纸面交易
1. 在「02 · 币安永续」选择执行模式为「纸面模拟」
2. Mac 选择「MT5 MCP 真实行情（Mac只读）」；Windows 选择原生 MT5 终端
3. 输入本次会话的 MCP Token（或设置 `MT5_TERMINAL_MCP_TOKEN`），点击「检查 MT5 连接」
4. 在「04 · 自动执行」点击「连接双边行情」
5. 点击「启动自动开仓」开始纸面交易
6. 观察下方实时价差图表和持仓组状态

「03」中的旧版纸面引擎只用于手动单步撮合验证，不会在后台持续运行；应用每次重启都会将它恢复为停止状态，但保留已有模拟持仓供查看。

### 3. 图表显示
- 连接成功后，下方的「05 · 实时与历史价差」会自动显示本机积累的可成交价差
- 图表显示入场/退出 Bid/Ask 口径折线、开仓阈值、每组止盈线和策略开平仓标记
- 可切换 1 小时至 30 天，拖动或缩放后可用「回到最新」恢复整个窗口
- 图表上方显示当前价差、窗口均值和范围，下方明确标出历史覆盖时间和采样数

## 完整配置流程

1. 在 MT5 登录自己的交易账户并保持终端运行；账号、服务器和交易密码由 MT5 管理。
2. 双击 GoldPairLocal.exe，程序启动本机服务并打开浏览器。保留控制台窗口；关闭会停止本机服务。
3. 点「检查 MT5 连接」。单个终端可自动查找；多个终端时展开设置指定 terminal64.exe。
4. 单个 XAU/USD 品种可自动识别；多个黄金品种时从候选列表选定，再检查。
5. 核对实际账号、服务器与黄金品种。检查成功后会自动应用最小手数、步长和每手盎司，并自动计算等盎司配平数量；修改每组手数后会重新计算。
6. 页面参数在停止输入约 0.8 秒后自动保存。可先在「02 · 币安永续」点「检查币安连接」，纸面模式检查公开行情与规格，实盘模式还检查账户交权限、持仓模式和手续费率；检查不下单。然后在「04 · 自动执行」连接双边行情，应用会读取两个平台的实际合约规则并检查等黄金数量是否可下单。
7. 先在纸面模式观察实时入场/退出价差折线和持仓组；实盘前，在 Windows 终端完成连接和对账，最后由使用者点「启动自动开仓」。

账号与服务器均可留空进行首次检查；应用结果后会保存为目标身份。后续检查发现终端登录到其他账户时会拒绝应用，不会自动切换账户。
修改连接配置、程序重启或超过五分钟后，数量预览要求重新检查。

只读检查可在自动交易关闭时运行。实盘执行需要：
- MT5 工具栏启用 Algo Trading / 自动交易；
- 工具 → 选项 → EA交易中取消「禁止通过外部 Python API 自动交易」；
- 使用交易密码登录；投资者密码只有只读权限。
检查页会提示这些问题，但不会自动更改终端权限，也不会发送测试订单。

## 执行模式与策略

方向固定为：**卖币安黄金永续，买 MT5 黄金**。每一组按 MT5 合约大小计算盎司数，再按币安的最小数量、步长、最大市价单数量和最低名义金额检查；不能等量配平时，应用拒绝启动。

- `paper`：纸上交易模式。使用币安正式环境行情和真实 MT5 行情；Mac 通过本机 MCP 只读轮询，任何一边都不发订单。适用于策略验证和参数调试。
- `live`：实盘交易模式。选择实盘时页面会自动切换到 Windows MT5 原生终端，连接真实币安账户和当前已登录的 MT5，允许真实下单。MT5 密码不在本应用中填写；币安仍必须输入本次会话的 API Key/Secret。完成账户与持仓对账，并点击「启动自动开仓」后才可能发送订单。

入场价差 = 币安 Bid × USDT/USD − MT5 Ask；退出价差 = 币安 Ask × USDT/USD − MT5 Bid。可选择每组或整篮子退出、按收窄幅度或目标价差止盈；选择后只显示对应的幅度/目标输入框。每一组在开仓时锁定参数；后续修改仅影响新组。Magic 只是 MT5 订单隔离标识，已收入高级设置，一般不需要修改。

可配置项包括每组手数、最大组数、总手数上限、重复开仓间隔、滑点、报价时效、最大单边敞口时间、最低净收益、单组/总亏损退出及最长持仓时间。正常止盈可要求最低预计净收益；止损、持仓超时和异常减仓不会被最低净收益阻止。

页面会根据模式和策略开关只显示当前有效的输入：“价差收窄”与“目标退出价差”不同时显示；实盘模式不显示纸面资金费、纸面 MT5 持仓费和旧版纸面撮合控件。

页面显示最新双边 Bid/Ask、报价时间、入场与退出价差折线、窗口统计、门槛/止盈线、开平仓标记、策略组、剩余敞口、预计净收益和事件日志。最近 24 小时保留高频采样，更早数据按分钟归档并保留 30 天；图表按观察窗口自动降采样，支持 1 小时至 30 天。这里故意不混入按 K 线收盘价反推的参考曲线，以免将“历史中间/收盘口径”误当成当时真正可成交的价差。

「06 · 收益与订单」按交易组显示开平仓时间、币安/MT5 配平量与开仓成交金额、交易毛收益、手续费、币安资金费收入/支出、MT5 swap、持仓期间实时净收益和已平仓净收益。双边订单流水另行列出每笔平台订单的申请数量、成交数量、成交价、成交金额、手续费、状态和平台票据。实盘平仓后，程序会用币安成交/收益历史和 MT5 deal 历史复核；复核前明确标为估算。

币安最优 Bid/Ask 优先使用官方 WebSocket `bookTicker` 长连接，断线自动重连并回退 REST。MetaTrader5 Python API 没有 tick 回调/WebSocket 接口，因此应用保持单一 MT5 工作进程和终端会话，每个策略周期读取最新 tick，默认周期 250 毫秒。报价最大年龄默认 3000 毫秒。

## 净收益与费用数据

- USDT/USD：默认自动读取币安 `USDTUSD` 资产指数，30 秒更新；数据暂时不可用时保留上次值并告警。
- 币安佣金：实盘连接时从账户费率接口读取 taker 费率；每笔成交后以 `userTrades` 实际 commission 覆盖预估，非 USDT 费用资产会换算为 USDT。
- 币安资金费：纸面模式使用人工预估；实盘持仓期间每分钟读取已入账的 `FUNDING_FEE`，并在多组重叠时按数量分配。
- MT5 佣金：MT5 Python API 没有通用的“账户佣金费率”字段；实盘成交前使用页面预估，成交后立即读取 deal commission/fee 覆盖。
- MT5 持仓费：纸面模式使用人工每手每日预估；实盘持仓直接读取当前 position swap，平仓后再用成交历史复核。

因为各 MT5 经纪商的佣金表不是 MT5 Python 标准属性，这一项无法在首笔成交前保证全自动。本版实盘限定 MT5 账户币种为 USD，避免将其他账户币种的佣金误当成 USD。

连接检查仍以独立进程在 18 秒内完成；持续 MT5 会话也放在独立进程，终端卡住不会卡死网页。

## 成交、异常与对账

每次订单先写入本机 SQLite 日志，再向平台发送；使用唯一订单标识。网络超时、未知回报或 MT5 调用超时都不会直接重发，而是先查询订单、成交和持仓。开仓先买 MT5，再以 IOC 卖币安，并受最大单边敞口时间和滑点预算限制。

- 任一开仓腿部分成交、拒绝或不明：暂停新开仓，取消可取消订单，按已成交实际数量撤销本次新组；状态不明时不重发。
- 平仓某一腿成功：不反向重开，继续处理剩余腿；达到重试上限后进入需人工关注状态。
- 程序重启或账户断线：暂停新开仓，必须连接到原账户并点击「持仓对账」。若实际仓位与本策略日志不一致，应用拒绝自动处理。
- 只处理带本程序 Magic 标识和唯一注释的 MT5 仓位，拒绝把其他手工仓位纳入策略。

平仓后会按币安订单成交记录、MT5 成交历史和资金费记录复核费用；复核前显示的是预计净收益。资金费归属依赖本程序是该合约唯一受管仓位，因此对账不一致时不会自动归属。

## 本机数据

源码运行：项目 data/。
Windows 构建包：%LOCALAPPDATA%\GoldPairLocal\，升级应用不会覆盖数据。
配置写入采用临时文件加原子替换。币安 API Key/Secret 仅保存在运行中的进程内存中，用于本次连接；不写入配置文件、SQLite 日志或 API 响应，程序重启后必须重新输入。请为此应用单独创建最小权限的币安正式网 API Key，不要开启提币权限。本版没有混合测试网下单模式；`paper` 模式只读正式网公开行情且不下单。旧配置若已有凭据，接口不会回传其内容。
本机服务检查 Host 与写请求来源，不开放局域网；不提供任意文件下载接口。分发包通过明确资源清单构建，不打入 data/ 和个人密钥。

## Mac 的 MT5 MCP 真实行情

Mac 开发环境不再需要手填模拟 Bid/Ask。页面通过本机 `http://127.0.0.1:22346/mcp` 读取 MT5 账户身份、黄金品种、合约规格和实时 Bid/Ask。MCP 适配器仅实现读取命令，不调用下单、改单或平仓工具。

Token 只可通过页面当前会话传入，或在启动前设置环境变量：

```bash
export MT5_TERMINAL_MCP_TOKEN='重新生成的Token'
python3 launcher.py
```

Token 不写入 `data/config.json`、SQLite、事件日志或 API 响应。MCP 地址只允许本机 HTTP 回环地址，防止将凭据发送到外部主机。

## 开发与 Windows 打包

macOS 可运行页面、单元测试和 MCP 真实行情纸面交易，但不能在原生 macOS Python 中使用 Windows MetaTrader5 交易组件。Windows 安装包须在 Windows 64 位构建和验收。

Mac 验证：选择 MT5 MCP 真实行情 → 检查并应用实际品种规格 → 选择纸面模式 → 连接双边行情 → 观察价差图和纸面成交。纸面成交使用当时真实报价，但不代表实际市价单可成交结果。

源码预览：python3 launcher.py
测试：python3 -m unittest discover -s tests -p 'test_*.py'
Windows 构建机安装 Python 3.11 x64，然后运行：
powershell -ExecutionPolicy Bypass -File build_windows.ps1

打包脚本创建隔离构建环境，安装固定版本依赖，运行测试，打入 MetaTrader5 组件与静态页面，并执行包内组件导入自检。产物：
dist/GoldPairLocal-Windows.zip

仓库已配置 `.github/workflows/windows-build.yml`。推送到 `main` 后 GitHub Actions 会在 Windows x64 环境运行同一打包脚本，成功后可在该次 Actions 的 Artifacts 中下载 `GoldPairLocal-Windows`。MCP Token、本机配置和交易数据均被 `.gitignore` 排除。

接收者解压整个文件夹后双击 GoldPairLocal.exe；不要只复制 exe。
包内不含 MT5 终端，经纪商终端需要预先安装登录。构建脚本自检不代替实机 MT5 连接验收。
当前开发环境为 macOS：尚未生成或验证 Windows exe，也没有对真实 Windows MT5、币安账户权限、手续费、成交质量或断线恢复做实机验收，不能把构建脚本视为已验收安装包。实盘前必须在 Windows 模拟账户和小额环境分别验收。

## 实现依据

- Binance USDⓈ-M `bookTicker` WebSocket：https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams
- Binance 账户佣金费率：https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/User-Commission-Rate
- Binance 资产指数：https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Multi-Assets-Mode-Asset-Index
- MetaQuotes initialize：https://www.mql5.com/en/docs/python_metatrader5/mt5initialize_py
- MetaQuotes symbol_info_tick：https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfotick_py
- MetaQuotes symbol_info：https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py
- MetaQuotes terminal_info：https://www.mql5.com/en/docs/python_metatrader5/mt5terminalinfo_py
- 官方 MetaTrader5 Python 包：https://pypi.org/project/MetaTrader5/
- PyInstaller 平台构建说明：https://pyinstaller.org/en/stable/operating-mode.html
