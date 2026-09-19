#property strict
#property version "1.00"
#property description "GoldPairLocal quote-only local bridge. Does not place or modify orders."

input int LocalPort=8767;
input string LocalToken="";
int channel=INVALID_HANDLE;
bool authenticated=false;
ulong sequence=0;
long previous_time=0;
double previous_bid=0,previous_ask=0;
string identity="",ack="";
bool hello_sent=false;

string EscapeJSON(string s)
{
   StringReplace(s,"\\","\\\\");
   StringReplace(s,"\"","\\\"");
   StringReplace(s,"\r","\\r");
   StringReplace(s,"\n","\\n");
   StringReplace(s,"\t","\\t");
   return s;
}
string Identity()
{
   return "\"account\":\""+IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN))+"\",\"server\":\""+
      EscapeJSON(AccountInfoString(ACCOUNT_SERVER))+"\",\"symbol\":\""+EscapeJSON(_Symbol)+"\"";
}
void Disconnect()
{
   if(channel!=INVALID_HANDLE)
      Print("GoldPairQuotes: 本机行情连接已断开，等待自动重连。");
   if(channel!=INVALID_HANDLE) SocketClose(channel);
   channel=INVALID_HANDLE;authenticated=false;ack="";hello_sent=false;
   previous_time=0;previous_bid=0;previous_ask=0;
}
bool SendLine(string line)
{
   if(channel==INVALID_HANDLE || !SocketIsConnected(channel))
   {
      PrintFormat("GoldPairQuotes: 发送前发现 Socket 未连接，错误码=%d。",GetLastError());
      Disconnect();return false;
   }
   if(!SocketIsWritable(channel)) return false;
   uchar bytes[];
   int count=StringToCharArray(line+"\n",bytes,0,WHOLE_ARRAY,CP_UTF8)-1;
   if(SocketSend(channel,bytes,(uint)count)!=count)
   {
      PrintFormat("GoldPairQuotes: SocketSend 失败，错误码=%d。",GetLastError());
      Disconnect();return false;
   }
   return true;
}
void ReceiveAck()
{
   if(authenticated || channel==INVALID_HANDLE) return;
   uint ready=SocketIsReadable(channel);
   if(ready==0) return;
   uchar bytes[];
   int count=SocketRead(channel,bytes,MathMin(ready,128),10);
   if(count<=0)
   {
      PrintFormat("GoldPairQuotes: 读取本机行情接收器响应失败，错误码=%d。",GetLastError());
      Disconnect();return;
   }
   ack+=CharArrayToString(bytes,0,count,CP_UTF8);
   if(StringFind(ack,"OK\n")>=0)
   {
      authenticated=true;
      PrintFormat("GoldPairQuotes: 本机行情接收器已确认（127.0.0.1:%d），开始推送 MT5 报价。",LocalPort);
   }
   if(StringLen(ack)>128)
   {
      Print("GoldPairQuotes: 本机行情接收器响应无法识别，连接已重置。");
      Disconnect();
   }
}
void PublishTick()
{
   if(!TerminalInfoInteger(TERMINAL_CONNECTED)) {Disconnect();return;}
   if(Identity()!=identity) {Disconnect();return;}
   ReceiveAck();
   if(!authenticated) return;
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol,tick) || tick.bid<=0 || tick.ask<tick.bid) return;
   if(tick.time_msc==previous_time && tick.bid==previous_bid && tick.ask==previous_ask) return;
   sequence++;
   string data="{\"type\":\"tick\","+identity+",\"seq\":"+IntegerToString((long)sequence)+
      ",\"bid\":"+DoubleToString(tick.bid,10)+",\"ask\":"+DoubleToString(tick.ask,10)+
      ",\"time_ms\":"+IntegerToString(tick.time_msc)+"}";
   if(SendLine(data)) {previous_time=tick.time_msc;previous_bid=tick.bid;previous_ask=tick.ask;}
}
int OnInit()
{
   if(StringLen(LocalToken)<32 || LocalPort<1024 || LocalPort>65535)
   {
      PrintFormat("GoldPairQuotes 参数无效：LocalPort=%d，LocalToken 长度=%d。请在网页点击‘准备 EA 与本机参数’，在 EA 输入中点击‘加载’选择 GoldPairQuotes.set；不要直接挂载未配置的 ex5。",LocalPort,StringLen(LocalToken));
      return INIT_PARAMETERS_INCORRECT;
   }
   EventSetTimer(1);
   PrintFormat("GoldPairQuotes: EA 已初始化，账号=%I64d，服务器=%s，品种=%s，端口=%d。等待连接本机行情接收器。",
      AccountInfoInteger(ACCOUNT_LOGIN),AccountInfoString(ACCOUNT_SERVER),_Symbol,LocalPort);
   Print("GoldPairQuotes: 如连接失败，请确认网页应用与 MT5 在同一台 Windows 电脑运行，并允许访问 127.0.0.1。");
   return INIT_SUCCEEDED;
}
void OnTick() { PublishTick(); }
void OnTimer()
{
   // Reconnection runs on the timer, never in the price callback.
   if(channel!=INVALID_HANDLE && (!SocketIsConnected(channel) || identity!=Identity())) Disconnect();
   if(channel==INVALID_HANDLE && TerminalInfoInteger(TERMINAL_CONNECTED))
   {
      channel=SocketCreate();
      if(channel==INVALID_HANDLE)
      {
         PrintFormat("GoldPairQuotes: SocketCreate 失败，错误码=%d。",GetLastError());
         return;
      }
      SocketTimeouts(channel,10,10);
      if(!SocketConnect(channel,"127.0.0.1",(uint)LocalPort,100))
      {
         int socket_error=GetLastError();
         if(socket_error==4014)
            Print("GoldPairQuotes: SocketConnect 被 MT5 拒绝，错误码=4014。请在工具→选项→EA交易中勾选‘允许 WebRequest 请求下列 URL’，添加 http://127.0.0.1，然后重新加载 EA。");
         else
            PrintFormat("GoldPairQuotes: SocketConnect 127.0.0.1:%d 失败，错误码=%d。请确认网页应用在同一台 Windows 电脑运行且端口正在监听。",LocalPort,socket_error);
         Disconnect();return;
      }
      PrintFormat("GoldPairQuotes: TCP 已连接到 127.0.0.1:%d，正在验证 LocalToken。",LocalPort);
      identity=Identity();
      hello_sent=SendLine("{\"token\":\""+EscapeJSON(LocalToken)+"\","+identity+"}");
      if(!hello_sent)
      {
         Print("GoldPairQuotes: LocalToken 握手发送失败。");
         return;
      }
   }
   ReceiveAck();
   if(authenticated)
   {
      PublishTick();
      SendLine("{\"type\":\"ping\"}");
   }
}
void OnDeinit(const int reason) {EventKillTimer();Disconnect();}
