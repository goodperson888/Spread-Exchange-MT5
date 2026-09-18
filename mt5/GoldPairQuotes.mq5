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
   if(channel!=INVALID_HANDLE) SocketClose(channel);
   channel=INVALID_HANDLE;authenticated=false;ack="";
   previous_time=0;previous_bid=0;previous_ask=0;
}
bool SendLine(string line)
{
   if(channel==INVALID_HANDLE || !SocketIsConnected(channel)) {Disconnect();return false;}
   if(!SocketIsWritable(channel)) return false;
   uchar bytes[];
   int count=StringToCharArray(line+"\n",bytes,0,WHOLE_ARRAY,CP_UTF8)-1;
   if(SocketSend(channel,bytes,(uint)count)!=count) {Disconnect();return false;}
   return true;
}
void ReceiveAck()
{
   if(authenticated || channel==INVALID_HANDLE) return;
   uint ready=SocketIsReadable(channel);
   if(ready==0) return;
   uchar bytes[];
   int count=SocketRead(channel,bytes,MathMin(ready,128),10);
   if(count<=0) {Disconnect();return;}
   ack+=CharArrayToString(bytes,0,count,CP_UTF8);
   if(StringFind(ack,"OK\n")>=0) authenticated=true;
   if(StringLen(ack)>128) Disconnect();
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
   {Print("Generate the local EA setup in GoldPairLocal and load its .set file.");return INIT_PARAMETERS_INCORRECT;}
   EventSetTimer(1);
   Print("Quote-only EA started. Allow http://127.0.0.1 in Tools > Options > Expert Advisors.");
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
      if(channel==INVALID_HANDLE) return;
      SocketTimeouts(channel,10,10);
      if(!SocketConnect(channel,"127.0.0.1",(uint)LocalPort,100)) {Disconnect();return;}
      identity=Identity();
      if(!SendLine("{\"token\":\""+EscapeJSON(LocalToken)+"\","+identity+"}")) return;
   }
   ReceiveAck();
   if(authenticated)
   {
      PublishTick();
      SendLine("{\"type\":\"ping\"}");
   }
}
void OnDeinit(const int reason) {EventKillTimer();Disconnect();}
