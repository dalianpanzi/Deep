import pandas as pd
import numpy as np
import datetime

df_air=pd.read_excel(r'E:...xls')
df_air.info()
df_cold['时间']=pd.to_datetime(df_cold['时间'])
df_air=df_air.drop_duplicates(subset=['计量表','时间'],keep='first')#删除重复数据

res={}
for a in df_all["时间"]:
    if a in res:
        res[a]=res[a]+1#计数每个时间点出现次数
    else:
        res[a]=1
        
tar=2+11+3
#for循环不能直接在res上，否则删除数据，循环会出错要在list上
for c in list(res):
    if res[c]<tar:
        res.pop(c)   
t=list(res.keys()) 

def every_var(df,t):
    see=[]
    for time in t:
        see.append(df[df['时间']==time])
    see=np.array(see)
    see=see.reshape(see.shape[0],-1)
    return see

#计算时间间隔在设定值以上或以下
a=datetime.timedelta(days=0,hours=3)
split={}
for i in range(len(t)-1):
    if( t[i+1]-t[i])> a :
        split[i+1]=(t[i+1]-t[i])
inter={}
in_b=datetime.timedelta(minutes=15)
for i in range(len(t)-1):
    if (( t[i+1]-t[i]) <= a) & (t[i+1]-t[i] > in_b):
        inter[i+1]=(t[i+1]-t[i]) 

#
def give_num(array):
    len_col=array.shape[1]
    n_var=pd.DataFrame(array[:,1],columns=['time'])
    for i in range(2,len_col,3):#3是因为有重复的time和序号，不取重
        ll=pd.DataFrame(array[:,i],columns=[i])
        ll=ll[i].apply(lambda x:int(x))   #数据转int 转完是int64
        n_var=pd.concat((n_var,ll),axis=1)    
    vill_var=[]
    nl_col=n_var.columns
    for j in range(1,len(nl_col)):
        vill=[]
        for k in range(1,len(n_var)-1):
        #相邻值差距过大，且后一行不为0
            if (2*n_var[nl_col[j]][k]<n_var[nl_col[j]][k+1] or n_var[nl_col[j]][k]>2*n_var[nl_col[j]][k+1]) and (n_var[nl_col[j]][k+1]!=0):
                vill.append(k)
            elif n_var[nl_col[j]][k]==0:
                vill.append(k)
        vill_var.append(vill)
    return n_var,vill_var#vill_var原来问题数据的行号
#去问题数据
def del_vill(list_vill):
  for y in range(len(list_vill)):
      for a in list_vill[y]:
          if a in split:
              list_vill[y].remove(a)
  return list_vill

#算差值
def re_df(df,vill_df):
    real_df=df.copy()
    for y in range(1,len(df.columns)):
        for a in range(1,len(df)):
            if a not in vill_df[y-1] and a not in split:
                real_df[df.columns[y]][a]=df[df.columns[y]][a] - df[df.columns[y]][a-1] 
    return real_df    

#插值，时间间隔是15min 
def inter_df(real_df):
    lenc=len(real_df.columns)
    for x in inter:
        lenn=len(real_df)
        helper=pd.DataFrame({'time':pd.date_range(start=t[x-1],end=t[x],freq='0h15min')})
        num=len(helper)-1
        real_df=pd.merge(real_df,helper,on='time',how='outer')
        for xx in range(1,lenc):
            hh=(real_df[real_df.columns[xx]][x])/num
            real_df[real_df.columns[xx]][lenn:]=hh
            real_df[real_df.columns[xx]][x]=hh
    return real_df

real_air=real_air.sort_values('time')#按time排序

def spl(real_df):
    spl_list=[]
    for s in split:
        a=real_df[real_df['index']==s].index.item()
        if spl_list==[]:
            spl_list.append(real_df.iloc[:a,])
        else:
            ff=spl_list[-1]
            b=ff.tail(1).index.item() + 1
            spl_list.append(real_df.iloc[b:a,])
    return spl_list
#mean=平均值
def prepare(spl_df,mean_df):
    spli_df=[]
    for a in spl_df:
        if len(a)>8:
            a.reset_index(inplace=True)
            c=a.columns
            cc=len(c)
            for col in range(3,cc):
                a[c[col]][0]=a[c[col]][1]
            a=a[a[c[col]] != 0]
            a=a[a[c[col]] < 2*mean_df[c[col]]]
            if len(a)>8:
                spli_df.append(a)                  
    return spli_df

def rere(spli_df,var):
    rr_df=[]
    for i in range(len(spli_df)):
        if i not in var:
            rr_df.append(spli_df[i])
    return rr_df

air,cold,elec=[],[],[]#做两次
for i in range(len(rr_air)):
    if len(rr_elec[i])<len(rr_air[i]) and len(rr_elec[i])<len(rr_cold[i]):
        elec.append(i)
    else:
        if len(rr_air[i])<len(rr_cold[i]):
            air.append(i)
        else:
            cold.append(i)

delt=datetime.timedelta(minutes=30)
s_air={}    
for i in air:
    ss=[]
    s=rr_air[i]
    for x in range(1,len(s)):
        if s['time'].iloc[x]-s['time'].iloc[x-1] > delt:
            ss.append(x)
    s_air[i]=ss 
for a in list(s_air.keys()):
    if s_air[a]==[]:
        del s_air[a]

def same(df,target,df_target):
    for r in target:
        for y in list(df[r]['time'].values):
            if y not in df_target[r]['time'].values:
                df[r]=df[r].drop(df[r][df[r]['time']==y].index)
    return df

for y in list(rr_air[0]['time'].values):
    if y not in rr_cold[0]['time'].values:
        rr_air[0]=rr_air[0].drop(rr_air[0][rr_air[0]['time']==y].index)

def prepare_data(data):
    x_train=[]
    y_train=[]    
    databatch = 8
    for ff in data:
        x_list=[]
        y_list=[]
        for i in range(len(ff)-databatch-1):
            x_list.append(ff[i:i+databatch])
            y_list.append(ff.iloc[i+databatch])
        x_list=np.array(x_list)
        y_list=np.array(y_list)
        x_train.append(x_list)
        y_train.append(y_list)
    return x_train,y_train

def x(train):
    Xatr=train[0]
    for i in range(1,len(train)-1):
        Xatr=np.concatenate((Xatr, train[i]))
    return Xatr





