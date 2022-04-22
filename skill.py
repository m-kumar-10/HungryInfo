import pandas as pd,datetime,glob,os,sys,re,sqlalchemy,numpy as np, matplotlib.pyplot as plt
import json 
%matplotlib inline

def login(IP_PORT_DB):
    engine=sqlalchemy.create_engine('mysql+pymysql://user_analytics:anaKm7Iv80l@172.10.112.'+str(IP_PORT_DB))
    return engine

def timeplot(dfr,col):
    dfr['Quarter'] = pd.PeriodIndex(dfr[col], freq='Q-Mar').strftime('Q%q')
    dfr['FY'] = pd.PeriodIndex(dfr[col], freq='A-Mar')
    dfr['FY']=dfr['FY'].apply(lambda y:str(y-1)+"-"+str(y-2000))
    dfr['MONTH']=dfr[col].dt.strftime('%b-%y')
    return dfr

def filter_action(dfr):
    dfr=dfr[(dfr['ACTION'].isin(['new','XML-new','qc-makelive'])) & (dfr['company_id'].isin(topcids)) & ~(dfr['file'].isin(private_dup_jobs['file']))].reset_index(drop=True)
    return dfr
#to break a list in chunks and iterate over it
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    
    
    def import_from_bi_bucket(path='',file_format='parquet'):
    dflist=[]
    import pandas as pd
    import s3fs
    import glob
    import boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(name="nau-bi-non-versioned")
    if path=='':
        return('please provide path of parent directory - such as: user/BI_Test/jobpulse/jj_final_dtype_correction/from_20190101/')
    else:
        try:
            FilesNotFound = True
            for obj in bucket.objects.filter(Prefix=path):
                if '.{}'.format(file_format) in obj.key:
            #         print('{0}:{1}'.format(bucket.name, obj.key))
                    FilesNotFound = False
                    if file_format=='parquet':
                        dflist.append(pd.read_parquet('s3://nau-bi-non-versioned/{}'.format(obj.key)))
                    elif file_format=='csv':
                        dflist.append(pd.read_csv('s3://nau-bi-non-versioned/{}'.format(obj.key)))
        except Exception as e:
            print(e)
        
        dflist=pd.concat(dflist)
        return dflist
    
def login(IP_PORT_DB):
    return sqlalchemy.create_engine('mysql+pymysql://user_analytics:anaKm7Iv80l@172.10.112.'+str(IP_PORT_DB))


''''''''''''''''''''''''''''''''''''''''''
def dtypes2(input):
    if input==None or input==[] or input=='[]':
        return None
    d= input
    d = d.strip('[]')
    start, end ,l = 0,0,[]
    for e, el in enumerate(d):
        if el=='}':
            start = end
            end = e+1
            word = d[start:end]
            word = word.strip(',')
            try:
                l.append(json.loads(word))
            except:
                0
    return l


def mapping(input):
    if input==None:
        return None
    store=[]
    for d in input:
        try:
            if d['type']=='skill':
                store.append(d['value'])
        except:
            pass
    return store


dictionary = {}
def hist(input):
    if input==None:
        pass
    else:
        for skill in input:
            dictionary[skill]=dictionary.get(skill,0)+1
# Loading DataFrame.
%%time
ef = import_from_bi_bucket('user/BI_Manish/Adv_Search_Click/search_parameters_60_days/')


ef.rename(columns={'EZ_KEYWORD_ALL':'skill'}, inplace=True)

ef.dropna(subset=['skill'],axis=0, inplace=True)

"""We have approx 20 lakh search results"""
ef['skills'] = ef['skill'].map(dtypes2,None)

ef['len'] = ef['skills'].apply(lambda x: 0 if(x==None) else len(x))

jf = ef[ef['len']>=1]

jf.reset_index(drop=True, inplace=True)


jf['skills_srch'] = jf.skills.map(mapping,None)
dictionary = {}
jf['skills_srch'].map(hist,None)


final = pd.DataFrame({'skills':dictionary.keys(),
                    'counts':dictionary.values()})
final.sort_values(['counts'], ascending=False).head()



print(final.shape)

top_10 = final.sort_values(['counts'], ascending=False).head(10).reset_index(drop=True)
print(top_10)


total_srchs = ef.shape[0]
print(total_srchs)

#Total Searches with skills.......
print(jf.shape[0])



#Searches with skill in corresponding 3 months.
jf['skills_counts'] = jf['skills_srch'].apply(lambda x:len(x))

jf['dtm'] = jf['created_on'].apply(lambda x:x[0:6])

jf.groupby(['dtm']).sum() ## dtm is in format of year-month, and the actual total sum of skill searches in that
                            # month is given in skills_counts columns.
                            
                            
                            
# DataFrame name is <final>

#Now we'r gonna plot our data using library named <plotly> which give us interactive graphs...............

import plotly.express as px
fig = px.histogram(final,
                   x='skills',
                   y='counts',
                   title='Distribution of Skills',
                   log_y=True,
                   opacity=0.9,
                   labels={'Total Skills_search':sum(vlu)},
                   color_discrete_sequence=['#FF8787']).update_xaxes(categoryorder='total ascending')

from plotly.offline import init_notebook_mode
init_notebook_mode(connected=True)
fig.show()



"""Time for new dataframe."""""

march = import_from_bi_bucket('user/BI_Manish/Adv_Search_Click/march_data/')

print(march.shape)

march.dropna(subset=['All', 'Any', 'Exc'], axis=0, how='all')

march.head()



march.drop(['Any'], axis=1, inplace=True)

march.Exc.nunique()


%%time
march['Exc_'] = march.Exc.map(dtypes2,None)

march.drop(['Exc'],axis=1,inplace=True)

march['Exc_skill'] = march.Exc_.map(mapping,None)

march['Exc_skill_count'] = march.Exc_skill.apply(lambda x: 0 if (x==None) else len(x))

march['All_'] = march.All.map(dtypes2,None)

march['All_skill'] = march.All_.map(mapping,None)

march['All_skill_counts'] = march.All_skill.apply(lambda x: 0 if (x==None) else len(x))

march.drop(['Exc_','All_','All'], axis=1, inplace=True)



%%time
dictionary ={}
march.All_skill.map(hist,None)
all_skill = list(dictionary.keys())


dictionary={}
march.Exc_skill.map(hist,None)
Exc_skill = list(dictionary.keys())


total_skill = Exc_skill+all_skill
total_skill = set(total_skill)

all_skill_count ={}
for x in total_skill:
    all_skill_count[x] = dictionary.get(x,0)
    
exc_skill_count ={}
for x in total_skill:
    exc_skill_count[x] = dictionary.get(x,0)
    
total = list(total_skill)
march_summary = pd.DataFrame({'Skills':total,
                              'All_skill_count':list(all_skill_count.values()),
                              'Exc_skill_count':list(exc_skill_count.values())})


print(march_summary.tail())

import plotly.express as px
fig = px.scatter(march_summary, x='Skills',y = ['All_skill_count','Exc_skill_count'], log_y=True)


# Let's save this data summary locally....
march.to_csv('march.csv')
