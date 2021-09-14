import pandas as pd
import facebookads
import facebook_business

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

print('FacebookAds1')


## ADD Facebook Account Info
access_token = 
ad_account_id = 
app_secret = 
app_id = 
FacebookAdsApi.init(access_token=access_token)

fields = [
    'campaign_id',
    'campaign_name',
    'adset_id',
    'adset_name',
    'impressions',
    'clicks',
    'unique_clicks',
    'action_values',
    'spend',
]
params = {
    #'time_range': {'since':'2021-08-28','until':'2021-08-31'},
    'date_preset':'last_28d',
    'filtering': [{'field':'action_type','operator':'IN','value':['omni_purchase']}],
    'level': 'adset',
    'breakdowns': [],
    'time_increment':'1',
    'action_breakdowns': ['action_type']
}

results=(AdAccount(ad_account_id).get_insights(
    fields=fields,
    params=params,
    ))

df= pd.DataFrame(list(results))

df.to_csv('C:/BI/Atualizacao_Incremental/baseFacebookAds_1.csv', index=False)
