import pandas as pd

from datetime import date, timedelta
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

print('GA-baseGA')

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = ## ADD json service account file here
VIEW_ID = ## ADD viewID for G.A. account

caminho_arquivo='C:/BI/Atualizacao_Incremental/baseGoogleAds_GROUP_1.csv'

dia_final = str(date.today() - timedelta(days=20))

def initialize_analyticsreporting():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)
  analytics = build('analyticsreporting', 'v4', credentials=credentials)
  return analytics

##### GET report
##### Documentação de metricas e dimensões:     https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
def get_report(analytics, pageTokenVar):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': dia_final, 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:impressions'}, {'expression': 'ga:adClicks'}, {'expression': 'ga:adcost'}, {'expression': 'ga:transactionsPerSession'}, {'expression': 'ga:sessions'}, {'expression': 'ga:transactionRevenue'}, {'expression': 'ga:transactions'}, {'expression': 'ga:ROAS'}, {'expression': 'ga:costPerTransaction'}, {'expression': 'ga:costPerConversion'}],
          'dimensions': [{'name': 'ga:date'}, {'name': 'ga:adwordsCampaignID'}, {'name': 'ga:campaign'}, {'name': 'ga:adwordsCreativeID'}, {'name': 'ga:adGroup'}, {'name': 'ga:adwordsAdGroupID'}],
          'pageSize': 1000000,
          'pageToken': pageTokenVar,
          'samplingLevel': 'LARGE',
          
        }    
      ],
      'useResourceQuotas': 'true',
    }
  ).execute()
    
def handle_report(analytics,pagetoken,rows):  
    response = get_report(analytics, pagetoken)

    #Header, Headers Dimensões, Headers Metricas 
    columnHeader = response.get("reports")[0].get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    #Paginação
    pagetoken = response.get("reports")[0].get('nextPageToken', None)
    
    #Linhas
    rowsNew = response.get("reports")[0].get('data', {}).get('rows', [])
    rows = rows + rowsNew
    print("len(rows): " + str(len(rows)))

    #Query prox pagina
    if pagetoken != None:
        return handle_report(analytics,pagetoken,rows)
    else:
        #nicer results
        nicerows=[]
        for row in rows:
            dic={}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            for header, dimension in zip(dimensionHeaders, dimensions):
                dic[header] = dimension

            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
                    if ',' in value or ',' in value:
                        dic[metric.get('name')] = float(value)
                    else:
                        dic[metric.get('name')] = float(value)
            nicerows.append(dic)
        return nicerows

# Pau no gato
def main():    
    analytics = initialize_analyticsreporting()
    
    global dfanalytics
    dfanalytics = []

    rows = []
    rows = handle_report(analytics,'0',rows)

    dfanalytics = pd.DataFrame(list(rows))

if __name__ == '__main__':
  main()

dfanalytics.to_csv(caminho_arquivo, index=False)

print(caminho_arquivo)
