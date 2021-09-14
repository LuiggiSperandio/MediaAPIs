import pandas as pd

from datetime import date, timedelta
from operator import itemgetter

from rtbhouse_sdk.reports_api import Conversions, ReportsApiSession
from tabulate import tabulate

#documentation1 = https://github.com/rtbhouse-apps/rtbhouse-python-sdk
#documentation2 = https://api.panel.rtbhouse.com/api/docs

caminho_arquivo = 'C:/BI/Atualizacao_Incremental/baseRTB1.csv'

print("API - RTB House")

## ADD RTB Account info
USER = ''
PASSWORD = ''

if __name__ == "__main__":
    api = ReportsApiSession(USER, PASSWORD)
    advertisers = api.get_advertisers()
    day_to = date.today()
    day_from = day_to - timedelta(days=30)
    group_by = ["day","subcampaign"]
    metrics = ["impsCount", "clicksCount", "campaignCost", "conversionsCount", "conversionsValue"]
    stats = api.get_rtb_stats(
        advertisers[0]["hash"],
        day_from,
        day_to,
        group_by,
        metrics,
        count_convention=Conversions.ATTRIBUTED_POST_CLICK,
    )
    columns = group_by + metrics
    data_frame = [
        [row[c] for c in columns]
        for row in reversed(sorted(stats, key=itemgetter("day","subcampaign")))
    ]
    result = (tabulate(data_frame, headers=columns))
    df= pd.DataFrame(data_frame, columns=columns)
    df.to_csv(caminho_arquivo, index=False)
    print(caminho_arquivo)
    print(result)





#       metrics = ["impsCount", "clicksCount", "campaignCost", "conversionsCount", "ctr"]
#       count_convention=Conversions.ATTRIBUTED_POST_CLICK,
#       group_by = ["day"]

#    day_to = date.today()
#    day_from = day_to - timedelta(days=30)

