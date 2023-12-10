# импортируем библиотеки
import pandas as pd
import numpy as np
# читаем csv файлов с данными
ads = pd.read_csv('ads.csv')
leads = pd.read_csv('leads.csv')
purchases = pd.read_csv('purchases.csv')
# меняем типы данных
ads['created_at'] = pd.to_datetime(ads['created_at'])
leads['lead_created_at'] = pd.to_datetime(leads['lead_created_at'])
purchases['purchase_created_at'] = pd.to_datetime(purchases['purchase_created_at'])
ads['d_utm_campaign'] = ads['d_utm_campaign'].astype(str)
ads['d_utm_content'] = ads['d_utm_content'].astype(str)
ads['d_utm_term'] = ads['d_utm_term'].astype(str)
leads['d_lead_utm_content'] = leads['d_lead_utm_content'].astype(str)
leads['d_lead_utm_term'] = leads['d_lead_utm_term'].astype(str)
# объединяем рекламу и лиды по дате и utm меткам
leads_ya = ads.merge(leads,  
                     left_on=[
                         'created_at',
                         'd_utm_source', 
                         'd_utm_medium', 
                         'd_utm_campaign', 
                         'd_utm_content', 
                         'd_utm_term'], 
                     right_on=[
                         'lead_created_at',
                         'd_lead_utm_source', 
                         'd_lead_utm_medium', 
                         'd_lead_utm_campaign', 
                         'd_lead_utm_content', 
                         'd_lead_utm_term'])
# объединяем лиды и продажи
purchases_ya = leads_ya.dropna(subset=['client_id']).merge(purchases, how='inner', on='client_id')
# считаем количество дней между заявкой и продажей
purchases_ya['day'] = (purchases_ya['purchase_created_at'] - purchases_ya['lead_created_at']).dt.days
# фильтруем датасет по количеству дней
purchases_ya = purchases_ya.query('0 <= day < 15')
# выбираем лиды ближайшие к продаже
purchases_min_day = purchases_ya.groupby(['lead_id'])['day'].min().reset_index().merge(purchases_ya, how='left', on=['lead_id', 'day'])
# считаем количество продаж и сумму выручки по лидам
sum_purchases = purchases_min_day.groupby('lead_id').agg({'purchase_id': 'count', 'm_purchase_amount': 'sum'}).reset_index()
# объединяем лиды с количество продаж и сумму выручки по лидам
sum_leads_ya = leads_ya.merge(sum_purchases, how='left', on='lead_id')
# считаем количество лидов продаж и сумму выручки по дате и utm меткам
count_leads = sum_leads_ya.groupby([
    'created_at', 
    'd_utm_source', 
    'd_utm_medium', 
    'd_utm_campaign'
    ]).agg({
        'lead_id': 'count', 
        'purchase_id': 'count', 
        'm_purchase_amount': 'sum'
        }).reset_index()
# считаем количество кликов и затраты по дате и utm меткам
sum_ads = ads.groupby([
    'created_at', 
    'd_utm_source', 
    'd_utm_medium', 
    'd_utm_campaign'
    ]).agg({
        'm_clicks': 'sum', 
        'm_cost': 'sum'
        }).reset_index()
# объединяем рекламу и лиды по дате и utm меткам
report = sum_ads.merge(count_leads, how='left', on=['created_at', 'd_utm_source', 'd_utm_medium', 'd_utm_campaign']).fillna(0)
# считаем CPL 
report['CPL'] = (report['m_cost']/report['lead_id']).round(2).replace(np.inf, np.nan)
# считаем ROAS
report['ROAS '] = (report['m_purchase_amount']/report['m_cost']).round(2).replace(np.inf, np.nan)
# меняем типы данных  
report['m_clicks'] = report['m_clicks'].astype(int)
report['lead_id'] = report['lead_id'].astype(int)
report['purchase_id'] = report['purchase_id'].astype(int)
report['m_cost'] = report['m_cost'].round(2)
# меняем названия столбцов в соответстви с ТЗ
report.columns = [
    'Дата', 
    'UTM source', 
    'UTM medium', 
    'UTM campaign', 
    'Количество кликов', 
    'Расходы на рекламу', 
    'Количество лидов', 
    'Количество покупок', 
    'Выручка от продаж', 
    'CPL', 
    'ROAS']
# записываем результат в файл
report.to_excel('report.xlsx')