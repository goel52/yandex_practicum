import datetime
import time
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import psycopg2
from sqlalchemy import create_engine
import psycopg2
import io
import os
import requests
import pandas as pd





args = {
    'owner': 'oleg',
    'start_date': datetime.datetime(2021, 11, 1),
    'provide_context': True
}



start_hour = 1
horizont_hours = 48

lat = 47.939
lng = 46.681
moscow_timezone = 3
local_timezone = 4


def extract_data(**kwargs):
    ti = kwargs['ti']
    url = 'https://api.exchangerate.host/latest'
    response = requests.get(url, verify=False)
    data = response.json()
    ti.xcom_push(key='ubs', value=data)



def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='ubs', task_ids=['extract_data'])[0]
    is_success = data["success"]
    base = data["base"]
    date = data["date"]
    rates = data['rates']
    df = pd.DataFrame()
    df["id"] = 1
    df["base"] = [base]
    df["date"] = [date]
    coll = list(rates.keys())
    vall = list(rates.values())
    #можно было сделать df[coll] = vall. Но на более старых версиях питона не работает. Поэтому пробежимся циклом
    for c,v in zip(coll,vall):
        df[c] = v
    df.rename(columns={"ALL": "ALL_"})
    print(df)
    ti.xcom_push(key='ubs', value=df)






def load_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0]
    print("PPPPPPpp")
    print(df)







with DAG('ubs', description='ubs', schedule_interval='*/1 * * * *', catchup=False,
         default_args=args) as dag:  # 0 * * * *   */1 * * * *
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load_data = PythonOperator(task_id='load_data', python_callable=load_data)
    create_cloud_table = PostgresOperator(
        task_id="create_clouds_value_table",
        postgres_conn_id="database_PG",
        sql="""
                                CREATE TABLE IF NOT EXISTS ubs (
                                base text NOT NULL,
                                date text NOT NULL,
                                AED FLOAT NOT NULL,
                                AFN FLOAT NOT NULL,
                                ALL_ FLOAT NOT NULL,
                                AMD FLOAT NOT NULL,
                                ANG FLOAT NOT NULL,
                                AOA FLOAT NOT NULL,
                                ARS FLOAT NOT NULL,
                                AUD FLOAT NOT NULL,
                                AWG FLOAT NOT NULL,
                                AZN FLOAT NOT NULL,
                                BAM FLOAT NOT NULL,
                                BBD FLOAT NOT NULL,
                                BDT FLOAT NOT NULL,
                                BGN FLOAT NOT NULL,
                                BHD FLOAT NOT NULL,
                                BIF FLOAT NOT NULL,
                                BMD FLOAT NOT NULL,
                                BND FLOAT NOT NULL,
                                BOB FLOAT NOT NULL,
                                BRL FLOAT NOT NULL,
                                BSD FLOAT NOT NULL,
                                BTC FLOAT NOT NULL,
                                BTN FLOAT NOT NULL,
                                BWP FLOAT NOT NULL,
                                BYN FLOAT NOT NULL,
                                BZD FLOAT NOT NULL,
                                CAD FLOAT NOT NULL,
                                CDF FLOAT NOT NULL,
                                CHF FLOAT NOT NULL,
                                CLF FLOAT NOT NULL,
                                CLP FLOAT NOT NULL,
                                CNH FLOAT NOT NULL,
                                CNY FLOAT NOT NULL,
                                COP FLOAT NOT NULL,
                                CRC FLOAT NOT NULL,
                                CUC FLOAT NOT NULL,
                                CUP FLOAT NOT NULL,
                                CVE FLOAT NOT NULL,
                                CZK FLOAT NOT NULL,
                                DJF FLOAT NOT NULL,
                                DKK FLOAT NOT NULL,
                                DOP FLOAT NOT NULL,
                                DZD FLOAT NOT NULL,
                                EGP FLOAT NOT NULL,
                                ERN FLOAT NOT NULL,
                                ETB FLOAT NOT NULL,
                                EUR_ FLOAT NOT NULL,
                                FJD FLOAT NOT NULL,
                                FKP FLOAT NOT NULL,
                                GBP FLOAT NOT NULL,
                                GEL FLOAT NOT NULL,
                                GGP FLOAT NOT NULL,
                                GHS FLOAT NOT NULL,
                                GIP FLOAT NOT NULL,
                                GMD FLOAT NOT NULL,
                                GNF FLOAT NOT NULL,
                                GTQ FLOAT NOT NULL,
                                GYD FLOAT NOT NULL,
                                HKD FLOAT NOT NULL,
                                HNL FLOAT NOT NULL,
                                HRK FLOAT NOT NULL,
                                HTG FLOAT NOT NULL,
                                HUF FLOAT NOT NULL,
                                IDR FLOAT NOT NULL,
                                ILS FLOAT NOT NULL,
                                IMP FLOAT NOT NULL,
                                INR FLOAT NOT NULL,
                                IQD FLOAT NOT NULL,
                                IRR FLOAT NOT NULL,
                                ISK FLOAT NOT NULL,
                                JEP FLOAT NOT NULL,
                                JMD FLOAT NOT NULL,
                                JOD FLOAT NOT NULL,
                                JPY FLOAT NOT NULL,
                                KES FLOAT NOT NULL,
                                KGS FLOAT NOT NULL,
                                KHR FLOAT NOT NULL,
                                KMF FLOAT NOT NULL,
                                KPW FLOAT NOT NULL,
                                KRW FLOAT NOT NULL,
                                KWD FLOAT NOT NULL,
                                KYD FLOAT NOT NULL,
                                KZT FLOAT NOT NULL,
                                LAK FLOAT NOT NULL,
                                LBP FLOAT NOT NULL,
                                LKR FLOAT NOT NULL,
                                LRD FLOAT NOT NULL,
                                LSL FLOAT NOT NULL,
                                LYD FLOAT NOT NULL,
                                MAD FLOAT NOT NULL,
                                MDL FLOAT NOT NULL,
                                MGA FLOAT NOT NULL,
                                MKD FLOAT NOT NULL,
                                MMK FLOAT NOT NULL,
                                MNT FLOAT NOT NULL,
                                MOP FLOAT NOT NULL,
                                MRO FLOAT NOT NULL,
                                MRU FLOAT NOT NULL,
                                MUR FLOAT NOT NULL,
                                MVR FLOAT NOT NULL,
                                MWK FLOAT NOT NULL,
                                MXN FLOAT NOT NULL,
                                MYR FLOAT NOT NULL,
                                MZN FLOAT NOT NULL,
                                NAD FLOAT NOT NULL,
                                NGN FLOAT NOT NULL,
                                NIO FLOAT NOT NULL,
                                NOK FLOAT NOT NULL,
                                NPR FLOAT NOT NULL,
                                NZD FLOAT NOT NULL,
                                OMR FLOAT NOT NULL,
                                PAB FLOAT NOT NULL,
                                PEN FLOAT NOT NULL,
                                PGK FLOAT NOT NULL,
                                PHP FLOAT NOT NULL,
                                PKR FLOAT NOT NULL,
                                PLN FLOAT NOT NULL,
                                PYG FLOAT NOT NULL,
                                QAR FLOAT NOT NULL,
                                RON FLOAT NOT NULL,
                                RSD FLOAT NOT NULL,
                                RUB FLOAT NOT NULL,
                                RWF FLOAT NOT NULL,
                                SAR FLOAT NOT NULL,
                                SBD FLOAT NOT NULL,
                                SCR FLOAT NOT NULL,
                                SDG FLOAT NOT NULL,
                                SEK FLOAT NOT NULL,
                                SGD FLOAT NOT NULL,
                                SHP FLOAT NOT NULL,
                                SLL FLOAT NOT NULL,
                                SOS FLOAT NOT NULL,
                                SRD FLOAT NOT NULL,
                                SSP FLOAT NOT NULL,
                                STD FLOAT NOT NULL,
                                STN FLOAT NOT NULL,
                                SVC FLOAT NOT NULL,
                                SYP FLOAT NOT NULL,
                                SZL FLOAT NOT NULL,
                                THB FLOAT NOT NULL,
                                TJS FLOAT NOT NULL,
                                TMT FLOAT NOT NULL,
                                TND FLOAT NOT NULL,
                                TOP FLOAT NOT NULL,
                                TRY FLOAT NOT NULL,
                                TTD FLOAT NOT NULL,
                                TWD FLOAT NOT NULL,
                                TZS FLOAT NOT NULL,
                                UAH FLOAT NOT NULL,
                                UGX FLOAT NOT NULL,
                                USD FLOAT NOT NULL,
                                UYU FLOAT NOT NULL,
                                UZS FLOAT NOT NULL,
                                VES FLOAT NOT NULL,
                                VND FLOAT NOT NULL,
                                VUV FLOAT NOT NULL,
                                WST FLOAT NOT NULL,
                                XAF FLOAT NOT NULL,
                                XAG FLOAT NOT NULL,
                                XAU FLOAT NOT NULL,
                                XCD FLOAT NOT NULL,
                                XDR FLOAT NOT NULL,
                                XOF FLOAT NOT NULL,
                                XPD FLOAT NOT NULL,
                                XPF FLOAT NOT NULL,
                                XPT FLOAT NOT NULL,
                                YER FLOAT NOT NULL,
                                ZAR FLOAT NOT NULL,
                                ZMW FLOAT NOT NULL,
                                ZWL FLOAT NOT NULL);
                            """,
    )

    insert_in_table = PostgresOperator(
        task_id="insert_clouds_table",
        postgres_conn_id="database_PG",
        sql=[f"""INSERT INTO ubs VALUES(
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['base']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['date']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AED']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AFN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ALL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AMD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ANG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AOA']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ARS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AUD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AWG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['AZN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BAM']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BBD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BDT']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BGN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BHD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BIF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BMD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BND']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BOB']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BRL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BSD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BTC']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BTN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BWP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BYN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['BZD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CAD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CDF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CHF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CLF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CLP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CNH']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CNY']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['COP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CRC']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CUC']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CUP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CVE']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['CZK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['DJF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['DKK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['DOP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['DZD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['EGP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ERN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ETB']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['EUR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['FJD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['FKP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GBP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GEL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GGP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GHS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GIP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GMD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GNF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GTQ']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['GYD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['HKD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['HNL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['HRK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['HTG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['HUF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['IDR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ILS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['IMP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['INR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['IQD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['IRR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ISK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['JEP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['JMD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['JOD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['JPY']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KES']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KGS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KHR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KMF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KPW']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KRW']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KWD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KYD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['KZT']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LAK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LBP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LKR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LRD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LSL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['LYD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MAD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MDL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MGA']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MKD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MMK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MNT']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MOP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MRO']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MRU']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MUR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MVR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MWK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MXN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MYR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['MZN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NAD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NGN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NIO']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NOK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NPR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['NZD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['OMR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PAB']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PEN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PGK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PHP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PKR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PLN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['PYG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['QAR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['RON']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['RSD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['RUB']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['RWF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SAR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SBD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SCR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SDG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SEK']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SGD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SHP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SLL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SOS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SRD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SSP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['STD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['STN']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SVC']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SYP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['SZL']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['THB']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TJS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TMT']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TND']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TOP']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TRY']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TTD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TWD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['TZS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['UAH']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['UGX']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['USD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['UYU']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['UZS']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['VES']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['VND']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['VUV']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['WST']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XAF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XAG']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XAU']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XCD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XDR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XOF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XPD']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XPF']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['XPT']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['YER']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ZAR']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ZMW']}}}}',
                                '{{{{ti.xcom_pull(key='ubs', task_ids=['transform_data'])[0].iloc[0]['ZWL']}}}}')
                            """ ]
    )

    extract_data >> transform_data >> load_data >> create_cloud_table >> insert_in_table