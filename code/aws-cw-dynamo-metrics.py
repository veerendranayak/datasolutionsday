# Script to get the ConsumedCapacityUnits for all dynamodb tables in an account

# Author : Rajeev Thottathil 
# Email  : thottr@amazon.com

import boto3
from datetime import datetime
import sys

if (len(sys.argv) <= 2):
    print('Syntax : aws-cw-dynamo-metrics.py begindatetime enddatetime')
    print("Eg     : aws-cw-dynamo-metrics.py '2022/08/24 12:00' '2022/08/24 14:00'")
    quit()

# Create CloudWatch client
dynamodb = boto3.client('dynamodb')
cloudwatch = boto3.client('cloudwatch')

ddb_tables = {}

# Collect a list of all table names
paginator = dynamodb.get_paginator('list_tables')
for response in paginator.paginate():
        for table_name in response['TableNames']:
            ddb_tables[table_name] = {}

l_begin_time=sys.argv[1]
l_end_time=sys.argv[2]

# Iterate through the list of tables and get table level cw metrics
# Sum of Consumed Capacity Units are generated for 5 minute intervals.
# Limit the cloudwatch metric query time to the begin and end times

for t_tab in ddb_tables.keys():
      response = cloudwatch.get_metric_data(
          MetricDataQueries=[
              {
                  'Id': 'consumed_wcu',
                  'MetricStat': {
                      'Metric': {
                          'Namespace': 'AWS/DynamoDB',
                          'MetricName': 'ConsumedWriteCapacityUnits',
                          "Dimensions": [
                                 {
                                   "Name": 'TableName',
                                   "Value": t_tab
                                 }
                          ]
                      },
                      'Period': 300,
                      'Stat': 'Sum',
                      'Unit': 'Count'
                  }
              },
              {
                  'Id': 'consumed_rcu',
                  'MetricStat': {
                      'Metric': {
                          'Namespace': 'AWS/DynamoDB',
                          'MetricName': 'ConsumedReadCapacityUnits',
                          "Dimensions": [
                                 {
                                   "Name": 'TableName',
                                   "Value": t_tab
                                 }
                          ]
                      },
                      'Period': 300,
                      'Stat': 'Sum',
                      'Unit': 'Count'
                  }
              }
          ],
          StartTime=datetime.strptime(l_begin_time,"%Y/%m/%d %H:%M"),
          EndTime=datetime.strptime(l_end_time,"%Y/%m/%d %H:%M"),
          ScanBy='TimestampDescending',
          MaxDatapoints=2000
      )
      # Print metric values for each 5 min interval at table level
      for i in range(len(response['MetricDataResults'])):
          l_row_ctr=0
          for snaptime in response['MetricDataResults'][i]['Timestamps']:
              print('Table',t_tab,'NA',response['MetricDataResults'][i]['Label'],snaptime,response['MetricDataResults'][i]['Values'][l_row_ctr])
              l_row_ctr+=1

      # Iterate through each Global Secondary Index that exists on the table and print cloudwatch metric values
      gsiresponse = dynamodb.describe_table(TableName=t_tab)
      if 'GlobalSecondaryIndexes' in gsiresponse['Table']:
         for row in gsiresponse['Table']['GlobalSecondaryIndexes']:
             l_gsi=row['IndexName']
             gsimetricresponse = cloudwatch.get_metric_data(
                 MetricDataQueries=[
                     {
                         'Id': 'consumed_wcu',
                         'MetricStat': {
                             'Metric': {
                                 'Namespace': 'AWS/DynamoDB',
                                 'MetricName': 'ConsumedWriteCapacityUnits',
                                 "Dimensions": [
                                        {
                                          "Name": 'GlobalSecondaryIndexName',
                                          "Value": l_gsi
                                        }
                                 ]
                             },
                             'Period': 300,
                             'Stat': 'Sum',
                             'Unit': 'Count'
                         }
                     },
                     {
                         'Id': 'consumed_rcu',
                         'MetricStat': {
                             'Metric': {
                                 'Namespace': 'AWS/DynamoDB',
                                 'MetricName': 'ConsumedReadCapacityUnits',
                                 "Dimensions": [
                                        {
                                          "Name": 'GlobalSecondaryIndexName',
                                          "Value": l_gsi
                                        }
                                 ]
                             },
                             'Period': 300,
                             'Stat': 'Sum',
                             'Unit': 'Count'
                         }
                     }
                 ],
                 StartTime=datetime.strptime(l_begin_time,"%Y/%m/%d %H:%M"),
                 EndTime=datetime.strptime(l_end_time,"%Y/%m/%d %H:%M"),
                 ScanBy='TimestampDescending',
                 MaxDatapoints=2000
             )

             l_row_ctr=0
             for i in range(len(response['MetricDataResults'])):
                 l_row_ctr=0
                 for snaptime in response['MetricDataResults'][0]['Timestamps']:
                     print('Gsi',t_tab,l_gsi,response['MetricDataResults'][i]['Label'],snaptime,response['MetricDataResults'][i]['Values'][l_row_ctr])
                     l_row_ctr+=1
