import pandas as pd
import requests
import toloka.client as toloka
import os
from tqdm import tqdm


URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

start_check_date = '2022-12-01'
finish_check_date = '2023-01-10'

feedback_files_dir = ''

list_of_projects = []
list_of_pools = []

full_df = pd.DataFrame()

# CONCATENATE ALL FEEDBACK FILES TO ONE
for root, subdirectories, files in os.walk(feedback_files_dir):
    for file in files:
        if not 'all' in file:
            path = os.path.join(root, file)
            df = pd.read_excel(path, sheet_name='Sheet1')
            date = file.split('_')[3].replace('.xlsx', '')
            month = file.split('_')[2]
            df['date'] = date
            df['month'] = month
            full_df = pd.concat([full_df, df])

full_df.to_excel('all_feedbacks.xlsx', sheet_name='Sheet1', index=False)

for project_id in list_of_projects:
    success = False
    while success != True:
        try:
            for status in ['CLOSED', 'OPEN']:
                r = requests.get(f'https://toloka.dev/api/v1/pools?status={status}&project_id={project_id}', headers=HEADERS).json()
                print(r)
                for pool in r['items']:
                    list_of_pools.append(pool['id'])
                success = True
        except Exception as e:
            print('Account change')
            if OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            elif OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
            toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

full_df_toloka = pd.DataFrame()

# COLLECT ALL ASSIGNMENTS TO ONE FILE
for pool_id in list_of_pools:
    success = False
    while success != True:
        print('Try ', pool_id)
        try:
            df_toloka = toloka_client.get_assignments_df(pool_id, status = ['SUBMITTED'])
            full_df_toloka = pd.concat([full_df_toloka, df_toloka])
            success = True
        except Exception as e:
            print('Account change')
            if OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            elif OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
            toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

full_sets_df = pd.DataFrame()

# CHECK IF SET HAS STATUS SUBMITTED AND IS IT IN FEEDBACK FILE
for assignment_id in tqdm(full_df_toloka['ASSIGNMENT:assignment_id']):
    success = False
    assignment_date = full_df_toloka[full_df_toloka['ASSIGNMENT:assignment_id']==assignment_id]['ASSIGNMENT:started'].values[0].split('T')[0]
    if assignment_date >= start_check_date and assignment_date <= finish_check_date:
        tries = 0
        while success != True:
            print('Try ', assignment_id)
            try:
                pool_id = toloka_client.get_assignment(assignment_id=assignment_id).pool_id
                pool_data = toloka_client.get_pool(pool_id=pool_id)
                project_id = pool_data.project_id
                pool_name = pool_data.private_name
                assignment_link = f'https://platform.toloka.ai/requester/project/{project_id}/pool/{pool_id}/assignments/{assignment_id}?direction=ASC'
                if assignment_id in full_df['assignment_id'].unique():
                    in_excels = True
                    innodata_status = full_df[full_df['assignment_id']==assignment_id]['Status'].values[0]
                    date = full_df[full_df['assignment_id']==assignment_id]['date'].values[0]
                    if str(full_df[full_df['assignment_id']==assignment_id]['Reason'].values[0]) != "nan":
                        reason = str(full_df[full_df['assignment_id'] == assignment_id]['Reason'].values[0])
                    elif str(full_df[full_df['assignment_id']==assignment_id]['Remark'].values[0]) != "nan":
                        reason = str(full_df[full_df['assignment_id']==assignment_id]['Remark'].values[0])
                    elif str(full_df[full_df['assignment_id']==assignment_id]['remark'].values[0]) != "nan":
                        reason = str(full_df[full_df['assignment_id']==assignment_id]['remark'].values[0])
                    else:
                        reason = ''
                else:
                    in_excels = False
                    innodata_status = ''
                    date = ''
                    reason = ''
                df = pd.DataFrame(data={'assignment_id':[assignment_id], 'pool_name':[pool_name], 'project_id':[project_id], 'assignment_link':assignment_link, 'in_excels':[in_excels], 'innodata_status':[innodata_status], 'date':[assignment_date], 'reason':[reason]})
                full_sets_df = pd.concat([full_sets_df, df])
                success = True
            except Exception as e:
                tries += 1
                print(e)
                print('Change account')
                if OAUTH_TOKEN == '':
                    OAUTH_TOKEN = ''
                elif OAUTH_TOKEN == '':
                    OAUTH_TOKEN = ''
                HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
                toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
                if tries == 10:
                    success = True

full_sets_df.to_excel('lost_sets.xlsx', index=False)