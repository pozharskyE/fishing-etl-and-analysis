import asyncio
import aiohttp
import json
import pandas as pd


# https://dev.vk.com/ru/api/getting-started
with open('secret_vk_app_service_token.txt', 'r') as f:
    SERVICE_TOKEN = f.read()

API_METHOD = 'groups.getMembers'
GROUP_ID = 56433000
SORT_METHOD = 'id_asc'
FIELDS = 'last_seen,contacts,city,bdate'
VERSION = '5.199'

RAW_OUTPUT_FILE = 'members_raw_response.json'
TRANSFORMED_OUTPUT_CSV_FILE = 'members_df.csv'


BASE_URL = f"https://api.vk.com/method/{API_METHOD}?group_id={GROUP_ID}&sort={SORT_METHOD}&fields={FIELDS}&access_token={SERVICE_TOKEN}&v={VERSION}"

LIMIT = 1000 # - how many datapoints (items) can one request contain

COUNT = 855000  # how many datapoints to receive. NOTE: it should not be bigger, than the real number of members in target group (I did not check what would happen)
NUM_WORKERS = 5  # how many request will be in each task group (will be executed in parallel). Note: too many parallel requests may be blocked by vk.com's api (5 works just fine for me)


async def fetch_chunk(url, offset):
    url = url + "&offset=" + str(offset)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()

    return data['response']


async def main():
    # because limit of count is 1000  => we need to make request until all data will be recieved 
    # to make requests much faster (the amount of requests is too big to run it synchronously) => we need to use async

    members_array = []

    base_offset = 0
    while base_offset + (LIMIT * NUM_WORKERS) <= COUNT :


        # for example if {base_offset=3000, LIMIT=1000, NUM_WORKERS=3} ===> offsets=[3000, 4000, 5000] 
        offsets = [base_offset + (worker_idx * LIMIT) for worker_idx in range(NUM_WORKERS)]


        tasks = []
        async with asyncio.TaskGroup() as tg:
            for offset in offsets:
                task = tg.create_task(fetch_chunk(BASE_URL, offset))
                tasks.append(task)


        for task in tasks:
            members_array.extend(task.result()['items'])


        # printing like "package 2000-3000 received   (1.2%)"  (of course, for each received package in our task group)
        for worker_idx in range(NUM_WORKERS):
            beginning = base_offset + (worker_idx * LIMIT)
            ending = base_offset + ((worker_idx + 1) * LIMIT)
            percent = round(ending / COUNT * 100, 3)
            print(f"package {beginning}-{ending} received   ({percent}%)")
            

        base_offset += LIMIT * NUM_WORKERS


    with open(RAW_OUTPUT_FILE, 'w') as f:
        json.dump(members_array, f, indent=2)

    print(f"All packages have been received and raw result array was written to a file '{RAW_OUTPUT_FILE}' ")
    print('Transforming...')



    # transforming and saving as csv
    with open(RAW_OUTPUT_FILE, 'r') as f:
        members = json.load(f)

    members_df = pd.DataFrame(members)

    temp = members_df[members_df['is_closed'] == False]

    temp = temp[temp['deactivated'] != 'banned']
    temp = temp[temp['deactivated'] != 'deleted']
    temp = temp.drop('deactivated', axis=1)

    temp['full_name'] = temp['first_name'] + ' ' + temp['last_name']
    temp = temp.drop(['first_name', 'last_name'], axis=1)

    temp.rename(columns={'id': 'user_id_vk', 'city': 'town'}, inplace=True)

    temp = temp[['user_id_vk', 'full_name', 'last_seen', 'town', 'mobile_phone', 'home_phone']].reset_index(drop=True)

    temp['friends_count'] = pd.Series(dtype='int')

    members_df = temp
    members_df.to_csv(TRANSFORMED_OUTPUT_CSV_FILE, index=False)
    
    print(f"Raw response was transformed and the result was written to a file {TRANSFORMED_OUTPUT_CSV_FILE}")


if __name__ == '__main__':
    asyncio.run(main())