import asyncio
import aiohttp
import pandas as pd
import time


# https://dev.vk.com/ru/api/getting-started
with open('secret_vk_app_service_token.txt', 'r') as f:
    SERVICE_TOKEN = f.read()



API_METHOD = 'friends.get'
VERSION = '5.199'
BASE_URL = f"https://api.vk.com/method/{API_METHOD}?access_token={SERVICE_TOKEN}&v=5.199"


TARGET_DF = 'members_df.csv'
POSITION_CACHE_FILE = 'where_stopped_get_friends_count.txt'

with open(POSITION_CACHE_FILE, 'r') as f:
    where = f.read()
    if where:
        position = int(where)
    else:
        position = 0
    

COUNT = 20000
NUM_WORKERS = 5


SAVE_ONCE_IN_TASKGROUPS = 20

errors_counter = 0

async def fetch_count(url, user_id):
    url = url + "&user_id=" + str(user_id)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    try:
        return data['response']['count']
    except KeyError:
        global errors_counter
        errors_counter += 1
        return None


async def main():
    target_df = pd.read_csv(TARGET_DF)
    user_ids = target_df.loc[position:(position + COUNT - 1), 'user_id_vk']

    for i, start in enumerate(range(0, COUNT, NUM_WORKERS), start=1):
        timestamp1 = time.time()

        end = start + NUM_WORKERS
        taskgroup_ids = user_ids[start:end]

        tasks = []
        async with asyncio.TaskGroup() as tg:
            for user_id in taskgroup_ids:
                task = tg.create_task(fetch_count(BASE_URL, user_id))
                tasks.append(task)
            
        friend_nums = [task.result() for task in tasks]

        for (user_id, friends_num) in zip(taskgroup_ids, friend_nums):
            target_df.loc[target_df['user_id_vk'] == user_id, 'friends_count'] = friends_num

        percent = round(end / COUNT * 100, 3)
        print(f"Friends_num for users [{position + start}-{position + end - 1}] were fetched ({percent}%)")

        # save changes and cache where stopped (last save)
        if i % SAVE_ONCE_IN_TASKGROUPS == 0:
            target_df.to_csv(TARGET_DF, index=False)

            with open(POSITION_CACHE_FILE, 'w') as f:
                f.write(str(position + end))

            print(f"Changes were saved to '{TARGET_DF}'; position cached to '{POSITION_CACHE_FILE}'; overall_errors_counter={errors_counter}")


        # if it took less than 1.05 seconds - sleep 
        timestamp2 = time.time()
        it_took = timestamp2 - timestamp1
        if 1.05 > it_took:
            time.sleep(1.05 - it_took)


    print("Done")
    

if __name__ == '__main__':
    asyncio.run(main())