# import requests
from telethon import TelegramClient, sync
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.functions.messages import GetHistoryRequest
import time
from dbase import *

# Для локального запуска 1
dbase_host = 'mysql-135b10a1-kgdk-9e54.aivencloud.com'
dbase_port = 14592
dbase_user = 'avnadmin'
dbase_pass = 'AVNS_udYFDGkTfYpb4XZ8bEe'
dbase_name = 'spam01'
TOKEN = '5110830741:AAG-nlLX4Xy7shFN3DxJL0_7XiPSeGE7WHA'
ConnectionDbaseData = (dbase_host, dbase_port, dbase_user, dbase_pass, dbase_name)

api_id = 14079422
api_hash = '53ea1513da51311b19c56114cff142b2'
client = TelegramClient('connect', api_id, api_hash)
client.start()

# channels = ['Full_Time_Trading']
# channels = ['Coin_Post']
channels = ['investfuture']
# text_to_send = 'Миру - мир'
text_to_send = 'Удобный бот для отслеживания инсайдерских сделок https://t.me/Insider_deals_bot'


def write_comment_to_post(channels, limit, text_to_send):
    for channel in channels:
        try:
            posts = client(GetHistoryRequest(
                peer=channel,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0))
            for msg in posts.messages:
                client.send_message(entity=channel, message=text_to_send, comment_to=msg)
                print('В пост \'{0}\' канала \'{1}\' отправлено сообщение \'{2}\''.format(msg, channel, text_to_send))
        except:
            print("нет канала " + channel)


def get_id_users(channels):
    for channel in channels:
        for post in client.iter_messages(channel):
            try:
                for reply in client.iter_messages(channel, reply_to=post.id):
                    print(reply.sender.id, reply.sender.username, reply.text)
                    # add_record_to_table(ConnectionDbaseData, 'tickers', TickerName=ticker_name, ShareName=share_name)
                    if is_unique_record(ConnectionDbaseData, 'Users', 'User_Id', str(reply.sender.id)):
                        add_record_to_table(ConnectionDbaseData, 'Users', User_Id=str(reply.sender.id), Surname=reply.sender.username, Field1=channel)
            except Exception as e:
                pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # write_comment_to_post(channels=channels, limit=5, text_to_send=text_to_send)
    get_id_users(channels)
    # client.send_message(5498873946, "Hello User ID")
