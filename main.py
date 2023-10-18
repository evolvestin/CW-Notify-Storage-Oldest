# -*- coding: utf-8 -*-
import os
import re
import glob
import asyncio
import gspread
import _thread
from GDrive import Drive
from aiogram import types
import functions as objects
from aiogram.utils import executor
from functions import italic, stamper
from telethon.sync import TelegramClient, types as telethon_types
from aiogram.dispatcher import Dispatcher
if __name__ == '__main__':
    import environ
    print('Запуск с окружением', environ.environ)

stamp1 = objects.time_now()
objects.environmental_files()
temp_prefix = 'temp-'
server_dict = {}
idMe = 396978030
limit = 50000
# =====================================================================================================================


def starting_server_dict_creation():
    global server_dict
    json_list = glob.glob('*.json')
    client = gspread.service_account(json_list[0])
    resources = client.open('Notify').worksheet('resources').get('A1:Z50000', major_dimension='COLUMNS')
    options = resources.pop(0)
    for option in options:
        for resource in resources:
            if server_dict.get(resource[0]) is None:
                for server_json in json_list:
                    if server_json.startswith(resource[0]):
                        server_dict[resource[0]] = {}
                        search_json = re.search(resource[0] + r'_client(\d)\.json', server_json)
                        server_dict[resource[0]]['json' + search_json.group(1)] = server_json
            if server_dict.get(resource[0]) is not None and option in ['channel', 'storage']:
                server_dict[resource[0]][option] = resource[options.index(option)]
    if os.environ.get('local') is None:
        drive_client = Drive(json_list[0])
        for file in drive_client.files():
            if file['name'] == f"{os.environ['session']}.session":
                drive_client.download_file(file['id'], file['name'])


Auth = objects.AuthCentre(os.environ['TOKEN'])
bot = Auth.start_main_bot('async')
starting_server_dict_creation()
dispatcher = Dispatcher(bot)
# =====================================================================================================================


def number_secure(text):
    return re.sub(r'\D', '', str(text))


def former(message: telethon_types.Message):
    response = 'False'
    if message and message.id and message.message and message.date:
        stamp = stamper(str(message.date), '%Y-%m-%d %H:%M:%S+00:00')
        if stamp <= objects.time_now() - 24 * 60 * 60:
            response = f"{message.id}/{re.sub('/', '&#47;', message.message)}".replace('\n', '/')
    return response


@dispatcher.message_handler()
async def repeat_all_messages(message: types.Message):
    if message['chat']['id'] != idMe:
        await bot.send_message(message['chat']['id'], 'К тебе этот бот не имеет отношения, уйди пожалуйста')
    else:
        if message.text.startswith('/log'):
            doc = open('log.txt', 'rt')
            await bot.send_document(idMe, doc, reply_markup=None)
        else:
            await bot.send_message(message['chat']['id'], 'Я работаю', reply_markup=None)


def create_temp_spreadsheet(client, spreadsheet_title, option=None):
    temp_spreadsheet = client.create(temp_prefix + spreadsheet_title)
    spreadsheet = client.open(spreadsheet_title)
    temp_worksheet = temp_spreadsheet.sheet1
    temp_worksheet.resize(rows=limit, cols=1)
    temp_worksheet.update_title('old')
    for user in reversed(spreadsheet.list_permissions()):
        role = user['role']
        if role == 'owner':
            role = 'writer'
        temp_spreadsheet.share(user['emailAddress'], 'user', role, False)
    temp_spreadsheet.batch_update(objects.properties_json(temp_worksheet.id, limit, option))


async def handler(client: TelegramClient, server: dict) -> None:
    response = 'False'
    messages = await client.get_messages(server['channel'], ids=[server['old']])
    for message in messages:
        response = former(message)
    if response.startswith('False'):
        await asyncio.sleep(8)
    else:
        try:
            server['temp_worksheet'].update_cell(len(server['old_values']) + 1, 1, response)
            server['old_values'].append(response)
        except IndexError and Exception as error:
            storage_name = server['storage']
            search_exceed = re.search('exceeds grid limits', str(error))
            if search_exceed:
                worksheet_number = 0
                await asyncio.sleep(100)
                client = gspread.service_account(server['json1'])
                temp_spreadsheet = client.open(temp_prefix + storage_name)
                temp_values = temp_spreadsheet.worksheet('old').col_values(1)

                dev = Auth.send_dev_message('Устраняем таблицу', tag=italic)
                main_spreadsheet = client.open(storage_name)
                for w in main_spreadsheet.worksheets():
                    if number_secure(w.title):
                        title = number_secure(w.title)
                        if int(title) > worksheet_number:
                            worksheet_number = int(title)
                main_worksheet = main_spreadsheet.add_worksheet(str(worksheet_number + 1), limit, 1)
                main_spreadsheet.batch_update(
                    objects.properties_json(main_worksheet.id, limit, temp_values))

                dev_edited = Auth.edit_dev_message(dev, italic('\n— Новая: ' + storage_name +
                                                               '/' + str(worksheet_number + 1)))
                create_temp_spreadsheet(client, storage_name, [response])
                client.del_spreadsheet(temp_spreadsheet.id)
                Auth.edit_dev_message(dev_edited, italic('\n— Успешно'))
                server['old_values'] = [response]
                await asyncio.sleep(30)
            else:
                client = gspread.service_account(server['json1'])
                server['temp_worksheet'] = client.open(temp_prefix + storage_name).worksheet('old')
                server['temp_worksheet'].update_cell(len(server['old_values']) + 1, 1, response)
                server['old_values'].append(response)
        server['old'] += 1
        await asyncio.sleep(1.2)
        objects.printer(f"{server['channel']}/{server['old']} Добавил в google старый лот")


def oldest(server):
    objects.printer(f'Сервер {server}')
    server['old'] = 0
    spreadsheet_files = []
    server['old_values'] = []
    server['temp_worksheet'] = None
    client = gspread.service_account(server['json1'])
    for s in client.list_spreadsheet_files():
        if s['name'] in [i + server['storage'] for i in ['', temp_prefix]]:
            spreadsheet = client.open(s['name'])
            last_worksheet = None
            last_worksheet_id = 1
            for worksheet in spreadsheet.worksheets():
                title = worksheet.title
                if number_secure(title):
                    title = number_secure(title)
                    if int(title) > last_worksheet_id:
                        last_worksheet_id = int(title)
                        last_worksheet = worksheet
                elif title == 'old':
                    last_worksheet = worksheet
            if s['name'] not in spreadsheet_files:
                spreadsheet_files.append(s['name'])
            if last_worksheet:
                values = last_worksheet.col_values(1)
                if s['name'] == temp_prefix + server['storage']:
                    server['temp_worksheet'] = last_worksheet
                    server['old_values'] = values
                for value in values:
                    value = number_secure(value.split('/')[0])
                    if value:
                        if int(value) > server['old']:
                            server['old'] = int(value)
    if server['old'] and spreadsheet_files:
        server['old'] += 1
        asyncio.set_event_loop(asyncio.new_event_loop())
        if temp_prefix + server['storage'] not in spreadsheet_files:
            create_temp_spreadsheet(client, server['storage'])
        telegram_client = TelegramClient(
            os.environ['session'], int(os.environ['api_id']), os.environ['api_hash']).start()
        while True:
            try:
                with telegram_client:
                    telegram_client.loop.run_until_complete(handler(telegram_client, server))
            except IndexError and Exception:
                Auth.executive(None)
    else:
        s_name = 'Undefined'
        for name in server_dict:
            if server_dict[name] == server:
                s_name = name
        Auth.send_dev_message('Нет подключения к google.\nНе запущен CW-Notify-Storage-Oldest(' + s_name + ')')


def start():
    Auth.start_message(stamp1)
    for value in server_dict.values():
        _thread.start_new_thread(oldest, (value,))
    executor.start_polling(dispatcher)


if __name__ == '__main__':
    start()
