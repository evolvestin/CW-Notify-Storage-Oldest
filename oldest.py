# -*- coding: utf-8 -*-
import os
import re
import glob
import asyncio
import gspread
import requests
from aiogram import types
import functions as objects
from bs4 import BeautifulSoup
from aiogram.utils import executor
from functions import italic, stamper
from aiogram.dispatcher import Dispatcher

stamp1 = objects.time_now()
objects.environmental_files()
temp_prefix = 'temp-'
server_dict = {}
idMe = 396978030
limit = 50000
# ========================================================================================================


def starting_server_dict_creation():
    global server_dict
    json_list = glob.glob('*.json')
    client = gspread.service_account(json_list[0])
    resources = client.open('Notify').worksheet('resources').get('A1:Z50000', major_dimension='COLUMNS')
    options = resources.pop(0)
    for option in options:
        for resource in resources:
            if server_dict.get(resource[0]) is None:
                server_dict[resource[0]] = {}
                for server_json in json_list:
                    if server_json.startswith(resource[0]):
                        search_json = re.search(resource[0] + r'_client(\d)\.json', server_json)
                        server_dict[resource[0]]['json' + search_json.group(1)] = server_json
            if option == 'channel':
                value = 'https://t.me/' + resource[options.index(option)] + '/'
                server_dict[resource[0]][option] = value
            elif option == 'storage':
                server_dict[resource[0]]['storage'] = resource[options.index(option)]


Auth = objects.AuthCentre(os.environ['TOKEN'])
bot = Auth.start_main_bot('async')
starting_server_dict_creation()
dispatcher = Dispatcher(bot)
Auth.start_message(stamp1)
# ========================================================================================================


def number_secure(text):
    return re.sub(r'\D', '', str(text))


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


def former(text):
    response = 'False'
    soup = BeautifulSoup(text, 'html.parser')
    is_post_not_exist = soup.find('div', class_='tgme_widget_message_error')
    if is_post_not_exist is None:
        lot_raw = str(soup.find('div', class_='tgme_widget_message_text js-message_text')).replace('<br/>', '\n')
        get_au_id = soup.find('div', class_='tgme_widget_message_link')
        if get_au_id:
            au_id = re.sub('t.me/.*?/', '', get_au_id.get_text())
            lot = BeautifulSoup(lot_raw, 'html.parser').get_text()
            stamp = stamper(str(soup.find('time', class_='datetime').get('datetime')), '%Y-%m-%dT%H:%M:%S+00:00')
            if stamp <= objects.time_now() - 24 * 60 * 60:
                response = au_id + '/' + re.sub('/', '&#47;', lot).replace('\n', '/')
    if is_post_not_exist:
        search_error_requests = re.search('Channel with username .*? not found', is_post_not_exist.get_text())
        if search_error_requests:
            response += 'Requests'
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


async def oldest(server):
    print('Сервер', server)
    old = 0
    old_values = []
    temp_worksheet = None
    spreadsheet_files = []
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
                    temp_worksheet = last_worksheet
                    old_values = values
                for value in values:
                    value = number_secure(value.split('/')[0])
                    if value:
                        if int(value) > old:
                            old = int(value)
    if old and spreadsheet_files:
        old += 1
        if temp_prefix + server['storage'] not in spreadsheet_files:
            create_temp_spreadsheet(client, server['storage'])
        while True:
            try:
                print_text = server['channel'] + str(old)
                text = requests.get(print_text + '?embed=1')
                response = former(text.text)
                if response.startswith('False'):
                    await asyncio.sleep(8)
                else:
                    try:
                        temp_worksheet.update_cell(len(old_values) + 1, 1, response)
                        old_values.append(response)
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
                            old_values = [response]
                            await asyncio.sleep(30)
                        else:
                            client = gspread.service_account(server['json1'])
                            temp_worksheet = client.open(temp_prefix + storage_name).worksheet('old')
                            temp_worksheet.update_cell(len(old_values) + 1, 1, response)
                            old_values.append(response)
                    old += 1
                    await asyncio.sleep(1.2)
                    objects.printer(print_text + ' Добавил в google старый лот')
            except IndexError and Exception:
                await Auth.async_exec()
    else:
        s_name = 'Undefined'
        for name in server_dict:
            if server_dict[name] == server:
                s_name = name
        Auth.send_dev_message('Нет подключения к google.\nНе запущен CW-Notify-Storage-Oldest(' + s_name + ')')


def start():
    loop = asyncio.new_event_loop()
    for server_name in server_dict:
        loop.create_task(oldest(server_dict[server_name]))
    asyncio.set_event_loop(loop)
    executor.start_polling(dispatcher)


if __name__ == '__main__':
    start()
