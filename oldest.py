# -*- coding: utf-8 -*-
import re
import copy
import _thread
import gspread
import requests
import datetime
from time import sleep
import concurrent.futures
from SQL import SQLighter
from bs4 import BeautifulSoup
from datetime import datetime
from additional.game_time import timer
from additional.dimension import bot_dimension
from requests_futures.sessions import FuturesSession
from additional.objects import thread_exec as executive
from oauth2client.service_account import ServiceAccountCredentials
from additional.objects import code, bold, query, printer, stamper, log_time, secure_sql, \
    start_message, start_main_bot, send_dev_message, edit_dev_message

stamp1 = int(datetime.now().timestamp())
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
start_sql_request = 'INSERT INTO old (au_id, lot_id, enchant, item_name, quality, ' \
                    'condition, modifiers, seller, cost, buyer, stamp, status) VALUES '
start_sql_update = 'INSERT INTO old (au_id, lot_id, enchant, item_name, quality, ' \
                    'condition, modifiers, seller, cost, buyer, stamp, status) VALUES '
properties_title_list = ['lot_id', 'enchant', 'item_name', 'quality', 'condition',
                         'modifiers', 'seller', 'cost', 'buyer', 'stamp', 'status', 'raw']
lot_updater_channel = 'https://t.me/lot_updater/'
variable = bot_dimension()
first_open = True
update_array = []
idMe = 396978030
limit = 50000
limiter = 300
old = 1
# ====================================================================================


def form_mash(lot):
    stamp_now = int(datetime.now().timestamp()) - 36 * 60 * 60
    lot = re.sub('\'', '&#39;', lot)
    lot_properties = {'au_id': 0}
    lot_split = lot.split('/')
    search_au_id = re.search('(\d+)', lot_split[0])
    if search_au_id:
        lot_properties['au_id'] = int(search_au_id.group(1))
    for i in properties_title_list:
        if i == 'lot_id' or i == 'stamp':
            lot_properties[i] = 0
        else:
            lot_properties[i] = 'None'
    for g in lot_split:
        for i in variable['form']:
            search = re.search(variable['form'].get(i), g)
            if search:
                if i == 'title':
                    item_name = re.sub(' \+\d+[⚔🛡]', '', search.group(2))
                    enchant_search = re.search('⚡\+(\d+) ', item_name)
                    lot_properties['lot_id'] = int(search.group(1))
                    item_name = re.sub(' \+\d+💧', '', item_name)
                    enchant = 'None'
                    if enchant_search:
                        item_name = re.sub('⚡\+\d+ ', '', item_name)
                        enchant = enchant_search.group(1)
                    lot_properties['item_name'] = item_name
                    lot_properties['enchant'] = enchant
                elif i == 'condition':
                    lot_properties[i] = re.sub(' ⏰.*', '', search.group(1))
                elif i == 'modifiers':
                    lot_properties[i] = ''
                elif i == 'cost':
                    lot_properties[i] = int(search.group(1))
                elif i == 'stamp':
                    lot_properties[i] = timer(search)
                elif i == 'status':
                    status = search.group(1)
                    if status == 'Failed':
                        status = 'Cancelled'
                    if status == '#active':
                        if lot_properties['stamp'] < stamp_now:
                            status = 'Finished'
                    lot_properties[i] = status
                elif i == 'raw':
                    lot_properties[i] = lot
                else:
                    lot_properties[i] = search.group(1)
        if lot_properties['modifiers'] != 'None' and g.startswith(' '):
            lot_properties['modifiers'] += '  ' + g.strip() + '\n'
    if lot_properties['modifiers'] != 'None' and lot_properties['modifiers'].endswith('\n'):
        lot_properties['modifiers'] = lot_properties['modifiers'][:-1]
    return lot_properties


def database_filler():
    global old
    global creds1
    global client1
    db = SQLighter('old.db')
    creds1 = ServiceAccountCredentials.from_json_keyfile_name(variable['json_old'], scope)
    client1 = gspread.authorize(creds1)
    spreadsheet_list = client1.list_spreadsheet_files()
    for s in spreadsheet_list:
        document_name = s['name']
        if document_name == variable['storage'] or document_name == 'temp-' + variable['storage']:
            document = client1.open(document_name)
            for w in document.worksheets():
                worksheet = document.worksheet(w.title)
                values = worksheet.col_values(1)
                sql_request_line = ''
                position = 0
                point = 0
                for g in values:
                    position += 1
                    lot_object = form_mash(g)
                    au_id = lot_object.get('au_id')
                    if au_id > old:
                        old = au_id
                    if au_id != 0:
                        sql_request_line += "('{}', '{}', '{}', '{}', '{}', '{}', " \
                                            "'{}', '{}', '{}', '{}', '{}', '{}'), ".format(*lot_object.values())
                        point += 1
                        if point == 1000:
                            sql_request_line = sql_request_line.rstrip()
                            sql_request_line = sql_request_line[:-1] + ';'
                            db.create_lots(start_sql_request + sql_request_line)
                            sql_request_line = ''
                            point = 0
                    else:
                        error = document_name + '/' + w.title + '/' + str(position) + ' пуст или неисправен'
                        send_dev_message(variable['storage'], error)
                if sql_request_line != '':
                    sql_request_line = sql_request_line.rstrip()
                    sql_request_line = sql_request_line[:-1] + ';'
                    db.create_lots(start_sql_request + sql_request_line)
    double = []
    double_raw = db.get_double()
    if double_raw:
        for i in double_raw:
            lot_header = str(i[0]) + ' ' + str(i[1])
            if lot_header not in double:
                double.append(lot_header)
    for lot_header in double:
        send_dev_message(variable['storage'], 'Повторяющийся в базе элемент: ' + bold(lot_header))


start_search = query(lot_updater_channel + str(variable['lot_updater']), '(.*)')
if start_search:
    s_message = start_message(variable['storage'], stamp1)
    database_filler()
    s_message = edit_dev_message(s_message, '\n' + log_time(tag=code))
else:
    additional_text = '\nНет подключения к ' + lot_updater_channel + ' ' + bold('Бот выключен')
    s_message = start_message(variable['storage'], stamp1, additional_text)
    _thread.exit()
bot = start_main_bot('non-async', variable['TOKEN'])
new = copy.copy(old + 1)
old += 1
# ====================================================================================


def last_time_request():
    global last_requested
    last_requested = int(datetime.now().timestamp())


def telegram_editor(text, print_text):
    try:
        message = bot.edit_message_text(code(text), -1001376067490, variable['lot_updater'], parse_mode='HTML')
        response = message.text.split('/')
    except IndexError and Exception:
        print_text += ' (пост не изменился)'
        response = text.split('/')
    printer(print_text)
    return response


def updater(pos, cost, stat, const):
    global worksheet_storage
    row = str(pos + 1)
    try:
        cell_list = worksheet_storage.range('A' + row + ':C' + row)
        cell_list[0].value = const[pos]
        cell_list[1].value = cost
        cell_list[2].value = stat
        worksheet_storage.update_cells(cell_list)
    except IndexError and Exception:
        creds2 = ServiceAccountCredentials.from_json_keyfile_name(variable['json_storage'], scope)
        client2 = gspread.authorize(creds2)
        worksheet_storage = client2.open('Notify').worksheet(variable['Notify'])
        cell_list = worksheet_storage.range('A' + row + ':C' + row)
        cell_list[0].value = const[pos]
        cell_list[1].value = cost
        cell_list[2].value = stat
        worksheet_storage.update_cells(cell_list)
    sleep(1)
    printer('i = ' + str(pos) + ' новое')


def former(text, form='new'):
    soup = BeautifulSoup(text, 'html.parser')
    is_post_not_exist = soup.find('div', class_='tgme_widget_message_error')
    if is_post_not_exist is None:
        stamp_day_ago = int(datetime.now().timestamp()) - 24 * 60 * 60
        lot_raw = str(soup.find('div', class_='tgme_widget_message_text js-message_text')).replace('<br/>', '\n')
        au_id = re.sub('t.me/.*?/', '', soup.find('div', class_='tgme_widget_message_link').get_text())
        lot = BeautifulSoup(lot_raw, 'html.parser').get_text()
        response = {'raw': au_id + '/' + re.sub('/', '&#47;', lot).replace('\n', '/')}
        if form == 'new':
            response = form_mash(response['raw'])
        else:
            drop_time = soup.find('time', class_='datetime')
            stamp = stamper(str(drop_time['datetime']), '%Y-%m-%dT%H:%M:%S+00:00')
            if stamp > stamp_day_ago:
                response = {'raw': 'active'}
    else:
        response = {'raw': 'False'}
        search_error_requests = re.search('Channel with username .*? not found', is_post_not_exist.get_text())
        if search_error_requests:
            response['raw'] += 'Requests'
    return response


def detector():
    while True:
        try:
            global new
            global limiter
            global first_open
            if first_open:
                loop = True
                request_array = []
                while loop is True:
                    futures = []
                    au_id_array = []
                    sql_request_line = ''
                    session = FuturesSession()
                    if len(request_array) == 0:
                        request_array = range(new, new + limiter)
                    for k in request_array:
                        url = variable['channel'] + str(k) + '?embed=1'
                        futures.append(session.get(url))
                        au_id_array.append(k)
                        limiter -= 1
                    request_array = []
                    for future in concurrent.futures.as_completed(futures):
                        last_time_request()
                        result = former(future.result().content)
                        if result['raw'] == 'False':
                            edit_dev_message(s_message, '\n' + log_time(tag=code))
                            first_open = False
                            loop = False
                            new -= 30
                            break
                        elif result['raw'] != 'FalseRequests' and result['raw'] != 'False':
                            au_id_array[au_id_array.index(result['au_id'])] = None
                            if result['au_id'] > new:
                                new = result['au_id']
                            sql_request_line += "('{}', '{}', '{}', '{}', '{}', '{}', " \
                                                "'{}', '{}', '{}', '{}', '{}', '{}'), ".format(*result.values())
                    for k in au_id_array:
                        if k is not None:
                            request_array.append(k)
                    if sql_request_line != '':
                        db = SQLighter('old.db')
                        sql_request_line = sql_request_line.rstrip()
                        sql_request_line = sql_request_line[:-1] + ';'
                        secure_sql(db.create_lots, start_sql_request + sql_request_line)
                    if limiter <= 0:
                        delay = 60 - (int(datetime.now().timestamp()) - last_requested)
                        limiter = 300
                        sleep(delay)
            db = SQLighter('old.db')
            print_text = variable['channel'] + str(new)
            text = requests.get(print_text + '?embed=1')
            response = former(text.text)
            if response['raw'] != 'FalseRequests' and response['raw'] != 'False':
                del response['raw']
                au_id = secure_sql(db.get_au_id)
                if response['au_id'] not in au_id:
                    secure_sql(db.create_lot, response.values())
                    print_text += ' Добавлен в базу'
                else:
                    print_text += ' Уже есть в базе'
                new += 1
            else:
                print_text += ' Форму не нашло'
                sleep(8)
            printer(print_text)
        except IndexError and Exception:
            executive()


def lot_updater():
    while True:
        try:
            global limiter
            global update_array
            if first_open:
                sleep(30)
            else:
                point = 0
                futures = []
                au_id_array = []
                sql_request_array = []
                db = SQLighter('old.db')
                session = FuturesSession()
                if len(update_array) == 0:
                    update_array = secure_sql(db.get_active_au_id)
                for k in update_array:
                    url = variable['channel'] + str(k) + '?embed=1'
                    futures.append(session.get(url))
                    au_id_array.append(k)
                    limiter -= 1
                update_array = []
                for future in concurrent.futures.as_completed(futures):
                    last_time_request()
                    result = former(future.result().content)
                    if result['raw'] != 'FalseRequests' and result['raw'] != 'False':
                        au_id_array[au_id_array.index(result['au_id'])] = None
                        if result['status'] != '#active':
                            point += 1
                            sql_request_array.append(result)
                for k in au_id_array:
                    if k is not None:
                        update_array.append(k)
                for k in sql_request_array:
                    secure_sql(db.update_lot, k)
                if len(sql_request_array) > 0:
                    printer('Обновлено лотов в базе ' + str(len(sql_request_array)))
                if limiter <= 0:
                    delay = 60 - (int(datetime.now().timestamp()) - last_requested)
                    printer('Ухожу в сон на ' + str(delay) + ' секунд')
                    limiter = 300
                    sleep(delay)
        except IndexError and Exception:
            executive()


def telegram():
    while True:
        try:
            global first_open
            db = SQLighter('old.db')
            if first_open:
                sleep(10)
            else:
                sleep(20)
                lots_raw = query(lot_updater_channel + str(variable['lot_updater']), '(.*)')
                if lots_raw:
                    array = lots_raw.group(1).split('/')
                    row = '/'
                    for i in array:
                        if i != '':
                            row += i + '/'
                    au_id = secure_sql(db.get_active_au_id)
                    for i in au_id:
                        if str(i) not in array:
                            if len(row) < 4085:
                                row += str(i) + '/'
                    array = telegram_editor(row, 'добавил новые лоты в telegram')
                    for i in array:
                        if i != '':
                            if int(i) not in au_id:
                                row = re.sub('/' + str(i) + '/', '/', row)
                    telegram_editor(row, 'удалил закончившиеся из google')
        except IndexError and Exception:
            executive()


def messages():
    while True:
        try:
            if first_open is False:
                global worksheet_storage
                point = 0
                const = []
                printer('начало')
                db = SQLighter('old.db')
                creds2 = ServiceAccountCredentials.from_json_keyfile_name(variable['json_storage'], scope)
                client2 = gspread.authorize(creds2)
                const_pre = client2.open('Notify').worksheet('items').col_values(1)
                worksheet_storage = client2.open('Notify').worksheet(variable['Notify'])
                old_stats = worksheet_storage.col_values(3)
                sleep(2)
                for g in const_pre:
                    const.append(g + '/None')
                    qualities = secure_sql(db.get_quality, g)
                    if len(qualities) > 1:
                        const.append(g + '/Common')
                    for q in qualities:
                        if q != 'None':
                            const.append(g + '/' + q)
                while point < len(const):
                    text = ''
                    time_30 = int(datetime.now().timestamp()) - (7 * 24 * 60 * 60)
                    f_max = 0
                    max_30 = 0
                    newcol = []
                    unsold = []
                    f_min = 1000
                    min_30 = 1000
                    f_average = 0
                    newcol_30 = []
                    average_30 = 0
                    f_un_average = 0
                    un_average_30 = 0
                    split = const[point].split('/')
                    col = secure_sql(db.get_lots, split[0])
                    for z in col:
                        quality = z[4]
                        cost = z[8]
                        buyer = z[9]
                        stamp = z[10]
                        status = z[11]
                        if status != 'Cancelled':
                            if quality == split[1] or split[1] == 'None' or \
                                    (split[1] == 'Common' and quality == 'None'):
                                if buyer != 'None':
                                    if stamp >= time_30:
                                        newcol_30.append(cost)
                                        average_30 += cost
                                        if min_30 > cost:
                                            min_30 = cost
                                        if max_30 < cost:
                                            max_30 = cost
                                    newcol.append(cost)
                                    f_average += cost
                                    if f_min > cost:
                                        f_min = cost
                                    if f_max < cost:
                                        f_max = cost
                                else:
                                    if stamp >= time_30:
                                        un_average_30 += 1
                                    unsold.append(cost)
                                    f_un_average += 1

                    if len(newcol) > 0:
                        last = newcol[len(newcol) - 1]
                        last_sold = '_{8} ' + str(last)
                    else:
                        last_sold = ''

                    newcol.sort()
                    newcol_30.sort()

                    if len(newcol) % 2 == 0 and len(newcol) != 0:
                        lot1 = int(newcol[len(newcol) // 2])
                        lot2 = int(newcol[len(newcol) // 2 - 1])
                        median = round((lot1 + lot2) / 2, 2)
                        if (median % int(median)) == 0:
                            median = int(median)
                    elif len(newcol) == 0:
                        median = 0
                    else:
                        median = int(newcol[len(newcol) // 2])

                    if len(newcol_30) % 2 == 0 and len(newcol_30) != 0:
                        lot1_30 = int(newcol_30[len(newcol_30) // 2])
                        lot2_30 = int(newcol_30[len(newcol_30) // 2 - 1])
                        median_30 = round((lot1_30 + lot2_30) / 2, 2)
                        if (median_30 % int(median_30)) == 0:
                            median_30 = int(median_30)
                    elif len(newcol_30) == 0:
                        median_30 = 0
                    else:
                        median_30 = int(newcol_30[len(newcol_30) // 2])

                    if len(newcol) > 0:
                        f_average = round(f_average / len(newcol), 2)
                    else:
                        f_average = 0
                    if len(newcol_30) > 0:
                        average_30 = round(average_30 / len(newcol_30), 2)
                    else:
                        average_30 = 0

                    if f_min == 1000:
                        f_min = 0
                    if min_30 == 1000:
                        min_30 = 0

                    t_costs = str(median) + '/' + str(median_30)

                    text += '__' + bold('{1} ') + str(len(newcol)) + '__' + \
                        bold('{2}') + '_' + \
                        '{4} ' + str(median) + '_' + \
                        '{5} ' + str(f_average) + '_' + \
                        '{6} ' + str(f_min) + '/' + str(f_max) + '_' + \
                        '{7} ' + str(f_un_average) + '/' + str(len(newcol) + f_un_average) + '__' + \
                        bold('{3}') + '_' + \
                        '{4} ' + str(median_30) + '_' + \
                        '{5} ' + str(average_30) + '_' + \
                        '{6} ' + str(min_30) + '/' + str(max_30) + '_' + \
                        '{7} ' + str(un_average_30) + '/' + str(len(newcol_30) + un_average_30) + \
                        str(last_sold) + '__'

                    if len(old_stats) > point:
                        if text != old_stats[point]:
                            updater(point, t_costs, text, const)
                    else:
                        updater(point, t_costs, text, const)
                    point += 1
                printer('конец')
            else:
                sleep(20)
        except IndexError and Exception:
            executive()


@bot.message_handler(func=lambda message: message.text)
def repeat_all_messages(message):
    if message.chat.id != idMe:
        bot.send_message(message.chat.id, 'К тебе этот бот не имеет отношения, уйди пожалуйста')
    else:
        if message.text.startswith('/base'):
            modified = re.sub('/base_', '', message.text)
            if modified.startswith('n'):
                doc = open('new.db', 'rb')
                bot.send_document(idMe, doc)
            elif modified.startswith('o'):
                doc = open('old.db', 'rb')
                bot.send_document(idMe, doc)
            else:
                doc = open('log.txt', 'rt')
                bot.send_document(idMe, doc)
            doc.close()
        else:
            bot.send_message(message.chat.id, 'Я работаю')


def telepol():
    try:
        bot.polling(none_stop=True, timeout=60)
    except IndexError and Exception:
        bot.stop_polling()
        sleep(1)
        telepol()


if __name__ == '__main__':
    gain = [detector, lot_updater, telegram, messages]
    for thread_element in gain:
        _thread.start_new_thread(thread_element, ())
    telepol()
