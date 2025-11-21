import _thread
import base64
import calendar
import codecs
import inspect
import os
import re
import sys
import time
import traceback
import unicodedata
from ast import literal_eval
from datetime import datetime, timezone

import telebot
from unidecode import unidecode

host = 'server'
app_name = 'oldest'
log_file_name = 'log.txt'
sql_patterns = ['database is locked', 'disk image is malformed', 'no such table']
search_retry_pattern = r'Retry in (\d+) seconds|"Too Many Requests: retry after (\d+)"'
search_major_fails_pattern = 'The (read|write) operation timed out|Backend Error|is currently unavailable.'
search_minor_fails_pattern = (
    'Failed to establish a new connection|Read timed out.|ServerDisconnectedError|Message_id_invalid|Connection aborted'
)


def bold(text):
    return '<b>' + str(text) + '</b>'


def italic(text):
    return '<i>' + str(text) + '</i>'


def code(text):
    return '<code>' + str(text) + '</code>'


def time_now():
    return int(datetime.now().timestamp())


def html_link(link, text):
    return '<a href="' + str(link) + '">' + str(text) + '</a>'


def html_secure(text):
    response = re.sub('<', '&#60;', str(text))
    response = re.sub('[{]', '&#123;', response)
    return re.sub('[}]', '&#125;', response)


def stamper(date, pattern=None):
    if pattern is None:
        pattern = '%d/%m/%Y %H:%M:%S'
    try:
        stamp = int(calendar.timegm(time.strptime(date, pattern)))
    except IndexError and Exception:
        stamp = False
    return stamp


def environmental_files(python=False, return_all_json=True):
    created_files = []
    directory = os.listdir('.')
    for key in os.environ.keys():
        key = key.lower()
        if key.endswith('.json'):
            if return_all_json:
                created_files.append(key)
            if key not in directory:
                file = open(key, 'w')
                file.write(os.environ.get(key))
                if return_all_json in [False, None]:
                    created_files.append(key)
                file.close()
        if key.endswith('.py') and python is True:
            with codecs.open(key, 'w', 'utf-8') as file:
                file.write(base64.b64decode(os.environ.get(key)).decode('utf-8'))
                file.close()
    return created_files


def printer(printer_text):
    thread_name = ''
    stack = inspect.stack()
    if len(stack) <= 4:
        stack = list(reversed(stack))
    for i in stack:
        if i[3] not in ['<module>', 'printer']:
            thread_name += i[3] + '.'
            if len(stack) > 4:
                break
    thread_name = re.sub('[<>]', '', thread_name[:-1])
    log_print_text = thread_name + '() [' + str(_thread.get_ident()) + '] ' + str(printer_text)
    print(log_print_text)


def properties_json(sheet_id, limit, option=None):
    if option is None:
        option = []
    body = {
        'requests': [
            {
                'updateCells': {
                    'rows': [
                        {
                            'values': [
                                {
                                    'userEnteredValue': {'stringValue': option[i]},
                                    'userEnteredFormat': {'horizontalAlignment': 'CENTER'},
                                }
                            ]
                        }
                        if len(option) - 1 >= i
                        else {
                            'values': [
                                {
                                    'userEnteredValue': {'stringValue': ''},
                                    'userEnteredFormat': {'horizontalAlignment': 'CENTER'},
                                }
                            ]
                        }
                        for i in range(0, limit)
                    ],
                    'fields': 'userEnteredValue, userEnteredFormat',
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': limit,
                        'startColumnIndex': 0,
                        'endColumnIndex': 1,
                    },
                }
            },
            {
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 1,
                    },
                    'properties': {'pixelSize': 1650},
                    'fields': 'pixelSize',
                }
            },
        ]
    }
    return body


class AuthCentre:
    def __init__(self, token, dev_chat_id=None):
        self.token = token
        self.dev_chat_id = -1001312302092
        if dev_chat_id:
            self.dev_chat_id = dev_chat_id
        self.bot = telebot.TeleBot(token)
        self.get_me = literal_eval(str(self.bot.get_me()))

    def send_dev_message(self, text, tag=code):
        if tag:
            text = tag(html_secure(text))
        text = bold(app_name) + ' (' + code(host) + '):\n' + text
        message = self.bot.send_message(self.dev_chat_id, text, disable_web_page_preview=True, parse_mode='HTML')
        return message

    def start_message(self):
        bot_linked_name = html_link(f'https://t.me/{self.get_me.get("username")}', bold(app_name))
        text = f'{bot_linked_name} ({code(host)}):\n{code(datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))}'
        message = self.bot.send_message(self.dev_chat_id, text, disable_web_page_preview=True, parse_mode='HTML')
        return message

    def start_main_bot(self):
        print('System started')
        return self.bot

    def edit_dev_message(self, old_message, text):
        entities = old_message.entities
        text_list = list(html_secure(old_message.text))
        if entities:
            position = 0
            used_offsets = []
            for i in text_list:
                true_length = len(i.encode('utf-16-le')) // 2
                while true_length > 1:
                    text_list.insert(position + 1, '')
                    true_length -= 1
                position += 1
            for i in reversed(entities):
                end_index = i.offset + i.length - 1
                if i.offset + i.length >= len(text_list):
                    end_index = len(text_list) - 1
                if i.type != 'mention':
                    tag = 'code'
                    if i.type == 'bold':
                        tag = 'b'
                    elif i.type == 'italic':
                        tag = 'i'
                    elif i.type == 'text_link':
                        tag = 'a'
                    elif i.type == 'underline':
                        tag = 'u'
                    elif i.type == 'strikethrough':
                        tag = 's'
                    if i.offset + i.length not in used_offsets or i.type == 'text_link':
                        text_list[end_index] += '</' + tag + '>'
                        if i.type == 'text_link':
                            tag = 'a href="' + i.url + '"'
                        text_list[i.offset] = '<' + tag + '>' + text_list[i.offset]
                        used_offsets.append(i.offset + i.length)
        new_text = ''.join(text_list) + text
        try:
            message = self.bot.edit_message_text(
                new_text,
                old_message.chat.id,
                old_message.message_id,
                disable_web_page_preview=True,
                parse_mode='HTML',
            )
        except IndexError and Exception:
            new_text += italic('\nCould not edit message. New one sent')
            message = self.bot.send_message(self.dev_chat_id, new_text, parse_mode='HTML')
        return message

    # =============================================================================================================
    # ================================================  EXECUTIVE  ================================================
    # =============================================================================================================

    def executive(self, logs):
        retry = 100
        func = None
        func_locals = []
        stack = inspect.stack()
        name = re.sub('[<>]', '', str(stack[-1][3]))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        full_name = bold(app_name) + '(' + code(host) + ').' + bold(name + '()')
        error_raw = traceback.format_exception(exc_type, exc_value, exc_traceback)
        printer('Crash ' + re.sub('<.*?>', '', full_name) + ' ' + re.sub('\n', '', error_raw[-1]))
        error = 'Crash ' + full_name + '\n\n'
        for i in error_raw:
            error += html_secure(i)
        search_retry = re.search(search_retry_pattern, str(error))
        search_minor_fails = re.search(search_minor_fails_pattern, str(error))
        search_major_fails = re.search(search_major_fails_pattern, str(error))
        if search_retry:
            retry = int(search_retry.group(1)) + 10
        if search_minor_fails:
            logs = None
            retry = 10
            error = ''
        if search_major_fails:
            logs = None
            retry = 99
            error = ''

        if logs is None:
            pass
        else:
            retry = 0
        self.send_json(logs, name, error)
        return retry, func, func_locals, full_name

    def send_json(self, logs, name, error):
        json_text = ''
        if type(logs) is str:
            for character in logs:
                replaced = unidecode(str(character))
                if replaced != '':
                    json_text += replaced
                else:
                    try:
                        json_text += '[' + unicodedata.name(character) + ']'
                    except ValueError:
                        json_text += '[???]'
        if json_text:
            doc = open(name + '.json', 'w')
            doc.write(json_text)
            doc.close()
            caption = None
            if len(error) <= 1024:
                caption = error
            doc = open(name + '.json', 'rb')
            self.bot.send_document(self.dev_chat_id, doc, caption=caption, parse_mode='HTML')
        if (json_text == '' and 0 < len(error) <= 1024) or (1024 < len(error) <= 4096):
            self.bot.send_message(self.dev_chat_id, error, parse_mode='HTML')
        elif len(error) > 4096:
            separator = 4096
            split_sep = len(error) // separator
            split_mod = len(error) / separator - len(error) // separator
            if split_mod != 0:
                split_sep += 1
            for i in range(0, split_sep):
                split_error = error[i * separator : (i + 1) * separator]
                if len(split_error) > 0:
                    self.bot.send_message(self.dev_chat_id, split_error, parse_mode='HTML')
