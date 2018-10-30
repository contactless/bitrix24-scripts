#!/usr/bin/python3
import argparse
import pprint
from bitrix24 import bitrix24
from config import *
print (bitrix24)
bx24 =  bitrix24.Bitrix24(b24_domain, webhook_key=b24_webhook_key, webhook_user=b24_webhook_user)
print (bx24)

def get_user_id(name):
    parts = name.split(' ', 1)
    if len(parts) != 2:
        return

    result = bx24.call('user.search', {
        'NAME' : parts[0],
        'LAST_NAME' : parts[1],
    })

    if len(result['result']) != 1:
        return
    return result['result'][0]['ID']


def _crm_contact_list_get_one(filter):
    result = bx24.call('crm.contact.list', {
        'filter' : filter,
        'select': [ "*", "UF_*", "PHONE" , "EMAIL", "WEB"]
    })

    if result:
        pprint.pprint(result['result'])
        if len(result['result']) == 1:
            return result['result'][0]

def find_existing_contact(emails, phone_numbers=[]):
    for email in emails:
        contact = _crm_contact_list_get_one({'EMAIL' : email})
        if contact:
            return contact

    for phone in phone_numbers:
        contact = _crm_contact_list_get_one({'PHONE' : phone})
        if contact:
            return contact

def normalize_phone(phone):
    return phone.replace(' ','').replace('+','').replace('-','').replace('(','').replace(')','')


DEFAULT_USER_ID = 6


def _add_if_new_and_not_empty(existing, fields, field_name, value):
    if existing:
        if existing.get(field_name):
            return

    if value:
        fields[field_name] = value

def process_row(row, group_id=None):

    if row['Email']:
        emails = [e.strip() for e in row['Email'].split(',')]
    else:
        emails = []

    if row['Тел']:
        phone_numbers = [normalize_phone(p) for p in row['Тел'].split(',')]
    else:
        phone_numbers = []
    print(
        "find_ex", emails, phone_numbers
    )
    existing_contact = find_existing_contact(emails, phone_numbers)
    exh_user_id = get_user_id(row['Чей клиент']) or DEFAULT_USER_ID

    crm_method_fields = {}

    full_name = " ".join([part for part in [row['Имя'], row['Отчество'], row['Фамилия']] if part])


    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'NAME', full_name)
    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'LAST_NAME', row['Компания - назв'])


    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'ASSIGNED_BY_ID', exh_user_id)


    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'POST', row['Должность'])

    if not existing_contact or not existing_contact.get('EMAIL'):
        crm_method_fields['EMAIL'] = {}
        for i, email in enumerate(emails):
            crm_method_fields['EMAIL'][i] = {
                'VALUE': email,
                'VALUE_TYPE': 'WORK',
            }

    if not existing_contact or not existing_contact.get('PHONE'):
        crm_method_fields['PHONE'] = {}
        for i, phone in enumerate(phone_numbers):
            crm_method_fields['PHONE'][i] = {
                'VALUE': phone,
                'VALUE_TYPE': 'WORK',
            }

    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'WEB', row['Компания - сайт'])
    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'SOURCE_DESCRIPTION', row['Название выставки'])
    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'ADDRESS_CITY', row['Город'])

    crm_method_fields['OPENED'] = True
    crm_method_fields['SOURCE_ID'] = 'TRADE_SHOW'

    comments = ''
    if row['Сфера деят']:
        comments += 'Сфера деятельности: %s<br/>' % row['Сфера деят']
    comments += row['Комментарий']

    _add_if_new_and_not_empty(existing_contact, crm_method_fields, 'COMMENTS', comments)

    pprint.pprint(crm_method_fields)

    contact_id = None
    if existing_contact:
        contact_id = existing_contact['ID']
        result = bx24.call('crm.contact.update', {
            'id' : contact_id,
            'fields': crm_method_fields,
            'params': {
                'REGISTER_SONET_EVENT' : True,
            }
        })
        print("Update contact:")
        pprint.pprint(result)
    else:
        result = bx24.call('crm.contact.add', {
            'fields': crm_method_fields,
            'params': {
                'REGISTER_SONET_EVENT' : True,
            }
        })
        print("ADD contact:")
        pprint.pprint( result)
        contact_id = result['result']



    feed_message = "%s: \nСфера деятельности: %s\nКомментарий: %s" % (row['Название выставки'], row['Сфера деят'], row['Комментарий'])

    #  вписываем комментарии в Feed
    result = bx24.call('crm.livefeedmessage.add', {
        "fields" : {
        'POST_TITLE' : '' ,
        'MESSAGE' : feed_message,
        'ENTITYTYPEID' : 3, # 3 - контакт;
        'ENTITYID' : contact_id
        }
    })

    activity_desc = row['Наши действия после выставки']

    if activity_desc:
        task_desc = "Наши действия: %s\n\nКонтекст:\n  Сфера деятельности: %s\n  Комментарий: %s" % (activity_desc, row['Сфера деят'], row['Комментарий'])
        person_desc = full_name
        if row['Компания - назв']:
            person_desc += " (%s)" % row['Компания - назв']


        task_data = {
            'TITLE': 'Cвязаться с %s по результатам %s' % (person_desc, row['Название выставки']),
            'DESCRIPTION': task_desc,
            'RESPONSIBLE_ID': exh_user_id,
            'UF_CRM_TASK' :  {0: 'C_%s' % contact_id},
        }

        if group_id is not None:
            task_data['GROUP_ID'] = group_id

        result = bx24.call('task.item.add', {
            'arNewTaskData' : task_data
            })
        pprint.pprint(result)




import csv, sys

import logging

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MQTT retained message deleter', add_help=False)

    parser.add_argument('-g', '--group', dest='group', type=int,
                        help='Bitrix group to assign tasks to', default=None)

    parser.add_argument('filename',  type=str,
                        help='CSV filename')

    args = parser.parse_args()



    with open(args.filename) as csvfile:
        with open(args.filename + ".err", 'wt') as csvfile_err:
            csvfile.readline()
            csvfile.readline()
            reader = csv.DictReader(csvfile)

            csvfile_err.write("skip this\n")
            csvfile_err.write("skip this too\n")
            print (reader._fieldnames)
            header_written = False

            for row in reader:
                if not header_written:
                    writer = csv.DictWriter(csvfile_err, fieldnames = reader._fieldnames)
                    writer.writeheader()
                    header_written = True

                print("Will process row: ", row)
                try:
                    process_row(row, group_id=args.group)
                except:
                    logging.exception("error while processing row %s" % row)

                    writer.writerow(row)




