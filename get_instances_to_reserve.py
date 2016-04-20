# -*- coding: utf-8 -*-

import csv


def now():
    ret = []
    f = open('data/instance_to_reserve.csv', 'r')
    reader = csv.reader(f)
    header_row = True
    for row in reader:
        if header_row:
            header_row = False
        else:
            ret.append(row)
    f.close()
