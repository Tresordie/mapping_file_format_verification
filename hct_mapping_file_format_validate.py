# -*- coding: utf-8 -*-
'''
@File    :   hct_mapping_file_format_validate.py
@Time    :   2024/10/18 10:25:59
@Author  :   SimonYuan 
@Version :   1.0
@Site    :   https://tresordie.github.io/
@Desc    :   None
'''


import re
import csv
from tqdm import tqdm
import pandas as pd


class hct_mapping_file_format_validate(object):
    def __init__(self, mapping_file_path):
        self.mapping_file_path = mapping_file_path
        self.mapping_file_verify_result_file_header = [
            "column_name",
            "index",
            "is_legal",
        ]
        self.export_csv_file_path = (
            mapping_file_path.split('.csv')[0] + '_verify_result.csv'
        )
        self.row_data_written = []

        self.hct_mapping_file_header = [
            "WORK_ORDER",
            "TLA",
            "CARTON_NO",
            "PALLET_NO",
            'CONTAINER_NO',
            'SKU',
            'MLB_PCBA',
            'FATP_SN',
            'NFC_UID',
            'STM32_UUID',
            'METAL_BASE',
            'FF_TRACKING_NUMBER',
            'SHIPPING_DATE',
            'FATP_INPUT_DATETIME',
            'ME_LINE_OUT_DATETIME',
            'OQC_LINE_OUTPUT_DATETIME',
        ]

        # fmt: off
        self.hct_mapping_file_column_regex_dict = {
            "WORK_ORDER"              : '^((BH-?[0-9]{6})|(PO[ML]BS?[0-9]{4}))$',
            "TLA"                     : '^[0-9]{2}-[0-9]{7}$',
            "CARTON_NO"               : '^[0-9A-Z]{6,}$',
            "PALLET_NO"               : '^[0-9A-Z]{6,}$',
            "CONTAINER_NO"            : '.*',
            "SKU"                     : '(?i)^(Chole|Cosmo)$',
            "MLB_PCBA"                : '^F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{3}$',
            "FATP_SN"                 : '^F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{3}$',
            "NFC_UID"                 : '^04.{12}000000$',
            "STM32_UUID"              : '^[0-9A-Fa-f]{24}$',
            "METAL_BASE"              : '^NA$',
            "FF_TRACKING_NUMBER"      : '.*',
            "SHIPPING_DATE"           : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            "FATP_INPUT_DATETIME"     : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            "ME_LINE_OUT_DATETIME"    : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            "OQC_LINE_OUTPUT_DATETIME": '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
        }
        # fmt: on

    def get_rows_quantity(self, csv_file):
        df = pd.read_csv(csv_file, header=None)
        return df.shape[0]

    def get_columns_quantity(self, csv_file):
        df = pd.read_csv(csv_file, header=None)
        return df.shape[1]

    def pd_read_csv_row(self, csv_path, row_num):
        df = pd.read_csv(csv_path, header=None, keep_default_na=False)
        return list(df.iloc[row_num, :])

    def creat_csv(self, csv_file_to_be_created, csv_header):
        with open(csv_file_to_be_created, "w", encoding="utf8", newline="") as f_bft:
            csv_write = csv.writer(f_bft)
            csv_write.writerow(csv_header)

    def write_row_to_csv(self, row_data, csv_path):
        with open(
            csv_path, "a", encoding="utf8", newline=""
        ) as f:  # newline='': delete empty line
            writer = csv.writer(f)
            writer.writerow(row_data)

    def pd_read_csv_column_by_name_header_set(self, csv_path, column_name):
        df = pd.read_csv(csv_path, keep_default_na=False)
        return list(df[column_name])

    def are_lists_equal(self, list1, list2):
        return list1 == list2

    def is_regex_pattern_match(self, regex_pattern, data):
        return bool(re.match(regex_pattern, data))

    def get_mapping_file_row_cnt(self):
        self.mapping_file_row_cnt = self.get_rows_quantity(self.mapping_file_path)

    def get_mapping_file_col_cnt(self):
        self.mapping_file_col_cnt = self.get_columns_quantity(self.mapping_file_path)

    def mapping_file_header_verify(self):
        factory_mapping_file_header_list = self.pd_read_csv_row(
            self.mapping_file_path, 0
        )
        # print(factory_mapping_file_header_list)

        if not self.are_lists_equal(
            factory_mapping_file_header_list, self.hct_mapping_file_header
        ):
            self.row_data_written.append('mapping_file_header')
            self.row_data_written.append('0')
            self.row_data_written.append("FALSE")

            self.write_row_to_csv(self.row_data_written, self.export_csv_file_path)
            self.row_data_written = []

    def has_unique_elements(self, input_list):
        unique_elements = set(input_list)
        return len(unique_elements) == len(input_list)

    def find_duplicate_indices(self, input_list):
        element_indices = {}
        duplicates = {}

        for index, value in enumerate(input_list):
            if value in element_indices:
                element_indices[value].append(index + 2)
            else:
                element_indices[value] = [index + 2]

        for key, indices in element_indices.items():
            if len(indices) > 1:
                duplicates[key] = indices

        return duplicates

    def has_items_unique(self, item_name):
        item_name_column_list = self.pd_read_csv_column_by_name_header_set(
            self.mapping_file_path, item_name
        )

        if not self.has_unique_elements(item_name_column_list):
            self.row_data_written.append(item_name)
            self.row_data_written.append('duplicated_check')
            self.row_data_written.append(
                self.find_duplicate_indices(item_name_column_list)
            )
            self.write_row_to_csv(self.row_data_written, self.export_csv_file_path)
            self.row_data_written = []

    def specific_column_verify(self, column_name, regex_rule):
        column_list = self.pd_read_csv_column_by_name_header_set(
            self.mapping_file_path, column_name
        )

        for i in tqdm(range(len(column_list))):
            if not self.is_regex_pattern_match(regex_rule, column_list[i]):
                self.row_data_written.append(column_name)
                self.row_data_written.append(i + 2)
                self.row_data_written.append("FALSE")
                self.write_row_to_csv(self.row_data_written, self.export_csv_file_path)
                self.row_data_written = []

    def mapping_file_verify(self):
        self.creat_csv(
            self.export_csv_file_path, self.mapping_file_verify_result_file_header
        )

        # header check
        self.mapping_file_header_verify()

        # duplicate item check
        self.has_items_unique('MLB_PCBA')
        self.has_items_unique('FATP_SN')
        self.has_items_unique('STM32_UUID')
        self.has_items_unique('NFC_UID')

        # specific column check
        for i in tqdm(range(len(self.hct_mapping_file_header))):
            self.specific_column_verify(
                self.hct_mapping_file_header[i],
                self.hct_mapping_file_column_regex_dict[
                    self.hct_mapping_file_header[i]
                ],
            )


if __name__ == '__main__':
    hct_mapping_file_format_validate = hct_mapping_file_format_validate(
        "./20241017_HCT_COSMO_MappingFile.csv"
    )

    hct_mapping_file_format_validate.mapping_file_verify()
