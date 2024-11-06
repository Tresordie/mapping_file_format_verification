#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@File    :   maple_mapping_file_format_validate.py
@Time    :   2024/11/06 16:31:56
@Author  :   SimonYuan
@Version :   1.0
@Site    :   https://tresordie.github.io/
@Desc    :   None
"""

import re
import csv
from tqdm import tqdm
import pandas as pd


class maple_mapping_file_format_validate(object):
    def __init__(self, mapping_file_path):
        self.mapping_file_path = mapping_file_path
        self.mapping_file_verify_result_file_header = [
            "column_name",
            "index",
            "is_legal",
        ]
        self.export_csv_file_path = (
            mapping_file_path.split(".csv")[0] + "_verify_result.csv"
        )
        self.row_data_written = []

        self.maple_mapping_file_header = [
            "PAIRING_UUID",
            "WORK_ORDER",
            "MANUFACTURING_ORDER",
            "SALES_ORDER_NUMBER",
            "LYFT_PO",
            "ARTICLE_NUMBER",
            "CELL_BLOCK_PN",
            "CELL_BLOCK_SN",
            "CELL_HOLDER_PN",
            "CELL_HOLDER_SN",
            "ENCLOSURE_PN",
            "ENCLOSURE_SN",
            "FIRST_CELL_SN",
            "LAST_CELL_SN",
            "BMS_NUMBER",
            "HARNESS_NUMBER",
            "MAPLE_PACK_SN",
            "PBSC PART NUMBER",
            "LYFT PART NUMBER",
            "FF_TRACKING_NUMBER",
            "CONTAINER_NUMBER",
            "SHIPPING_DATE",
            "CARTON_NO",
            "PALLET_NO",
            "FATP_INPUT_DATETIME",
            "ME_LINE_OUTPUT_DATETIME",
            "OQC_LINE_OUTPUT_DATETIME",
            "PACKING_DATETIME",
        ]

        # fmt: off
        self.maple_mapping_file_column_regex_dict = {
            "PAIRING_UUID"            : "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            "WORK_ORDER"              : "^[A-Za-z0-9]{5}-[A-Za-z0-9]{9}$",
            "MANUFACTURING_ORDER"     : "^[A-Za-z0-9]{5}-[A-Za-z0-9]{9}-[A-Za-z0-9]{3}-[A-Za-z0-9]{2}$",
            "SALES_ORDER_NUMBER"      : "^[A-Za-z0-9]{5}-[A-Za-z0-9]{9,}$",
            "LYFT_PO"                 : "^BH-\d{6,}$",
            "ARTICLE_NUMBER"          : "^[A-Za-z0-9]{12,}$",
            "CELL_BLOCK_PN"           : "^[A-Za-z0-9]{2}-[A-Za-z0-9]{7,}$",
            "CELL_BLOCK_SN"           : "^[A-Za-z0-9]{17,}$",
            "CELL_HOLDER_PN"          : "^[A-Za-z0-9]{2}-[A-Za-z0-9]{7,}$",
            "CELL_HOLDER_SN"          : "^[A-Za-z0-9]{17,}$",
            "ENCLOSURE_PN"            : "^[A-Za-z0-9]{2}-[A-Za-z0-9]{7,}$",
            "ENCLOSURE_SN"            : "^[A-Za-z0-9]{17,}$",
            "FIRST_CELL_SN"           : "^[A-Za-z0-9]{18}$",
            "LAST_CELL_SN"            : "^[A-Za-z0-9]{18}$",
            "BMS_NUMBER"              : "^[A-Za-z0-9]{17}$",
            "HARNESS_NUMBER"          : "^[A-Za-z0-9]{17,}$",
            "MAPLE_PACK_SN"           : "^[A-Za-z0-9]{17,}$",
            "PBSC PART NUMBER"        : "^PBS\d{2}-\d{6}-\d{2}$",
            "LYFT PART NUMBER"        : "^\d{2}-\d{7,}$",
            "FF_TRACKING_NUMBER"      : "^[0-9A-Za-z#]{6,}$",
            "CONTAINER_NUMBER"        : "^[0-9A-Za-z]{6,}$",
            "SHIPPING_DATE"           : "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "CARTON_NO"               : "^[0-9A-Za-z]{6,}$",
            "PALLET_NO"               : "^[0-9A-Za-z]{6,}$",
            "FATP_INPUT_DATETIME"     : "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "ME_LINE_OUTPUT_DATETIME" : "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "OQC_LINE_OUTPUT_DATETIME": "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "PACKING_DATETIME"        : "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
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
            self.row_data_written.append("mapping_file_header")
            self.row_data_written.append("0")
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
            self.row_data_written.append("duplicated_check")
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
        # self.has_items_unique("PAIRING_UUID")
        self.has_items_unique("ARTICLE_NUMBER")
        self.has_items_unique("CELL_BLOCK_PN")
        self.has_items_unique("CELL_BLOCK_SN")
        self.has_items_unique("CELL_HOLDER_PN")
        self.has_items_unique("CELL_HOLDER_SN")
        self.has_items_unique("ENCLOSURE_PN")
        self.has_items_unique("ENCLOSURE_SN")
        self.has_items_unique("FIRST_CELL_SN")
        self.has_items_unique("LAST_CELL_SN")
        self.has_items_unique("BMS_NUMBER")
        self.has_items_unique("HARNESS_NUMBER")
        self.has_items_unique("MAPLE_PACK_SN")
        self.has_items_unique("PBSC PART NUMBER")
        self.has_items_unique("LYFT PART NUMBER")

        # specific column check
        for i in tqdm(range(len(self.hct_mapping_file_header))):
            self.specific_column_verify(
                self.hct_mapping_file_header[i],
                self.hct_mapping_file_column_regex_dict[
                    self.hct_mapping_file_header[i]
                ],
            )


if __name__ == "__main__":
    maple_mapping_file_path_list = [
        "./20241017_HCT_COSMO_MappingFile.csv",
        "./20241030_HCT_COSMO_MappingFile.csv",
    ]

    maple_mapping_file_format_validate_multiple_process = []

    for i in range(len(maple_mapping_file_path_list)):
        maple_mapping_file_format_validate_multiple_process.append(
            maple_mapping_file_format_validate(maple_mapping_file_path_list[i])
        )

    for i in range(len(maple_mapping_file_format_validate_multiple_process)):
        maple_mapping_file_format_validate_multiple_process[i].mapping_file_verify()
