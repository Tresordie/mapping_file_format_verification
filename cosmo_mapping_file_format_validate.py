# -*- coding: utf-8 -*-
'''
@File    :   cosmo_mapping_file_format_validate.py
@Time    :   2024/10/18 22:18:29
@Author  :   SimonYuan 
@Version :   1.0
@Site    :   https://tresordie.github.io/
@Desc    :   None
'''


import re
import csv
from tqdm import tqdm
import pandas as pd


class cosmo_mapping_file_format_validate(object):
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

        self.cosmo_mapping_file_header = [
            'WORK_ORDER',
            'TLA',
            'SERIAL_NUMBER',
            'MLB_PCBA',
            'MLB_ICCID',
            'MLB_IMEI',
            'IDBASE64',
            'MLB_IMSI',
            'NFC_PCBA',
            'GNSS_PCBA',
            'DISPLAY',
            'BEACON_PCBA',
            'BEACON',
            'VCU_MODULE_SN',
            'TM_PCBA',
            'MOTOR_CONTROLLER_PCBA',
            'MOTOR_CONTROLLER_MODULE',
            'CABLE_LOCK_PCBA',
            'SWITCH_PCBA',
            'HOLSTER_PCBA',
            'CABLE_LOCK_MODULE',
            'BATTERY_LOCK',
            'BATTERY_BACKSTOP',
            'MOTOR_LACED_TO_WHEEL',
            'FORK',
            'BIKE_FRAME_ID',
            'OPS_SCANNABLE_CODE',
            'RIDEABLE_NAME',
            'RIDER_QR_CODE',
            'TRIANGLE_SN',
            'TRIANGLE_ID',
            'TRIANGLE_APP_ID',
            'RFID_TAG_EPC',
            'RFID_TAG_TID',
            'RFID_TAG_USER',
            'RFID_TAG_BARCODE',
            'IDBASE64_935',
            'HANDLEBAR',
            'FF_TRACKING_NUMBER',
            'CONTAINER_NUMBER',
            'SHIPPING_DATE',
            'CARTON_NO',
            'PALLET_NO',
            'FATP_INPUT_DATETIME',
            'ME_LINE_OUTPUT_DATETIME',
            'OQC_LINE_OUTPUT_DATETIME',
        ]

        # fmt: off
        self.cosmo_mapping_file_column_regex_dict = {
            'WORK_ORDER'              : '^((BH-?[0-9]{6})|(PO[ML]BS?[0-9]{4}))$',
            'TLA'                     : '^[0-9]{2}-[0-9]{7}$',
            'SERIAL_NUMBER'           : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'MLB_PCBA'                : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'MLB_ICCID'               : '^89[0-9]{16,20}$',
            'MLB_IMEI'                : '^[0-9]{15}$',
            'IDBASE64'                : '^[a-z0-9A-Z_-]{46}==$',
            'MLB_IMSI'                : '^[0-9]{15}$',
            'NFC_PCBA'                : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'GNSS_PCBA'               : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'DISPLAY'                 : '^[A-Z0-9-]+$',
            'BEACON_PCBA'             : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'BEACON'                  : '^(F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4})|(NA)$',
            'VCU_MODULE_SN'           : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'TM_PCBA'                 : '^[JF][TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'MOTOR_CONTROLLER_PCBA'   : '^[JF][TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'MOTOR_CONTROLLER_MODULE' : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'CABLE_LOCK_PCBA'         : '^(F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4})|(NA)$',
            'SWITCH_PCBA'             : '^(F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4})|(NA)$',
            'HOLSTER_PCBA'            : '^(F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4})|(NA)$',
            'CABLE_LOCK_MODULE'       : '^(F[TK][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4})|(NA)$',
            'BATTERY_LOCK'            : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'BATTERY_BACKSTOP'        : '^F[TKV][0-9]{2}[0-9]{2}[A-Z0-9]{4}[0-9][A-Z0-9]{2}[0-9]{4}$',
            'MOTOR_LACED_TO_WHEEL'    : '^[0-9]{2}-[0-9]{7}[Pp][0-9]{12}$',
            'FORK'                    : '^[0-9A-Z]+$',
            'BIKE_FRAME_ID'           : '^[0-9A-Z]+$',
            'OPS_SCANNABLE_CODE'      : '^OSC:[0-9]{19}$',
            'RIDEABLE_NAME'           : '^[0-9]{7}$',
            'RIDER_QR_CODE'           : '^ride\.lft\.to/[A-Z]+$',
            'TRIANGLE_SN'             : '^(S/N[-.][A-Z0-9]+)|(NA)$',
            'TRIANGLE_ID'             : '^([0-9A-F]{2}(-[0-9A-F]{2}){6})|([0-9]{9,10})$',
            'TRIANGLE_APP_ID'         : '^([0-9]+)|(NA)$',
            'RFID_TAG_EPC'            : '^(0x[0-9A-F]{24})|(NA)$',
            'RFID_TAG_TID'            : '^(0x[0-9a-f]{24})|(NA)$',
            'RFID_TAG_USER'           : '^(0x[0-9a-f]+)|(NA)$',
            'RFID_TAG_BARCODE'        : '^([0-9]{22})|(NA)$',
            'IDBASE64_935'            : '^[a-z0-9A-Z_-]{46}==$',
            'HANDLEBAR'               : '^([A-Z0-9]{6,})|(NA)$',
            'FF_TRACKING_NUMBER'      : '.*',
            'CONTAINER_NUMBER'        : '.*',
            'SHIPPING_DATE'           : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            'CARTON_NO'               : '^[0-9A-Z]{6,}$',
            'PALLET_NO'               : '^[0-9A-Z]{6,}$',
            'FATP_INPUT_DATETIME'     : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            'ME_LINE_OUTPUT_DATETIME' : '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            'OQC_LINE_OUTPUT_DATETIME': '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
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
            factory_mapping_file_header_list, self.cosmo_mapping_file_header
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

        if (
            column_name == 'MLB_IMEI'
            or column_name == 'MLB_IMSI'
            or column_name == 'RIDEABLE_NAME'
        ):
            for i in range(len(column_list)):
                if not self.is_regex_pattern_match(regex_rule, str(column_list[i])):
                    self.row_data_written.append(column_name)
                    self.row_data_written.append(i + 2)
                    self.row_data_written.append("FALSE")
                    self.write_row_to_csv(
                        self.row_data_written, self.export_csv_file_path
                    )
                    self.row_data_written = []
        else:
            for i in range(len(column_list)):
                if not self.is_regex_pattern_match(regex_rule, column_list[i]):
                    self.row_data_written.append(column_name)
                    self.row_data_written.append(i + 2)
                    self.row_data_written.append("FALSE")
                    self.write_row_to_csv(
                        self.row_data_written, self.export_csv_file_path
                    )
                    self.row_data_written = []

    def mapping_file_verify(self):
        self.creat_csv(
            self.export_csv_file_path, self.mapping_file_verify_result_file_header
        )

        # header check
        self.mapping_file_header_verify()

        # duplicate item check
        self.has_items_unique('SERIAL_NUMBER')
        self.has_items_unique('MLB_PCBA')
        self.has_items_unique('MLB_ICCID')
        self.has_items_unique('MLB_IMEI')
        self.has_items_unique('IDBASE64')
        self.has_items_unique('MLB_IMSI')
        self.has_items_unique('NFC_PCBA')
        self.has_items_unique('GNSS_PCBA')
        self.has_items_unique('DISPLAY')
        self.has_items_unique('BEACON_PCBA')
        self.has_items_unique('BEACON')
        self.has_items_unique('VCU_MODULE_SN')
        self.has_items_unique('TM_PCBA')
        self.has_items_unique('MOTOR_CONTROLLER_PCBA')
        self.has_items_unique('MOTOR_CONTROLLER_MODULE')
        self.has_items_unique('CABLE_LOCK_PCBA')
        self.has_items_unique('SWITCH_PCBA')
        self.has_items_unique('HOLSTER_PCBA')
        self.has_items_unique('CABLE_LOCK_MODULE')
        self.has_items_unique('BATTERY_LOCK')
        self.has_items_unique('BATTERY_BACKSTOP')
        self.has_items_unique('MOTOR_LACED_TO_WHEEL')
        self.has_items_unique('FORK')
        self.has_items_unique('BIKE_FRAME_ID')
        self.has_items_unique('OPS_SCANNABLE_CODE')
        self.has_items_unique('RIDEABLE_NAME')
        self.has_items_unique('RIDER_QR_CODE')
        self.has_items_unique('TRIANGLE_SN')
        self.has_items_unique('TRIANGLE_ID')
        self.has_items_unique('RFID_TAG_EPC')
        self.has_items_unique('RFID_TAG_TID')
        self.has_items_unique('RFID_TAG_BARCODE')
        self.has_items_unique('IDBASE64_935')
        self.has_items_unique('HANDLEBAR')

        # specific column check
        for i in tqdm(range(len(self.cosmo_mapping_file_header))):
            self.specific_column_verify(
                self.cosmo_mapping_file_header[i],
                self.cosmo_mapping_file_column_regex_dict[
                    self.cosmo_mapping_file_header[i]
                ],
            )


if __name__ == '__main__':
    cosmo_mapping_file_path_list = [
        './2024-09-11_Cosmo_MappingFile_BH-104405.csv',
        './2024-09-20_Cosmo_MappingFile_BH-104405.csv',
        './2024-09-24_Cosmo_MappingFile_BH-104405.csv',
        './2024-09-24_Cosmo_MappingFile_BH-104475.csv',
        './2024-09-24_Cosmo_MappingFile_BH-104476.csv',
        './2024-09-25_Cosmo_MappingFile_BH-104406.csv',
        './2024-10-02_Cosmo_MappingFile_BH-104401.csv',
        './2024-10-03_Cosmo_MappingFile_BH-104401.csv',
    ]

    cosmo_mapping_file_format_validate_multiple_process = []

    for i in range(len(cosmo_mapping_file_path_list)):
        cosmo_mapping_file_format_validate_multiple_process.append(
            cosmo_mapping_file_format_validate(cosmo_mapping_file_path_list[i])
        )

    for i in range(len(cosmo_mapping_file_format_validate_multiple_process)):
        cosmo_mapping_file_format_validate_multiple_process[i].mapping_file_verify()
