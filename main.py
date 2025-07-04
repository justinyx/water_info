import requests
from PIL import Image
import datetime
import pytesseract
import tempfile
import os
import cv2
import numpy as np
import pymysql
from pymysql import MySQLError
import logging
import schedule
import time
from decimal import Decimal, getcontext, ROUND_DOWN, InvalidOperation
import re
import datetime

from config import DB_CONFIG

db_config = DB_CONFIG
# 设置默认精度，根据你的需求调整
getcontext().prec = 10

# 设置 Tesseract 路径（根据实际安装路径修改）
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# OCR 配置常量
OCR_CONFIG = '--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789.:- '

# 水位URL
level_url = 'http://www.jsswj.com.cn:88/jsswxxSSI/static/map/chart/0/f5ae2cba035843f4bca62749cff74106_list.png?t=' + str(
    int(datetime.datetime.now().timestamp() * 1000))

# 雨情URL
rain_url = ('http://www.jsswj.com.cn:88/jsswxxSSI/static/map/chart/0/ad95798ccba3434d8'
            'b0dbe0ea22d0659_list.png?t=') + str(
    int(datetime.datetime.now().timestamp() * 1000))

# 日志设置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_volume(station_name: str, level: Decimal) -> Decimal:
    """
       根据水位站名和水位值，从 iot_capacity 表中获取对应的体积值。
       参数:
           station_name (str): 水位站名
           level (Decimal): 水位值
       返回:
           Decimal: 查询到的体积值
       """
    # 获取整数部分
    integer_part = level.to_integral(rounding=ROUND_DOWN)

    # 获取小数部分
    fractional_part = level - integer_part

    # 格式化为两位小数字符串
    format_str = f"{fractional_part:.2f}"

    # 提取小数点后的两位数字
    number_parts = re.findall(r'\.(\d)(\d)', format_str)
    if not number_parts:
        raise ValueError("无法解析小数格式")

    number_part1, number_part2 = number_parts[0]

    # 构建 SQL 查询
    if number_part2 == '0':
        select_sql = f"SELECT F{number_part1} AS volume FROM iot_capacity WHERE station_name=%s AND sw=%s"
    else:
        select_sql = f"SELECT F{number_part1} + l{number_part2} AS volume FROM iot_capacity WHERE station_name=%s AND sw=%s"

    # 建立数据库连接
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:

            # 执行 SQL 查询
            cursor.execute(select_sql, (station_name, integer_part))
            result = cursor.fetchone()
            if result is None:
                return Decimal('0')
            else:
                # 将元组结果转换为字典
                volume_value = result[0]  # 假设 volume 是查询结果的第一个字段
                return Decimal(volume_value)

    except MySQLError as e:
        logging.error(f"数据库查询错误: {e}")
        raise
    finally:
        connection.close()


def preprocess_image(cv_image):
    """
    使用 cv2.resize 将图像尺寸放大为原来的3倍；
使用 cv2.cvtColor 将图像从BGR格式转换为灰度图；
使用 cv2.threshold 结合Otsu算法对图像进行二值化处理；
使用 cv2.medianBlur 对二值图像进行中值滤波降噪；
返回处理后的二值图像。
"""
    resized_image = cv2.resize(cv_image, None, fx=3, fy=3, interpolation=cv2.INTER_LINEAR)
    gray = cv2.cvtColor(resized_image, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    binary = cv2.medianBlur(binary, 3)
    return binary


def insert_water_level_data(cursor, connection, time_obj, water_level):
    """插入水位数据到数据库"""
    if not isinstance(time_obj, (str, datetime.datetime)):
        logging.error("时间参数类型错误")
        return

    if not isinstance(water_level, Decimal):
        logging.error("水位参数类型错误")
        return

    try:
        # 获取水库容积
        water_volume = get_volume('横山水库', water_level)
        # 使用 INSERT IGNORE 简化逻辑，避免先查后插

        insert_update_sql = """
        INSERT INTO iot_device_data_f001 (dttime, a1,a2) 
        VALUES (%s, %s,%s) 
        ON DUPLICATE KEY UPDATE a1 = VALUES(a1),a2 = VALUES(a2)
        """
        cursor.execute(insert_update_sql, (time_obj, water_level, water_volume))

        if cursor.rowcount > 0:
            connection.commit()
            logging.info(f'已插入数据库: 时间 {time_obj}, 水位 {water_level} 米')
        else:
            logging.warning(f'该时间已存在，未插入: 时间 {time_obj}')
    except MySQLError as e:
        print(f"数据库插入错误: {e}")
        connection.rollback()


def fetch_and_process_image(url):
    """下载图片并进行OCR处理"""
    ocr_result = ""
    try:
        response = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"网络请求失败: {e}")
        return ""

    if response.status_code == 200:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
            tmpfile.write(response.content)
            tmpfile_path = tmpfile.name

        try:
            # 打开图片并识别文字
            image = Image.open(tmpfile_path)
            cv_image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)

            # 图像预处理
            binary = preprocess_image(cv_image)

            # 保存预处理后的图像到临时文件（供OCR使用）
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_processed:
                    processed_dumpfile_path: str = tmp_processed.name
                cv2.imwrite(processed_dumpfile_path, binary)

                # OCR 识别
                try:
                    ocr_result = pytesseract.image_to_string(Image.open(processed_dumpfile_path), config=OCR_CONFIG)
                except Exception as e:
                    print(f"OCR识别失败: {e}")
                    ocr_result = ""
                finally:
                    try:
                        os.remove(processed_dumpfile_path)
                    except Exception as e:
                        print(f"无法删除临时OCR文件: {e}")
            except Exception as e:
                print(f"图像处理过程中发生错误: {e}")

        except Exception as e:
            print(f"图像处理过程中发生错误: {e}")
        finally:
            try:
                os.remove(tmpfile_path)
            except Exception as e:
                print(f"无法删除原始图片临时文件: {e}")
    else:
        print('无法下载图片')
    return ocr_result


def parse_ocr_rain(ocr_result):
    """解析OCR识别的雨量数据"""
    current_year = datetime.datetime.now().year
    parsed_data: list[tuple[datetime.datetime, Decimal, Decimal]] = []
    lines = ocr_result.strip().split('\n')

    for line in lines:
        parts = line.split()
        if len(parts) != 3:
            continue

        date_time_str, rainfall, cumulative_rainfall = parts

        # 校验时间字符串是否为6位数字
        if not (len(date_time_str) == 10):
            print(f"Invalid date format: {date_time_str}")
            continue

        try:
            fixed_time_str = f"{date_time_str[:5]} {date_time_str[5:]}"
            full_time_str = f"{current_year}-{fixed_time_str}"
            time_obj = datetime.datetime.strptime(full_time_str, '%Y-%m-%d %H:00')
        except ValueError as e:
            print(f"Error parsing time from line: {line}, error: {e}")
            continue

        try:
            rainfall_val = Decimal(rainfall)
            cumulative_rainfall_val = Decimal(cumulative_rainfall)
        except ValueError as e:
            print(f"Error converting rainfall values in line: {line}, error: {e}")
            continue
        print(f"时间：{time_obj}, 雨量：{rainfall_val}毫米, 累计雨量:{cumulative_rainfall_val}毫米")
        parsed_data.append((time_obj, rainfall_val, cumulative_rainfall_val))

    return parsed_data


def parse_ocr_level(ocr_result):
    """解析OCR识别的 水位数据"""
    valid_records: list[tuple[datetime.datetime, Decimal, Decimal]] = []
    if not ocr_result.strip():
        logging.info("OCR识别结果为空")
    else:

        lines = ocr_result.strip().split('\n')
        current_time = datetime.datetime.now()
        valid_water_level_range = (0, 40)

        try:

            for line in lines:
                parts = line.split()
                if len(parts) != 2:
                    print(f"忽略无效行: {line}")
                    continue

                time_str, water_level_str = parts
                try:
                    # 校正时间格式
                    fixed_time_str = f"{time_str[:5]} {time_str[5:]}"
                    current_year = current_time.year
                    full_time_str = f"{current_year}-{fixed_time_str}"

                    time_obj = datetime.datetime.strptime(full_time_str, '%Y-%m-%d %H:00')
                    water_level = Decimal(water_level_str)
                    print(f'成功解析: 时间 {time_obj}, 水位 {water_level} 米')
                    if valid_water_level_range[0] < water_level < valid_water_level_range[1]:
                        # 获取水库容积
                        water_volume = get_volume('横山水库', water_level)
                        print(f'获取水库容积: 时间 {time_obj}, 水位 {water_level} 米, 容积 {water_volume} 万立方米')
                        valid_records.append((time_obj, water_level, water_volume))
                    else:
                        print(f'无效数据: 时间 {time_obj}, 水位 {water_level} 米')

                except ValueError as ve:
                    print(f'解析失败: {line} - 错误：{ve}')
            # 排序（升序）,按dttime升序插入数据库
            valid_records.sort(key=lambda x: x[0])

        except MySQLError as e:
            print(f"数据库错误: {e}")

    return valid_records


def insert_rain_data(data, flag_type: int):
    """
    # 插入或更新雨情数据到数据库
    :param data:
    :param flag_type:
    :return:
    """
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            if flag_type == 2:
                sql = """
                INSERT INTO iot_device_data_f001 (dttime, a3, a4)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE a3 = VALUES(a3), a4 = VALUES(a4)
                """
            else:
                sql = """
                INSERT INTO iot_device_data_f001 (dttime, a1, a2)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE a1 = VALUES(a1), a2 = VALUES(a2)
                """
            cursor.executemany(sql, data)
        connection.commit()
    finally:
        connection.close()


def main():
    """主函数"""
    # 解析水位数据
    ocr_result = fetch_and_process_image(level_url)

    parsed_data = parse_ocr_level(ocr_result)
    insert_rain_data(parsed_data, 1)
    # 解析雨情数据
    ocr_result = fetch_and_process_image(rain_url)
    print(f"解析结果: {ocr_result}")
    parsed_data = parse_ocr_rain(ocr_result)
    insert_rain_data(parsed_data, 2)


if __name__ == "__main__":
    main()
