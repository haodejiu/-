# fingerprint_server_with_xiaoai.py（简化版）- 修复时间戳问题
# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import csv
import requests
import sys
import threading
import time
import os  # 用于路径处理

# ========== 配置参数 ==========
# 基础路径：使用脚本所在目录（避免路径错误）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# MQTT配置
MQTT_BROKER = "localhost"          
MQTT_PORT = 1883                   
MQTT_TOPIC = "fingerprint/data"    
LOG_FILE = os.path.join(BASE_DIR, "fingerprint_log.csv")  # 原日志文件
# 考勤文件（简化版：去掉date和duration_minutes）
ATTENDANCE_FILE = os.path.join(BASE_DIR, "attendance.csv")  

# Home Assistant 配置
HA_IP = "192.168.3.120"
HA_PORT = 8123
HA_TOKEN = "HA令牌"
PLAY_ENTITY_ID = "notify.xiaomi_cn_759289298_l04m_play_text_a_5_1"

# 去重配置
DUPLICATE_INTERVAL = 10
recent_recognitions = {}  
# 考勤状态（仅用于判断签到/签退）
attendance_state = {}  
STATE_LOCK = threading.Lock()
AUTO_SIGNOUT_DEVICE_ID = "auto_midnight"
# =============================

# ========== 1. 考勤文件初始化（简化版） ==========
def init_attendance_file():
    """初始化考勤文件，表头为：timestamp,device_id,finger_id,score,user,action"""
    try:
        if not os.path.exists(ATTENDANCE_FILE):
            with open(ATTENDANCE_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp',     # 完整时间（YYYY/MM/DD HH:MM:SS）
                    'device_id',     # 设备ID
                    'finger_id',     # 原始指纹ID
                    'score',         # 匹配分数
                    'user',          # 用户名
                    'action',        # 动作（签到/签退）
                ])
            print(f"[考勤文件] 已创建 {ATTENDANCE_FILE}，简化版格式")
        else:
            # 检查现有文件格式
            try:
                with open(ATTENDANCE_FILE, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    header = next(reader, [])
                    print(f"[考勤文件] 考勤文件已存在: {ATTENDANCE_FILE}，表头: {header}")
            except Exception as e:
                print(f"[考勤文件检查异常] {e}")
    except Exception as e:
        print(f"[考勤文件初始化异常] {e}")
        # 尝试重新创建
        try:
            with open(ATTENDANCE_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'device_id', 'finger_id', 'score', 'user', 'action'
                ])
            print(f"[考勤文件] 异常后重新创建成功")
        except Exception as retry_error:
            print(f"[考勤文件重试失败] {retry_error}")


# ========== 2. 写入考勤记录（简化版） ==========
def append_attendance_row(timestamp_str, device_id, finger_id, score, user, action):
    """
    追加考勤记录到CSV（简化版）
    :param timestamp_str: 时间戳字符串
    :param device_id: 设备ID
    :param finger_id: 原始指纹ID
    :param score: 匹配分数
    :param user: 用户名
    :param action: 动作（签到/签退）
    """
    # 确保文件已初始化
    init_attendance_file()
    
    try:
        with open(ATTENDANCE_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp_str,     # 完整时间
                device_id,         # 设备ID
                finger_id,         # 原始指纹ID
                score,             # 匹配分数
                user,              # 用户名
                action,            # 签到/签退
            ])
        print(f"[考勤记录] 已记录 {user} (指纹ID:{finger_id}, 分数:{score}) 的{action}，时间：{timestamp_str}")
        
    except PermissionError:
        print(f"[考勤记录] 权限不足，无法写入文件: {ATTENDANCE_FILE}")
    except Exception as e:
        print(f"[考勤记录写入异常] 用户{user}：{e}")
        # 尝试重新初始化文件
        try:
            init_attendance_file()
            with open(ATTENDANCE_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp_str, device_id, finger_id, score, user, action
                ])
            print(f"[考勤记录] 重试后写入成功")
        except Exception as retry_error:
            print(f"[考勤记录重试失败] {retry_error}")


# ========== 3. 其他工具函数 ==========
def get_current_timestamp():
    """获取当前时间的标准格式字符串"""
    return datetime.now().strftime('%Y/%m/%d %H:%M:%S')

def ha_notify_speak(text):
    """通过Home Assistant播报文本"""
    if not text or not text.strip():
        print("[HA播报] 播报内容为空，跳过")
        return False
        
    url = f"http://{HA_IP}:{HA_PORT}/api/services/notify/send_message"
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {"entity_id": PLAY_ENTITY_ID, "message": text}
    
    try:
        resp = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
        resp.raise_for_status()
        print(f"[HA播报成功] {text}")
        return True
    except requests.exceptions.Timeout:
        print(f"[HA播报超时] 播报内容: {text}")
        return False
    except requests.exceptions.ConnectionError:
        print(f"[HA播报连接异常] 无法连接到Home Assistant: {HA_IP}:{HA_PORT}")
        return False
    except requests.exceptions.HTTPError as e:
        print(f"[HA播报HTTP异常] 状态码: {e.response.status_code}, 播报内容: {text}")
        return False
    except Exception as e:
        print(f"[HA播报异常] {e}, 播报内容: {text}")
        return False

def say(text):
    return ha_notify_speak(text)

def today_str():
    return datetime.now().strftime('%Y-%m-%d')

def load_today_status():
    """加载当天状态用于判断签到/签退"""
    state = {}
    if not os.path.exists(ATTENDANCE_FILE):
        return state
    try:
        with open(ATTENDANCE_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 只关注当天的记录
                try:
                    timestamp_str = row['timestamp'].strip()
                    # 解析时间戳，支持多种格式
                    if '/' in timestamp_str:
                        record_time = datetime.strptime(timestamp_str, '%Y/%m/%d %H:%M:%S')
                    else:
                        # 如果是数字时间戳，尝试转换
                        try:
                            record_time = datetime.fromtimestamp(float(timestamp_str))
                        except:
                            continue
                    
                    if record_time.date() != datetime.now().date():
                        continue
                    
                    user = row['user'].strip()
                    action = row['action'].strip()
                    
                    # 记录用户最后一次动作
                    state[user] = {
                        'last_action': action,
                        'state_date': today_str()
                    }
                except Exception as e:
                    print(f"[状态加载] 解析时间戳失败: {timestamp_str}, 错误: {e}")
                    continue
    except Exception as e:
        print(f"[加载当天状态异常] {e}")
    return state

def handle_checkin(user: str):
    """处理签到播报 - 简化版"""
    message = f"{user}签到成功！"
    say(message)

def handle_signout(user: str):
    """处理签退播报 - 简化版"""
    message = f"{user}签退成功！"
    say(message)

def decide_action(user):
    """判断当天是签到还是签退"""
    info = attendance_state.get(user)
    if info and info.get('state_date') != today_str():
        # 跨天重置状态
        attendance_state[user] = {'last_action': '签退', 'state_date': today_str()}
        info = attendance_state[user]
    
    # 无记录或上次是签退 → 本次签到
    # 上次是签到 → 本次签退
    return '签到' if (not info or info.get('last_action') == '签退') else '签退'

def is_duplicate(fingerprint_id):
    """去重逻辑"""
    current_time = datetime.now().timestamp()
    last_time = recent_recognitions.get(fingerprint_id, 0)
    if current_time - last_time < DUPLICATE_INTERVAL:
        print(f"[重复过滤] 忽略{DUPLICATE_INTERVAL}秒内的重复识别：{fingerprint_id}")
        return True
    recent_recognitions[fingerprint_id] = current_time
    return False

def update_test_log(device_id, new_entry):
    """更新测试日志：清理旧记录并添加新记录"""
    try:
        # 读取现有日志
        try:
            with open(LOG_FILE, 'r', encoding='utf-8-sig') as f:
                lines = list(csv.reader(f))
                header = lines[0] if lines else ['timestamp', 'device_id', 'finger_id', 'score']
                records = lines[1:]
        except FileNotFoundError:
            header = ['timestamp', 'device_id', 'finger_id', 'score']
            records = []
        
        # 清理该设备的旧测试记录
        cleaned_records = [row for row in records if not (
            len(row) > 2 and row[1] == device_id and row[2] in ['800', '900']
        )]
        
        # 写入清理后的日志
        with open(LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(cleaned_records)
            writer.writerow(new_entry)
            
        print(f"[测试日志] 已更新设备 {device_id} 的测试记录")
        
    except Exception as e:
        print(f"[测试日志更新异常] 设备 {device_id}: {e}")
        # 异常情况下尝试直接追加
        try:
            with open(LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                csv.writer(f).writerow(new_entry)
            print(f"[测试日志] 异常后直接追加成功")
        except Exception as append_error:
            print(f"[测试日志追加异常] {append_error}")

def setup_log_file():
    """初始化指纹识别日志文件"""
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'x', newline='', encoding='utf-8-sig') as f:
                csv.writer(f).writerow(['timestamp', 'device_id', 'finger_id', 'score'])
            print(f"[日志文件] 已创建指纹识别日志: {LOG_FILE}")
        else:
            print(f"[日志文件] 指纹识别日志已存在: {LOG_FILE}")
    except Exception as e:
        print(f"[日志文件初始化异常] {e}")
        # 尝试使用w模式创建
        try:
            with open(LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                csv.writer(f).writerow(['timestamp', 'device_id', 'finger_id', 'score'])
            print(f"[日志文件] 异常后重新创建成功")
        except Exception as retry_error:
            print(f"[日志文件重试失败] {retry_error}")

def auto_signout_for_today(cutoff_dt=None):
    """自动签退逻辑 - 简化版"""
    with STATE_LOCK:
        if cutoff_dt is None:
            cutoff_dt = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
        timestamp_str = cutoff_dt.strftime('%Y/%m/%d %H:%M:%S')
        
        for user, info in list(attendance_state.items()):
            if not info or info.get('state_date') != today_str() or info.get('last_action') != '签到':
                continue
                
            try:
                # 更新内存状态
                attendance_state[user] = {
                    'last_action': '签退', 
                    'state_date': today_str()
                }
                
                # 写入考勤CSV（自动签退时finger_id和score设为空）
                append_attendance_row(
                    timestamp_str=timestamp_str,
                    device_id=AUTO_SIGNOUT_DEVICE_ID,
                    finger_id='',  # 自动签退无指纹ID
                    score='',      # 自动签退无分数
                    user=user,
                    action='签退'
                )
                
                print(f"[自动签退] {user} | {timestamp_str}")
                
                # 自动签退的播报 - 简化版
                say(f"{user}签退成功！")
                    
            except Exception as e:
                print(f"[自动签退异常] 用户{user}：{e}")
                # 异常情况下仍然记录签退
                try:
                    append_attendance_row(
                        timestamp_str=timestamp_str,
                        device_id=AUTO_SIGNOUT_DEVICE_ID,
                        finger_id='',
                        score='',
                        user=user,
                        action='签退'
                    )
                    attendance_state[user] = {
                        'last_action': '签退', 
                        'state_date': today_str()
                    }
                except Exception as write_error:
                    print(f"[自动签退记录异常] 用户{user}：{write_error}")

def start_midnight_auto_signout():
    """自动签退线程 - 保留"""
    def worker():
        while True:
            try:
                now = datetime.now()
                target = now.replace(hour=23, minute=59, second=59, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                
                # 计算等待时间
                wait_seconds = max(1, int((target - now).total_seconds()))
                print(f"[自动签退] 下次执行时间：{target.strftime('%Y-%m-%d %H:%M:%S')}，等待 {wait_seconds} 秒")
                
                time.sleep(wait_seconds)
                
                # 执行自动签退
                print(f"[自动签退] 开始执行自动签退，时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                auto_signout_for_today(cutoff_dt=target)
                print(f"[自动签退] 自动签退执行完成")
                
            except Exception as e:
                print(f"[自动签退线程异常] {e}")
                # 异常后等待1分钟再继续
                time.sleep(60)
            else:
                # 正常执行后等待1秒
                time.sleep(1)
    
    # 启动自动签退线程
    auto_signout_thread = threading.Thread(target=worker, daemon=True, name="AutoSignoutThread")
    auto_signout_thread.start()
    print(f"[自动签退] 自动签退线程已启动，线程ID：{auto_signout_thread.ident}")


# ========== 4. 核心：MQTT消息处理（修复时间戳问题） ==========
def on_message(client, userdata, msg):
    """MQTT消息处理函数：处理指纹识别消息"""
    try:
        # 解析MQTT消息
        data = json.loads(msg.payload.decode('utf-8'))
        
        # 修复时间戳问题：始终使用当前系统时间
        current_timestamp = get_current_timestamp()
        device_id = data.get('device', 'unknown_device')
        raw_id = data.get('id', -1)
        score = data.get('score', 0)

        print(f"[调试] 收到MQTT消息: device={device_id}, id={raw_id}, score={score}")

        # 判断是否为测试消息
        is_test = str(raw_id) in ['800', '900']
        display_id = str(raw_id) if is_test else "unknown"
        
        # 从配置文件中导入指纹ID到姓名的映射
        try:
            from fingerprint_config import FINGERPRINT_NAMES
            display_id = str(raw_id) if is_test else FINGERPRINT_NAMES.get(raw_id, "unknown")
            print(f"[调试] 用户映射: 原始ID {raw_id} -> 用户名 {display_id}")
        except ImportError:
            print("[警告] 无法导入FINGERPRINT_NAMES配置，将使用原始ID")
        
        log_entry = [current_timestamp, device_id, display_id, score]

        if is_test:
            # 处理测试消息
            update_test_log(device_id, log_entry)
            print(f"[测试消息] 设备 {device_id} | ID: {display_id} | 分数: {score}")
        else:
            # 写入原指纹日志
            try:
                with open(LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                    csv.writer(f).writerow(log_entry)
                print(f"[指纹记录] 设备 {device_id} | 姓名: {display_id} | 原始ID: {raw_id} | 分数: {score}")
            except Exception as log_error:
                print(f"[指纹日志写入异常] {log_error}")

            # 处理签到/签退（排除未知用户和重复）
            if display_id != "unknown" and not is_duplicate(raw_id):
                with STATE_LOCK:
                    action = decide_action(display_id)
                    print(f"[调试] 用户 {display_id} 执行动作: {action}")

                    # ========== 签到逻辑 ==========
                    if action == '签到':
                        # 写入考勤CSV
                        append_attendance_row(
                            timestamp_str=current_timestamp,
                            device_id=device_id,
                            finger_id=str(raw_id),  # 记录原始指纹ID
                            score=str(score),       # 记录匹配分数
                            user=display_id,
                            action='签到'
                        )

                        # 更新内存状态
                        attendance_state[display_id] = {
                            'last_action': '签到',
                            'state_date': today_str()
                        }

                        # 播报 - 简化版
                        handle_checkin(display_id)

                    # ========== 签退逻辑 ==========
                    else:
                        # 写入考勤CSV
                        append_attendance_row(
                            timestamp_str=current_timestamp,
                            device_id=device_id,
                            finger_id=str(raw_id),  # 记录原始指纹ID
                            score=str(score),       # 记录匹配分数
                            user=display_id,
                            action='签退'
                        )
                        
                        # 更新内存状态
                        attendance_state[display_id] = {
                            'last_action': '签退', 
                            'state_date': today_str()
                        }
                        
                        # 播报 - 简化版
                        handle_signout(display_id)
            else:
                # 未知用户提示
                if not is_duplicate(raw_id):
                    try:
                        say("您的指纹还没有录入我们实验室的指纹库哦，N6-506智能物联网实验室欢迎您的加入!")
                        print(f"[未知指纹] 设备 {device_id} | 未录入ID: {raw_id} | 已提示用户")
                    except Exception as speak_error:
                        print(f"[未知用户播报异常] {speak_error}")
                        
    except json.JSONDecodeError as e:
        print(f"[MQTT消息解析异常] JSON格式错误: {e}")
        print(f"[MQTT消息] 原始数据: {msg.payload}")
    except Exception as e:
        print(f"[MQTT消息处理异常] {e}")
        import traceback
        traceback.print_exc()


# ========== 主函数（初始化+启动服务） ==========
if __name__ == "__main__":
    # 独立播报模式
    if len(sys.argv) >= 2 and sys.argv[1] == "--speak":
        msg = " ".join(sys.argv[2:])
        ha_notify_speak(msg)
        sys.exit(0)
    
    print("=" * 60)
    print("智能物联网实验室指纹签到系统启动中...（简化版-修复时间戳）")
    print("=" * 60)
    
    try:
        # 正常模式：初始化文件+启动服务
        print("[初始化] 正在初始化系统文件...")
        setup_log_file()          # 初始化原指纹日志
        init_attendance_file()    # 初始化考勤文件（简化版）
        
        print("[初始化] 正在加载当天考勤状态...")
        attendance_state.update(load_today_status())  # 加载当天状态
        
        print("[初始化] 正在启动自动签退服务...")
        start_midnight_auto_signout()  # 启动自动签退线程

        # 启动MQTT客户端
        print("[初始化] 正在连接MQTT服务...")
        client = mqtt.Client()
        client.on_connect = lambda c, u, f, rc: (
            print(f"[MQTT] 连接成功，代码：{rc}") if rc == 0 else print(f"[MQTT] 连接失败，代码：{rc}"), 
            c.subscribe(MQTT_TOPIC) if rc == 0 else None
        )
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT)
        
        print("=" * 60)
        print("系统启动完成！")
        print(f"MQTT监听已启动，主题: {MQTT_TOPIC}")
        print(f"考勤文件路径: {ATTENDANCE_FILE}（简化版格式）")
        print("=" * 60)
        
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n[系统] 收到中断信号，正在关闭...")
        sys.exit(0)
    except Exception as e:
        print(f"[系统启动异常] {e}")
        sys.exit(1)
