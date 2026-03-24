# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import csv
import sys
import threading
import time
import os
import queue
import subprocess
import re
import shutil

# ========== 配置参数 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# MQTT 配置
MQTT_BROKER = "localhost"          
MQTT_PORT = 1883                   
MQTT_TOPIC = "fingerprint/data"    
LOG_FILE = os.path.join(BASE_DIR, "fingerprint_log.csv")
ATTENDANCE_FILE = os.path.join(BASE_DIR, "attendance.csv")  

# TTS 模型配置
# 请确保路径正确，且存在对应的 .onnx 和 .json 文件
TTS_MODEL_PATH = "/home/pi/piper-models/zh_CN-huayan-medium.onnx"
TTS_CACHE_DIR = os.path.join(BASE_DIR, "tts_cache")
TTS_BIN_CANDIDATES = [
    os.path.join(BASE_DIR, "myvenv", "bin", "piper"),
    "/home/pi/zw/myvenv/bin/piper",
    shutil.which("piper") or "piper",
]

# 去重配置 (秒)
DUPLICATE_INTERVAL = 10
recent_recognitions = {}  
attendance_state = {}  
STATE_LOCK = threading.Lock()
AUTO_SIGNOUT_DEVICE_ID = "auto_midnight"
# =============================

# ========== 本地 TTS 引擎类 (缓存优先版：首次生成，后续极速播放) ==========
class LocalTTS:
    def __init__(self, model_path):
        self.model_path = model_path
        self.audio_queue = queue.Queue()
        self.cache_dir = TTS_CACHE_DIR
        self.is_running = False
        self.gen_lock = threading.Lock()
        self.piper_bin = None

    def _detect_piper_bin(self):
        """选择可用且支持 TTS 参数的 piper 可执行文件。"""
        for candidate in TTS_BIN_CANDIDATES:
            if not candidate:
                continue
            if not os.path.exists(candidate) and "/" in candidate:
                continue
            try:
                result = subprocess.run(
                    [candidate, "--help"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                help_text = (result.stdout or "") + "\n" + (result.stderr or "")
                if ("--model" in help_text) or ("-m MODEL" in help_text):
                    self.piper_bin = candidate
                    return True
            except Exception:
                continue
        return False
        
    def check_model(self):
        """检查模型文件"""
        if not os.path.exists(self.model_path):
            print(f"[TTS 错误] 模型文件不存在：{self.model_path}")
            return False
        config_path = self.model_path + ".json"
        if not os.path.exists(config_path):
            print(f"[TTS 警告] 配置文件不存在：{config_path}，尝试继续...")
        os.makedirs(self.cache_dir, exist_ok=True)
        if not self._detect_piper_bin():
            print("[TTS 错误] 未找到可用的 Piper TTS 可执行文件")
            return False
        print(f"[TTS] 使用引擎：{self.piper_bin}")
        print(f"[TTS] 模型检查通过：{os.path.basename(self.model_path)}")
        return True

    def _safe_name(self, text):
        return re.sub(r'[\\/:*?"<>|]+', "_", text.strip())

    def _cache_path(self, text):
        return os.path.join(self.cache_dir, f"{self._safe_name(text)}.wav")

    def _generate_wav(self, text, out_file):
        cmd = [self.piper_bin, "-m", self.model_path, "-f", out_file]
        result = subprocess.run(
            cmd,
            input=text.encode("utf-8"),
            capture_output=True,
            timeout=30
        )
        if result.returncode != 0:
            if result.stderr:
                print(f"[TTS 错误] {result.stderr.decode('utf-8')[:200]}")
            return False
        return os.path.exists(out_file) and os.path.getsize(out_file) > 44

    def ensure_cached_phrase(self, text):
        """确保文本对应语音已缓存。"""
        target = self._cache_path(text)
        if os.path.exists(target) and os.path.getsize(target) > 44:
            return target

        # 用锁避免并发重复生成同一个文件
        with self.gen_lock:
            if os.path.exists(target) and os.path.getsize(target) > 44:
                return target
            ok = self._generate_wav(text, target)
            if ok:
                print(f"[TTS] 已缓存: {text}")
                return target
            try:
                if os.path.exists(target):
                    os.remove(target)
            except Exception:
                pass
            return None

    def ensure_user_cache(self, user):
        """首次遇到用户时，预生成签到+签退两条缓存。"""
        checkin_text = f"{user}签到成功"
        signout_text = f"{user}签退成功"
        self.ensure_cached_phrase(checkin_text)
        self.ensure_cached_phrase(signout_text)

    def ensure_user_cache_async(self, user):
        threading.Thread(
            target=self.ensure_user_cache,
            args=(user,),
            daemon=True,
            name=f"TTSCache-{user}"
        ).start()

    def _play_worker(self):
        """后台线程：从队列取音频文件并播放"""
        while self.is_running:
            try:
                wav_file = self.audio_queue.get(timeout=1)
                if wav_file is None:
                    break
                subprocess.run(["aplay", "-q", wav_file], check=False, timeout=30)
                self.audio_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[TTS 播放错误] {e}")

    def start(self):
        """启动系统"""
        self.is_running = True
        threading.Thread(target=self._play_worker, daemon=True, name="TTSPlayer").start()
        print("[TTS] ✅ 播放线程已启动")

    def speak(self, text):
        """非阻塞播报：优先播放缓存，没有则生成后播放。"""
        if not text or not text.strip():
            return

        wav_file = self.ensure_cached_phrase(text)
        if wav_file:
            self.audio_queue.put(wav_file)
        else:
            print(f"[TTS 警告] 无法播报：{text}")

    def stop(self):
        self.is_running = False
        self.audio_queue.put(None)

# 实例化 TTS 引擎
TTS_ENGINE = LocalTTS(TTS_MODEL_PATH)
# ===========================================

# ========== 1. 考勤文件初始化 ==========
def init_attendance_file():
    try:
        if not os.path.exists(ATTENDANCE_FILE):
            with open(ATTENDANCE_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'device_id', 'finger_id', 'score', 'user', 'action'])
            print(f"[考勤文件] 已创建 {ATTENDANCE_FILE}")
    except Exception as e:
        print(f"[考勤文件初始化异常] {e}")

# ========== 2. 写入考勤记录 ==========
def append_attendance_row(timestamp_str, device_id, finger_id, score, user, action):
    init_attendance_file()
    try:
        with open(ATTENDANCE_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp_str, device_id, finger_id, score, user, action])
        # 移除打印以减少 IO 延迟
        # print(f"[考勤记录] {user} ({action}) @ {timestamp_str}")
    except Exception as e:
        print(f"[考勤记录写入异常] {e}")

# ========== 3. 工具函数 ==========
def get_current_timestamp():
    return datetime.now().strftime('%Y/%m/%d %H:%M:%S')

def say(text):
    """统一播报接口"""
    TTS_ENGINE.speak(text)

def today_str():
    return datetime.now().strftime('%Y-%m-%d')

def load_today_status():
    state = {}
    if not os.path.exists(ATTENDANCE_FILE):
        return state
    try:
        with open(ATTENDANCE_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    timestamp_str = row['timestamp'].strip()
                    if '/' in timestamp_str:
                        record_time = datetime.strptime(timestamp_str, '%Y/%m/%d %H:%M:%S')
                    else:
                        continue
                    if record_time.date() != datetime.now().date():
                        continue
                    user = row['user'].strip()
                    action = row['action'].strip()
                    state[user] = {'last_action': action, 'state_date': today_str()}
                except:
                    continue
    except Exception as e:
        print(f"[加载当天状态异常] {e}")
    return state

def handle_checkin(user: str):
    say(f"{user}签到成功")

def handle_signout(user: str):
    say(f"{user}签退成功")

def decide_action(user):
    info = attendance_state.get(user)
    if info and info.get('state_date') != today_str():
        attendance_state[user] = {'last_action': '签退', 'state_date': today_str()}
        info = attendance_state[user]
    return '签到' if (not info or info.get('last_action') == '签退') else '签退'

def is_duplicate(fingerprint_id):
    current_time = datetime.now().timestamp()
    last_time = recent_recognitions.get(fingerprint_id, 0)
    if current_time - last_time < DUPLICATE_INTERVAL:
        # 静默过滤，减少日志输出
        return True
    recent_recognitions[fingerprint_id] = current_time
    return False

def update_test_log(device_id, new_entry):
    try:
        try:
            with open(LOG_FILE, 'r', encoding='utf-8-sig') as f:
                lines = list(csv.reader(f))
                header = lines[0] if lines else ['timestamp', 'device_id', 'finger_id', 'score']
                records = lines[1:]
        except FileNotFoundError:
            header = ['timestamp', 'device_id', 'finger_id', 'score']
            records = []
        
        cleaned_records = [row for row in records if not (
            len(row) > 2 and row[1] == device_id and row[2] in ['800', '900']
        )]
        
        with open(LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(cleaned_records)
            writer.writerow(new_entry)
    except Exception as e:
        print(f"[测试日志更新异常] {e}")

def setup_log_file():
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                csv.writer(f).writerow(['timestamp', 'device_id', 'finger_id', 'score'])
            print(f"[日志文件] 已创建：{LOG_FILE}")
    except Exception as e:
        print(f"[日志文件初始化异常] {e}")

def auto_signout_for_today(cutoff_dt=None):
    with STATE_LOCK:
        if cutoff_dt is None:
            cutoff_dt = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
        timestamp_str = cutoff_dt.strftime('%Y/%m/%d %H:%M:%S')
        
        for user, info in list(attendance_state.items()):
            if not info or info.get('state_date') != today_str() or info.get('last_action') != '签到':
                continue
            try:
                attendance_state[user] = {'last_action': '签退', 'state_date': today_str()}
                append_attendance_row(timestamp_str, AUTO_SIGNOUT_DEVICE_ID, '', '', user, '签退')
                print(f"[自动签退] {user}")
                say(f"{user}签退成功")
            except Exception as e:
                print(f"[自动签退异常] {e}")

def start_midnight_auto_signout():
    def worker():
        while True:
            try:
                now = datetime.now()
                target = now.replace(hour=23, minute=59, second=59, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                wait_seconds = max(1, int((target - now).total_seconds()))
                time.sleep(wait_seconds)
                auto_signout_for_today(cutoff_dt=target)
            except Exception as e:
                print(f"[自动签退线程异常] {e}")
                time.sleep(60)
    
    auto_signout_thread = threading.Thread(target=worker, daemon=True, name="AutoSignoutThread")
    auto_signout_thread.start()
    print(f"[自动签退] 线程已启动")

# ========== 4. MQTT 消息处理 ==========
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        current_timestamp = get_current_timestamp()
        device_id = data.get('device', 'unknown_device')
        raw_id = data.get('id', -1)
        score = data.get('score', 0)

        is_test = str(raw_id) in ['800', '900']
        display_id = str(raw_id) if is_test else "unknown"
        
        try:
            from fingerprint_config import FINGERPRINT_NAMES
            display_id = str(raw_id) if is_test else FINGERPRINT_NAMES.get(raw_id, "unknown")
        except ImportError:
            pass
        
        log_entry = [current_timestamp, device_id, display_id, score]

        if is_test:
            update_test_log(device_id, log_entry)
        else:
            try:
                with open(LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                    csv.writer(f).writerow(log_entry)
            except Exception as log_error:
                print(f"[指纹日志写入异常] {log_error}")

            if display_id != "unknown" and not is_duplicate(raw_id):
                # 首次遇到该用户时，后台生成签到/签退两条缓存
                TTS_ENGINE.ensure_user_cache_async(display_id)

                speak_text = None
                with STATE_LOCK:
                    action = decide_action(display_id)
                    if action == '签到':
                        append_attendance_row(current_timestamp, device_id, str(raw_id), str(score), display_id, '签到')
                        attendance_state[display_id] = {'last_action': '签到', 'state_date': today_str()}
                        speak_text = f"{display_id}签到成功"
                    else:
                        append_attendance_row(current_timestamp, device_id, str(raw_id), str(score), display_id, '签退')
                        attendance_state[display_id] = {'last_action': '签退', 'state_date': today_str()}
                        speak_text = f"{display_id}签退成功"
                if speak_text:
                    say(speak_text)
            else:
                if not is_duplicate(raw_id):
                    try:
                        say("您的指纹还没有录入我们实验室的指纹库哦，N6-506智能物联网实验室欢迎您的加入")
                    except Exception as speak_error:
                        print(f"[未知用户播报异常] {speak_error}")
                        
    except json.JSONDecodeError as e:
        print(f"[MQTT 消息解析异常] {e}")
    except Exception as e:
        print(f"[MQTT 消息处理异常] {e}")

# ========== 主函数 ==========
if __name__ == "__main__":
    # 独立播报模式 (CLI)
    if len(sys.argv) >= 2 and sys.argv[1] == "--speak":
        msg = " ".join(sys.argv[2:])
        print(f"[CLI 模式] 正在播报：{msg}")
        if not TTS_ENGINE.check_model():
            sys.exit(1)
        TTS_ENGINE.start()
        say(msg)
        time.sleep(2)
        TTS_ENGINE.stop()
        sys.exit(0)
    
    print("=" * 60)
    print("智能物联网实验室指纹签到系统启动中... (缓存优先 TTS 版)")
    print("=" * 60)
    
    try:
        # 检查 TTS 模型
        print("[初始化] 正在检查语音模型...")
        if not TTS_ENGINE.check_model():
            print("[警告] TTS 模型检查失败，播报功能可能不可用")
        
        # 启动 TTS 播放线程
        TTS_ENGINE.start()
        
        print("[初始化] 正在初始化系统文件...")
        setup_log_file()
        init_attendance_file()
        
        print("[初始化] 正在加载当天考勤状态...")
        attendance_state.update(load_today_status())
        
        print("[初始化] 正在启动自动签退服务...")
        start_midnight_auto_signout()

        print("[初始化] 正在连接 MQTT 服务...")
        client = mqtt.Client()
        client.on_connect = lambda c, u, f, rc: (
            print(f"[MQTT] 连接成功，代码：{rc}") if rc == 0 else print(f"[MQTT] 连接失败，代码：{rc}"), 
            c.subscribe(MQTT_TOPIC) if rc == 0 else None
        )
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT)
        
        print("=" * 60)
        print("系统启动完成！")
        print(f"MQTT 监听已启动，主题：{MQTT_TOPIC}")
        print(f"TTS 模型：{TTS_MODEL_PATH}")
        print(f"TTS 缓存目录：{TTS_CACHE_DIR}")
        print("说明：首次遇到某姓名会生成缓存，后续可在 1 秒内播报")
        print("=" * 60)
        
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n[系统] 收到中断信号，正在关闭...")
        TTS_ENGINE.stop()
        sys.exit(0)
    except Exception as e:
        print(f"[系统启动异常] {e}")
        TTS_ENGINE.stop()
        sys.exit(1)
