from machine import UART, Pin
import machine
import time
import struct
import network
import ubinascii
import ujson
from umqtt.simple import MQTTClient

# ===== 配置常量 =====
class Config:
    # WiFi 配置
    WIFI_SSID = "N6-506-ARM"
    WIFI_PASS = "armn6-506qwg"
    WIFI_TIMEOUT = 30  # 秒

    # MQTT 配置
    MQTT_BROKERS = [
        {"host": "192.168.3.88", "port": 1883},
        {"host": "10.18.33.167", "port": 1883}
    ]
    MQTT_TOPIC = b"fingerprint/data"
    MQTT_KEEPALIVE = 60

    # 指纹模块配置
    UART_BAUDRATE = 57600
    UART_TX = 17
    UART_RX = 16

    # 指纹串口命令接收超时
    UART_CMD_TIMEOUT_MS = 800

    # 心跳配置
    HEARTBEAT_INTERVAL = 1000  # 循环次数

    # LED 配置
    LED_PIN = 2

    # 蜂鸣器配置
    BUZZER_PIN = 23


# ===== 全局变量 =====
led = Pin(Config.LED_PIN, Pin.OUT)
buzzer = Pin(Config.BUZZER_PIN, Pin.OUT)
uart = UART(1, baudrate=Config.UART_BAUDRATE, tx=Config.UART_TX, rx=Config.UART_RX)
wlan = network.WLAN(network.STA_IF)
mqtt_clients = []
device_id = ""


# ===== LED 控制 =====
class LEDController:
    @staticmethod
    def blink(interval=0.2, times=1, on_duration=None):
        on_time = on_duration if on_duration is not None else interval
        for i in range(times):
            led.on()
            time.sleep(on_time)
            led.off()
            if i < times - 1:
                time.sleep(interval)

    @staticmethod
    def solid(duration=1):
        led.on()
        time.sleep(duration)
        led.off()


# ===== 蜂鸣器控制 =====
class BuzzerController:
    @staticmethod
    def beep(duration_ms=100):
        buzzer.on()
        time.sleep_ms(duration_ms)
        buzzer.off()

    @staticmethod
    def long_beep(duration_ms=1000):
        buzzer.on()
        time.sleep_ms(duration_ms)
        buzzer.off()

    @staticmethod
    def beep_pattern(duration_ms=100, count=1, interval_ms=100):
        for i in range(count):
            buzzer.on()
            time.sleep_ms(duration_ms)
            buzzer.off()
            if i < count - 1:
                time.sleep_ms(interval_ms)

    @staticmethod
    def success_beep():
        BuzzerController.beep(100)
        time.sleep_ms(50)
        BuzzerController.long_beep(1000)

    @staticmethod
    def error_beep():
        BuzzerController.beep_pattern(100, 3, 50)


# ===== 网络连接 =====
class NetworkManager:
    @staticmethod
    def get_device_id():
        try:
            mac = network.WLAN().config('mac')
            mac_suffix = ubinascii.hexlify(mac[-2:]).decode().upper()
            ip = wlan.ifconfig()[0] if wlan.isconnected() else "0.0.0.0"
            return "{}@{}".format(mac_suffix, ip)
        except Exception as e:
            print("❌ 获取设备ID失败: {}".format(e))
            return "unknown_device"

    @staticmethod
    def connect_wifi():
        if wlan.isconnected():
            print('✅ WiFi已连接, IP:', wlan.ifconfig()[0])
            return True

        print('🔌 正在连接WiFi...')
        wlan.active(True)
        wlan.connect(Config.WIFI_SSID, Config.WIFI_PASS)

        start_time = time.time()
        while not wlan.isconnected():
            if time.time() - start_time > Config.WIFI_TIMEOUT:
                print('❌ WiFi连接超时')
                BuzzerController.error_beep()
                return False

            LEDController.blink(0.3, 1)
            time.sleep(0.7)

        print('✅ WiFi连接成功, IP:', wlan.ifconfig()[0])
        LEDController.solid(2)
        BuzzerController.beep(200)
        return True


# ===== MQTT 客户端管理 =====
class MQTTManager:
    @staticmethod
    def connect_all_brokers():
        global mqtt_clients, device_id
        mqtt_clients = []

        for i, broker in enumerate(Config.MQTT_BROKERS):
            try:
                client_id = "{}_broker{}".format(device_id, i + 1)
                client = MQTTClient(
                    client_id,
                    broker["host"],
                    port=broker["port"],
                    keepalive=Config.MQTT_KEEPALIVE
                )
                client.connect()
                mqtt_clients.append(client)
                print("✅ MQTT连接到 {}:{}".format(broker["host"], broker["port"]))

                LEDController.blink(0.2, i + 1)
                BuzzerController.beep(100)
                time.sleep(0.3)
            except Exception as e:
                print("❌ MQTT连接失败 {}:{} {}".format(broker["host"], broker["port"], e))

        if mqtt_clients:
            LEDController.solid(1)
            BuzzerController.beep_pattern(100, 2, 50)
            return True

        print("❌ 没有可用的MQTT连接")
        BuzzerController.error_beep()
        return False

    @staticmethod
    def send_to_all(finger_id, score):
        if not mqtt_clients:
            print("❌ 没有可用的MQTT客户端")
            LEDController.blink(0.3, 2)
            BuzzerController.error_beep()
            return 0

        payload = {
            "device": device_id,
            "id": finger_id,
            "score": score,
            "timestamp": time.time()
        }

        try:
            json_payload = ujson.dumps(payload)
        except Exception as e:
            print("❌ JSON序列化失败: {}".format(e))
            return 0

        success_count = 0
        for i, client in enumerate(mqtt_clients):
            try:
                client.publish(Config.MQTT_TOPIC, json_payload)
                print("📤 发送到MQTT broker {}: ID={}, Score={}".format(i + 1, finger_id, score))
                success_count += 1
            except Exception as e:
                print("❌ 发送到broker {}失败: {}".format(i + 1, e))
                if MQTTManager._reconnect_client(i):
                    try:
                        mqtt_clients[i].publish(Config.MQTT_TOPIC, json_payload)
                        success_count += 1
                        print("✅ 重连后发送成功到broker {}".format(i + 1))
                    except:
                        print("❌ 重连后发送仍然失败")

        if success_count > 0:
            LEDController.blink(0.1, min(success_count, 5))
            if finger_id not in [800, 900]:
                print("🎉 指纹签到并上传成功！")
                BuzzerController.success_beep()
        else:
            LEDController.blink(0.3, 2)
            BuzzerController.error_beep()

        return success_count

    @staticmethod
    def _reconnect_client(index):
        if index < len(Config.MQTT_BROKERS):
            try:
                broker = Config.MQTT_BROKERS[index]
                client_id = "{}_broker{}".format(device_id, index + 1)
                new_client = MQTTClient(client_id, broker["host"], port=broker["port"])
                new_client.connect()
                mqtt_clients[index] = new_client
                print("✅ 重新连接到broker {}".format(index + 1))
                return True
            except Exception as e:
                print("❌ 重新连接broker {}失败: {}".format(index + 1, e))
        return False


# ===== 指纹模块操作 =====
class FingerprintSensor:
    def __init__(self):
        self.uart = uart

    def send_command(self, cmd, description="命令", timeout_ms=None, min_len=0):
        if timeout_ms is None:
            timeout_ms = Config.UART_CMD_TIMEOUT_MS

        try:
            # 清空接收缓冲区
            while self.uart.any():
                _ = self.uart.read()

            self.uart.write(cmd)

            deadline = time.ticks_add(time.ticks_ms(), timeout_ms)
            resp = b""

            while time.ticks_ms() < deadline:
                if self.uart.any():
                    chunk = self.uart.read()
                    if chunk:
                        resp += chunk
                        if min_len and len(resp) >= min_len:
                            break
                time.sleep_ms(5)

            if not resp:
                return None

            return resp

        except Exception as e:
            # 仅在调试时开启，避免干扰主逻辑判断
            # print("❌ {}发送失败: {}".format(description, e))
            return None

    def get_image(self):
        cmd = b'\xef\x01\xff\xff\xff\xff\x01\x00\x03\x01\x00\x05'
        resp = self.send_command(cmd, "获取图像", min_len=12)

        success = resp and len(resp) > 9 and resp[9] == 0x00
        if success:
            LEDController.blink(0.1, 1)
            BuzzerController.beep(50)
        return success

    def generate_characteristics(self):
        cmd = b'\xef\x01\xff\xff\xff\xff\x01\x00\x04\x02\x01\x00\x08'
        resp = self.send_command(cmd, "生成特征", min_len=12)

        success = resp and len(resp) > 9 and resp[9] == 0x00
        if success:
            LEDController.blink(0.1, 1)
        return success

    def search_fingerprint(self):
        cmd = b'\xef\x01\xff\xff\xff\xff\x01\x00\x08\x04\x01\x00\x00\x00\xA3\x00\xB1'
        resp = self.send_command(cmd, "搜索指纹", min_len=14)

        if resp and len(resp) >= 14 and resp[9] == 0x00:
            page_id = struct.unpack('>H', resp[10:12])[0]
            score = struct.unpack('>H', resp[12:14])[0]
            print("✅ 指纹匹配成功! ID: {}, Score: {}".format(page_id, score))

            success_count = MQTTManager.send_to_all(page_id, score)

            if success_count == 0:
                print("⚠️  指纹识别成功但上传失败，仍然给出成功提示")
                BuzzerController.success_beep()

            return True
        else:
            # 匹配失败（可能是手指不对，或者特征不匹配）
            # 这里不打印错误，因为主循环会处理“流程结束”的逻辑
            return False


# ===== 主程序 =====
def main():
    global device_id

    print("🚀 指纹识别系统启动中...")
    buzzer.off()

    if not NetworkManager.connect_wifi():
        print("❌ 网络连接失败，系统退出")
        return

    device_id = NetworkManager.get_device_id()
    print("📟 设备ID: {}".format(device_id))

    if not MQTTManager.connect_all_brokers():
        print("⚠️  MQTT连接失败，继续运行但无法发送数据")

    MQTTManager.send_to_all(900, 900)

    fingerprint_sensor = FingerprintSensor()

    loop_count = 0
    
    # 【关键修改】状态标志：是否准备好接收新指纹
    ready_for_next = True

    while True:
        try:
            # 1. 如果准备好，打印提示，并标记为“忙碌”
            if ready_for_next:
                print("👆 请按压指纹...")
                ready_for_next = False

            # 2. 执行指纹识别流程
            process_finished = False
            
            if fingerprint_sensor.get_image():
                if fingerprint_sensor.generate_characteristics():
                    result = fingerprint_sensor.search_fingerprint()
                    # 无论搜索成功 (True) 还是失败 (False)，流程都结束了
                    process_finished = True
                else:
                    # 生成特征失败，流程结束
                    process_finished = True
            # 如果 get_image 失败（通常是因为没检测到手指），process_finished 保持 False
            # 循环会继续，但不会重复打印提示，直到检测到手指并完成流程

            # 3. 如果流程结束（成功或失败），重置状态，下次循环会再次打印提示
            if process_finished:
                ready_for_next = True
                # 可选：加一个小延时，让用户有时间移开手指，避免连续误触
                # time.sleep_ms(500) 

            # 4. 心跳处理
            loop_count += 1
            if loop_count >= Config.HEARTBEAT_INTERVAL:
                print("💓 发送心跳信号")
                MQTTManager.send_to_all(800, 800)
                loop_count = 0
                LEDController.blink(0.1, 1)

            # 短延时，让出 CPU
            time.sleep_ms(200)

        except KeyboardInterrupt:
            print("🛑 程序被用户中断")
            break
        except Exception as e:
            print("❌ 主循环错误: {}".format(e))
            # 发生严重错误时，也重置状态，防止死锁
            ready_for_next = True
            BuzzerController.error_beep()
            time.sleep(1)


if __name__ == "__main__":
    main()
