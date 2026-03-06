import asyncio
import json
import sys
import os
import re
import random
import threading
import logging
import tempfile
import traceback

from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

# === KONFIGURASI TELEGRAM ===
api_id = int(os.getenv('TG_API_ID', '27165484'))
api_hash = os.getenv('TG_API_HASH', 'b5f28de58166f16d6fedc4e0fd29a859')
client = TelegramClient('user_session', api_id, api_hash)

# === SETUP LOGGER ===
logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
    )

# === SCHEDULER ===
scheduler = AsyncIOScheduler()

# === DATA GLOBAL ===
blacklisted_groups = set()
whitelisted_groups = set()  # Whitelist mode groups
whitelist_mode_enabled = {}  # key: user_id, value: bool
job_data = {}
delay_setting = {}
delay_per_group_setting = {}  # Menyimpan delay per user (detik)
pesan_simpan = {}  # key: user_id, value: pesan terbaru
preset_pesan = {}  # key: user_id, value: {nama_preset: isi_pesan}
usage_stats = {}  # key: user_id, value: jumlah pesan yang berhasil dikirim
random_delay_batch_setting = {}  # key: user_id, value: {'min': float, 'max': float}
random_delay_group_setting = {}  # key: user_id, value: {'min': float, 'max': float}
member_filter_setting = {}  # key: user_id, value: {'min': int|None, 'max': int|None}
member_count_cache = {}  # key: dialog_id, value: {'count': int|None, 'ts': datetime}

start_time = datetime.now()
TOTAL_SENT_MESSAGES = 0
JOBS = {}
active_forward_tasks = {}  # key: user_id, value: {'stop_flag': bool, 'paused': bool, 'task': asyncio.Task}
forward_sessions = {}  # key: user_id, value: session stats dict

DEFAULT_ALLOWED_USERS = {7605681637, 1538087933}  # Ganti dengan ID admin awal
ALLOWED_USERS_FILE = 'allowed_users.txt'

# === FILE PERSISTENCE ===
DATA_DIR = 'data'
MEDIA_DIR = os.path.join(DATA_DIR, 'media')
BLACKLIST_FILE = os.path.join(DATA_DIR, 'blacklist.json')
PRESET_FILE = os.path.join(DATA_DIR, 'preset_pesan.json')
DELAY_FILE = os.path.join(DATA_DIR, 'delay_settings.json')
PESAN_SIMPAN_FILE = os.path.join(DATA_DIR, 'pesan_simpan.json')
WHITELIST_FILE = os.path.join(DATA_DIR, 'whitelist.json')
WHITELIST_MODE_FILE = os.path.join(DATA_DIR, 'whitelist_mode.json')

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def ensure_media_dir():
    ensure_data_dir()
    os.makedirs(MEDIA_DIR, exist_ok=True)

async def get_member_count_cached(entity, dialog_id):
    now = datetime.now()
    cached = member_count_cache.get(dialog_id)
    if cached and isinstance(cached, dict):
        ts = cached.get('ts')
        if isinstance(ts, datetime) and (now - ts) < timedelta(hours=6):
            return cached.get('count')

    count = None
    try:
        from telethon.tl.types import Channel, Chat
        from telethon.tl.functions.channels import GetFullChannelRequest
        from telethon.tl.functions.messages import GetFullChatRequest

        if isinstance(entity, Channel):
            full = await client(GetFullChannelRequest(entity))
            count = getattr(getattr(full, 'full_chat', None), 'participants_count', None)
        elif isinstance(entity, Chat):
            full = await client(GetFullChatRequest(entity.id))
            count = getattr(getattr(full, 'full_chat', None), 'participants_count', None)
    except Exception as e:
        logging.error(f"Error get_member_count_cached untuk {dialog_id}: {e}")

    member_count_cache[dialog_id] = {'count': count, 'ts': now}
    return count

def is_in_member_range(count, min_m=None, max_m=None):
    if count is None:
        return True
    if min_m is not None and count < min_m:
        return False
    if max_m is not None and count > max_m:
        return False
    return True

def save_blacklist():
    ensure_data_dir()
    try:
        with open(BLACKLIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(blacklisted_groups), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error save blacklist: {e}")

def load_blacklist():
    global blacklisted_groups
    try:
        with open(BLACKLIST_FILE, 'r', encoding='utf-8') as f:
            blacklisted_groups = set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        blacklisted_groups = set()

def save_preset():
    ensure_data_dir()
    try:
        data = {str(k): v for k, v in preset_pesan.items()}
        with open(PRESET_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error save preset: {e}")

def load_preset():
    global preset_pesan
    try:
        with open(PRESET_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            preset_pesan = {int(k): v for k, v in data.items()}
    except (FileNotFoundError, json.JSONDecodeError):
        preset_pesan = {}

def save_delay_settings():
    ensure_data_dir()
    try:
        data = {
            'delay_setting': {str(k): v for k, v in delay_setting.items()},
            'delay_per_group': {str(k): v for k, v in delay_per_group_setting.items()},
            'random_delay_batch': {str(k): v for k, v in random_delay_batch_setting.items()},
            'random_delay_group': {str(k): v for k, v in random_delay_group_setting.items()},
            'member_filter': {str(k): v for k, v in member_filter_setting.items()}
        }
        with open(DELAY_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error save delay settings: {e}")

def load_delay_settings():
    global delay_setting, delay_per_group_setting, random_delay_batch_setting, random_delay_group_setting, member_filter_setting
    try:
        with open(DELAY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            delay_setting = {int(k): v for k, v in data.get('delay_setting', {}).items()}
            delay_per_group_setting = {int(k): v for k, v in data.get('delay_per_group', {}).items()}
            random_delay_batch_setting = {int(k): v for k, v in data.get('random_delay_batch', {}).items()}
            random_delay_group_setting = {int(k): v for k, v in data.get('random_delay_group', {}).items()}
            member_filter_setting = {int(k): v for k, v in data.get('member_filter', {}).items()}
    except (FileNotFoundError, json.JSONDecodeError):
        delay_setting = {}
        delay_per_group_setting = {}
        random_delay_batch_setting = {}
        random_delay_group_setting = {}
        member_filter_setting = {}

def save_pesan_simpan():
    ensure_data_dir()
    try:
        data = {str(k): v for k, v in pesan_simpan.items()}
        with open(PESAN_SIMPAN_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error save pesan simpan: {e}")

def load_pesan_simpan():
    global pesan_simpan
    try:
        with open(PESAN_SIMPAN_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            pesan_simpan = {int(k): v for k, v in data.items()}
    except (FileNotFoundError, json.JSONDecodeError):
        pesan_simpan = {}

def save_whitelist():
    ensure_data_dir()
    try:
        data = {
            'groups': list(whitelisted_groups),
            'mode': {str(k): v for k, v in whitelist_mode_enabled.items()}
        }
        with open(WHITELIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Error save whitelist: {e}")

def load_whitelist():
    global whitelisted_groups, whitelist_mode_enabled
    try:
        with open(WHITELIST_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            whitelisted_groups = set(data.get('groups', []))
            whitelist_mode_enabled = {int(k): v for k, v in data.get('mode', {}).items()}
    except (FileNotFoundError, json.JSONDecodeError):
        whitelisted_groups = set()
        whitelist_mode_enabled = {}

# Load semua data persisten saat startup
load_blacklist()
load_preset()
load_delay_settings()
load_pesan_simpan()
load_whitelist()

# === FUNGSI LOAD & SAVE ALLOWED USERS ===
def load_allowed_users():
    users = set()
    try:
        with open(ALLOWED_USERS_FILE, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    users.add(int(line))
                except ValueError:
                    continue
    except FileNotFoundError:
        users = set(DEFAULT_ALLOWED_USERS)
        try:
            with open(ALLOWED_USERS_FILE, 'w', encoding='utf-8') as f:
                f.write("\n".join(map(str, sorted(users))))
        except Exception:
            pass
    if not users:
        users = set(DEFAULT_ALLOWED_USERS)
    return users

def save_allowed_users(users=None):
    if users is None:
        users = ALLOWED_USERS
    with open(ALLOWED_USERS_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(map(str, sorted(set(users)))))

ALLOWED_USERS = load_allowed_users()

async def is_allowed(event):
    try:
        sender = await event.get_sender()
        return bool(sender) and sender.id in ALLOWED_USERS
    except Exception as e:
        logging.error(f"[ERROR] Saat memeriksa is_allowed: {e}")
        return False

async def require_allowed(event):
    if await is_allowed(event):
        return True
    try:
        await event.respond("❌ Kamu tidak punya izin untuk menggunakan perintah ini.")
    except Exception:
        pass
    return False

HARI_MAPPING = {
    'senin': 'monday', 'selasa': 'tuesday', 'rabu': 'wednesday',
    'kamis': 'thursday', 'jumat': 'friday', 'sabtu': 'saturday',
    'minggu': 'sunday'
}

def update_usage(user_id, count):
    global TOTAL_SENT_MESSAGES
    usage_stats[user_id] = usage_stats.get(user_id, 0) + count
    TOTAL_SENT_MESSAGES += count

# === FUNGSI UNTUK MELAKUKAN FORWARDING PESAN ===
async def forward_job(
    user_id, mode, source, message_id_or_text,
    jumlah_grup, durasi_menit: float, jumlah_pesan,
    delay_per_group: int = 0
):
    fwd_start = datetime.now()
    end = fwd_start + timedelta(minutes=durasi_menit)
    jeda_batch = delay_setting.get(user_id, 5)
    if delay_per_group <= 0:
        delay_per_group = delay_per_group_setting.get(user_id, 0)

    rand_batch = random_delay_batch_setting.get(user_id)
    rand_group = random_delay_group_setting.get(user_id)

    member_filter = member_filter_setting.get(user_id, {})
    min_member = member_filter.get('min')
    max_member = member_filter.get('max')
    now = datetime.now()
    next_reset = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    harian_counter = 0
    total_counter = 0
    fail_counter = 0
    flood_wait_counter = 0
    use_whitelist = whitelist_mode_enabled.get(user_id, False)
    
    # Inisialisasi stop flag dan pause flag untuk user ini
    if user_id not in active_forward_tasks:
        active_forward_tasks[user_id] = {'stop_flag': False, 'paused': False}
    active_forward_tasks[user_id]['stop_flag'] = False
    active_forward_tasks[user_id]['paused'] = False
    
    # Inisialisasi session stats
    forward_sessions[user_id] = {
        'start_time': fwd_start,
        'end_time': end,
        'mode': mode,
        'target_grup': jumlah_grup,
        'target_pesan': jumlah_pesan,
        'durasi_menit': durasi_menit,
        'sent': 0,
        'failed': 0,
        'flood_waits': 0,
        'status': 'running',
        'last_group': '',
        'failed_groups': []
    }
    
    media_path = None
    text_payload = message_id_or_text
    if isinstance(message_id_or_text, dict):
        media_path = message_id_or_text.get('media_path')
        text_payload = message_id_or_text.get('text', '')

    # Parse multiple messages jika ada (dipisahkan oleh ||)
    if isinstance(text_payload, str) and '||' in text_payload:
        messages_list = [msg.strip() for msg in text_payload.split('||') if msg.strip()]
    else:
        messages_list = [text_payload]

    durasi_jam = durasi_menit / 60.0
    info_msg = f"[{now:%H:%M:%S}] Mulai meneruskan pesan selama {durasi_menit:.2f} menit ({durasi_jam:.2f} jam)~"
    print(info_msg)
    logging.info(info_msg)

    try:
        pesan_info = f"Sedang meneruskan pesan...\nDurasi: {durasi_menit:.2f} menit\nTarget harian: {jumlah_pesan} pesan."
        if len(messages_list) > 1:
            pesan_info += f"\nTotal pesan berbeda: {len(messages_list)}"
        if use_whitelist:
            pesan_info += f"\nMode: WHITELIST ({len(whitelisted_groups)} grup)"
        await client.send_message(user_id, pesan_info)
    except Exception as e:
        logging.error(f"Error kirim notifikasi ke {user_id}: {e}")

    while datetime.now() < end:
        # Cek stop flag
        if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
            stop_msg = f"[{datetime.now():%H:%M:%S}] Pengiriman dihentikan oleh user."
            print(stop_msg)
            logging.info(stop_msg)
            forward_sessions[user_id]['status'] = 'stopped'
            try:
                session = forward_sessions.get(user_id, {})
                await client.send_message(
                    user_id,
                    f"⏹️ Pengiriman pesan dihentikan.\n"
                    f"Terkirim: {total_counter} | Gagal: {fail_counter} | Flood wait: {flood_wait_counter}"
                )
            except Exception as e:
                logging.error(f"Error kirim notifikasi stop ke {user_id}: {e}")
            break
        
        # Cek pause flag
        while active_forward_tasks.get(user_id, {}).get('paused', False):
            forward_sessions[user_id]['status'] = 'paused'
            await asyncio.sleep(2)
            # Cek stop flag juga saat paused
            if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
                break
            # Cek waktu habis saat paused
            if datetime.now() >= end:
                break
        
        if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
            break
        
        forward_sessions[user_id]['status'] = 'running'
            
        if datetime.now() >= next_reset:
            harian_counter = 0
            next_reset += timedelta(days=1)
            reset_msg = (
                f"[{datetime.now():%H:%M:%S}] Reset harian: lanjut besok yaa, sayang!"
            )
            print(reset_msg)
            logging.info(reset_msg)

        counter = 0
        total_dialogs_checked = 0
        groups_found = 0
        groups_skipped = 0
        
        logging.info(f"Mulai iterasi dialog untuk {jumlah_grup} grup, durasi {durasi_menit} menit")
        
        async for dialog in client.iter_dialogs():
            total_dialogs_checked += 1
            # Cek stop flag lagi di dalam loop
            if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
                logging.info(f"Stop flag aktif. Total dialog dicek: {total_dialogs_checked}, Grup ditemukan: {groups_found}, Grup di-skip: {groups_skipped}")
                break
            
            # Cek pause di dalam loop juga
            while active_forward_tasks.get(user_id, {}).get('paused', False):
                forward_sessions[user_id]['status'] = 'paused'
                await asyncio.sleep(2)
                if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
                    break
                if datetime.now() >= end:
                    break
            forward_sessions[user_id]['status'] = 'running'
                
            if datetime.now() >= end:
                logging.info(f"Waktu habis. Total dialog dicek: {total_dialogs_checked}, Grup ditemukan: {groups_found}, Grup di-skip: {groups_skipped}")
                break
            if harian_counter >= jumlah_pesan:
                logging.info(f"Target harian tercapai. Total dialog dicek: {total_dialogs_checked}, Grup ditemukan: {groups_found}, Grup di-skip: {groups_skipped}")
                break
            
            # Cek apakah ini grup atau supergroup (bukan channel atau private chat)
            is_group_type = False
            dialog_name = dialog.name or "Tanpa Nama"
            try:
                from telethon.tl.types import Channel, Chat
                
                if dialog.is_group:
                    is_group_type = True
                elif isinstance(dialog.entity, Channel):
                    if hasattr(dialog.entity, 'megagroup') and dialog.entity.megagroup:
                        is_group_type = True
                    elif hasattr(dialog.entity, 'broadcast'):
                        if not dialog.entity.broadcast:
                            is_group_type = True
                    else:
                        is_group_type = True
                elif isinstance(dialog.entity, Chat):
                    is_group_type = True
                else:
                    if hasattr(dialog.entity, 'megagroup') and dialog.entity.megagroup:
                        is_group_type = True
            except Exception as e:
                logging.error(f"Error cek tipe dialog untuk {dialog_name}: {e}")
                try:
                    if dialog.is_group:
                        is_group_type = True
                except:
                    pass
                continue
            
            if not is_group_type:
                continue
            
            # Cek whitelist mode
            if use_whitelist:
                if dialog_name not in whitelisted_groups:
                    continue
            
            # Cek blacklist
            if dialog_name in blacklisted_groups:
                groups_skipped += 1
                continue
            
            # Cek filter member
            if min_member is not None or max_member is not None:
                try:
                    count = await get_member_count_cached(dialog.entity, dialog.id)
                    if not is_in_member_range(count, min_member, max_member):
                        continue
                except Exception as e:
                    logging.error(f"Error filter member untuk {dialog_name}: {e}")
                    continue
            
            groups_found += 1
            forward_sessions[user_id]['last_group'] = dialog_name
            try:
                pesan_terkirim_grup = 0
                for idx, current_message in enumerate(messages_list):
                    # Cek stop flag sebelum setiap pesan
                    if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
                        break
                    if datetime.now() >= end or harian_counter >= jumlah_pesan:
                        break
                    
                    sent_ok = False
                    max_retries = 3
                    for retry in range(max_retries):
                        try:
                            if mode == 'forward':
                                msg_id = None
                                src = source
                                
                                if isinstance(current_message, int):
                                    msg_id = current_message
                                elif isinstance(current_message, str):
                                    if current_message.strip().isdigit():
                                        msg_id = int(current_message.strip())
                                    else:
                                        parts = current_message.strip().split(maxsplit=1)
                                        if len(parts) == 2:
                                            src = parts[0]
                                            try:
                                                msg_id = int(parts[1])
                                            except ValueError:
                                                msg_id = int(current_message.strip())
                                        else:
                                            try:
                                                msg_id = int(current_message.strip())
                                            except ValueError:
                                                if isinstance(message_id_or_text, int):
                                                    msg_id = message_id_or_text
                                                elif isinstance(message_id_or_text, str) and message_id_or_text.isdigit():
                                                    msg_id = int(message_id_or_text)
                                                else:
                                                    break
                                
                                if msg_id is not None:
                                    msg = await client.get_messages(src, ids=msg_id)
                                    if msg:
                                        await client.forward_messages(
                                            dialog.id, msg.id, from_peer=src
                                        )
                                        sent_ok = True
                            else:
                                if media_path:
                                    await client.send_file(
                                        dialog.id,
                                        media_path,
                                        caption=str(current_message) if current_message is not None else "",
                                        force_document=False
                                    )
                                else:
                                    await client.send_message(
                                        dialog.id, current_message, link_preview=True
                                    )
                                sent_ok = True
                            
                            break  # Berhasil, keluar dari retry loop
                            
                        except FloodWaitError as e:
                            flood_wait_counter += 1
                            forward_sessions[user_id]['flood_waits'] = flood_wait_counter
                            wait_time = e.seconds
                            flood_msg = f"[{datetime.now():%H:%M:%S}] ⚠️ FloodWait: menunggu {wait_time} detik..."
                            print(flood_msg)
                            logging.warning(flood_msg)
                            try:
                                if retry == 0:  # Notif hanya sekali
                                    await client.send_message(user_id, f"⚠️ Rate limit Telegram! Menunggu {wait_time} detik lalu lanjut otomatis...")
                            except:
                                pass
                            await asyncio.sleep(wait_time + 1)
                            # Cek stop flag setelah menunggu
                            if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
                                break
                            continue  # Retry
                            
                        except Exception as e:
                            error_msg = f"[{datetime.now():%H:%M:%S}] Gagal kirim ke {dialog_name}: {e}"
                            logging.error(error_msg)
                            fail_counter += 1
                            forward_sessions[user_id]['failed'] = fail_counter
                            forward_sessions[user_id]['failed_groups'].append(dialog_name)
                            break  # Jangan retry untuk error lain
                    
                    if sent_ok:
                        pesan_terkirim_grup += 1
                        harian_counter += 1
                        total_counter += 1
                        forward_sessions[user_id]['sent'] = total_counter
                        update_usage(user_id, 1)
                        
                        if idx < len(messages_list) - 1 and delay_per_group > 0:
                            await asyncio.sleep(min(delay_per_group, 2))
                
                if pesan_terkirim_grup > 0:
                    counter += 1
                    log_msg = f"[{datetime.now():%H:%M:%S}] Sukses kirim {pesan_terkirim_grup} pesan ke grup: {dialog.name}"
                    if len(messages_list) > 1:
                        log_msg += f" ({len(messages_list)} bubble chat berbeda)"
                    print(log_msg)
                    logging.info(log_msg)

                sleep_group = 0
                if rand_group and isinstance(rand_group, dict):
                    try:
                        sleep_group = random.uniform(float(rand_group.get('min', 0)), float(rand_group.get('max', 0)))
                    except Exception:
                        sleep_group = 0
                elif delay_per_group > 0:
                    sleep_group = delay_per_group

                if sleep_group and sleep_group > 0:
                    await asyncio.sleep(sleep_group)

                if counter >= jumlah_grup or harian_counter >= jumlah_pesan:
                    break

            except Exception as e:
                error_msg = f"[{datetime.now():%H:%M:%S}] Gagal dikirim ke grup {dialog.name}: {e}"
                print(error_msg)
                logging.error(error_msg)
                fail_counter += 1
                forward_sessions[user_id]['failed'] = fail_counter
                forward_sessions[user_id]['failed_groups'].append(dialog_name)
                continue

        # Cek stop flag setelah batch
        if active_forward_tasks.get(user_id, {}).get('stop_flag', False):
            break

        # Log statistik setelah batch
        logging.info(f"Batch selesai: {counter} grup, {total_counter} pesan terkirim. Total dialog dicek: {total_dialogs_checked}, Grup ditemukan: {groups_found}, Grup di-skip: {groups_skipped}")

        if harian_counter >= jumlah_pesan:
            notif = (
                f"Target harian {jumlah_pesan} pesan tercapai!\n"
                "Bot lanjut lagi besok bub, tetap semangat sebarnya ya!"
            )
            info_notif = f"[{datetime.now():%H:%M:%S}] {notif}"
            print(info_notif)
            logging.info(info_notif)
            try:
                await client.send_message(user_id, notif)
            except Exception as e:
                logging.error(f"Error kirim notifikasi ke {user_id}: {e}")

            sleep_seconds = (next_reset - datetime.now()).total_seconds()
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
        else:
            batch_msg = (
                f"[{datetime.now():%H:%M:%S}] Batch {counter} grup selesai. Istirahat {jeda_batch} detik dulu ya..."
            )
            print(batch_msg)
            logging.info(batch_msg)
            sleep_batch = jeda_batch
            if rand_batch and isinstance(rand_batch, dict):
                try:
                    sleep_batch = random.uniform(float(rand_batch.get('min', 0)), float(rand_batch.get('max', 0)))
                except Exception:
                    sleep_batch = jeda_batch
            if sleep_batch < 0:
                sleep_batch = 0
            await asyncio.sleep(sleep_batch)

    # Update session stats
    forward_sessions[user_id]['status'] = 'finished' if forward_sessions[user_id]['status'] == 'running' else forward_sessions[user_id]['status']
    forward_sessions[user_id]['sent'] = total_counter
    forward_sessions[user_id]['failed'] = fail_counter
    forward_sessions[user_id]['flood_waits'] = flood_wait_counter

    # Hapus stop/pause flag setelah selesai
    if user_id in active_forward_tasks:
        active_forward_tasks[user_id]['stop_flag'] = False
        active_forward_tasks[user_id]['paused'] = False

    durasi_jam = durasi_menit / 60.0
    selesai = (
        f"Forward selesai!\n"
        f"Terkirim: {total_counter} | Gagal: {fail_counter} | Flood wait: {flood_wait_counter}\n"
        f"Durasi: {durasi_menit:.2f} menit ({durasi_jam:.2f} jam)"
    )
    selesai_msg = f"[{datetime.now():%H:%M:%S}] {selesai}"
    print(selesai_msg)
    logging.info(selesai_msg)
    try:
        await client.send_message(user_id, selesai)
    except Exception as e:
        logging.error(f"Error kirim pesan selesai ke {user_id}: {e}")

# === PERINTAH BOT ===

@client.on(events.NewMessage(pattern=r'^/scheduleforward\b'))
async def schedule_cmd(event):
    if not await require_allowed(event):
        return

    raw = event.message.raw_text.strip()
    parts = raw.split(maxsplit=2)
    if len(parts) < 3:
        return await event.respond(
            "Format salah:\n"
            "/scheduleforward mode pesan/sumber jumlah_grup durasi_menit jeda jumlah_pesan hari1,hari2 jam:menit"
        )
    try:
        mode = parts[1].lower()
        rest = parts[2].strip()
        sisa = rest.rsplit(' ', 6)
        if len(sisa) != 7:
            return await event.respond("Format tidak sesuai. Pastikan argumen lengkap!")

        pesan_atau_sumber, jumlah, durasi, jeda, jumlah_pesan, hari_str, waktu = sisa
        jumlah = int(jumlah)
        durasi = float(durasi)  # Sekarang dalam menit
        jeda = int(jeda)
        jumlah_pesan = int(jumlah_pesan)
        jam, menit = map(int, waktu.split(':'))
        hari_tokens = [h.strip().lower() for h in re.split(r'[\s,]+', hari_str) if h.strip()]
        hari_list = [HARI_MAPPING.get(h) for h in hari_tokens]
        if None in hari_list:
            return await event.respond(
                "Terdapat nama hari yang tidak valid. Gunakan: senin,selasa,...,minggu."
            )

        source = ''
        message_id_or_text = pesan_atau_sumber
        media_obj = event.message.media
        if mode == 'forward':
            fp = pesan_atau_sumber.split(maxsplit=1)
            if len(fp) != 2:
                return await event.respond(
                    "Format forward schedule harus menyertakan sumber dan ID pesan.\n"
                    "Contoh: /scheduleforward forward @channel 123 20 120 5 300 senin,jumat 08:00"
                )
            source = fp[0]
            message_id_or_text = fp[1]  # Bisa jadi string jika multiple messages
        elif mode == 'text':
            if media_obj:
                ensure_media_dir()
                filename = os.path.join(MEDIA_DIR, f"sched_{event.sender_id}_{int(datetime.now().timestamp())}")
                try:
                    saved = await client.download_media(event.message, file=filename)
                    message_id_or_text = {'text': pesan_atau_sumber, 'media_path': saved}
                except Exception as e:
                    logging.error(f"Error download media schedule: {e}")
                    return await event.respond(f"Gagal memproses media: {e}")
            else:
                message_id_or_text = pesan_atau_sumber
        else:
            return await event.respond("Mode harus 'forward' atau 'text'")

        for hari_eng in hari_list:
            job_id = f"{event.sender_id}{hari_eng}{int(datetime.now().timestamp())}"
            job_data[job_id] = {
                'user': event.sender_id,
                'mode': mode,
                'source': source,
                'message': pesan_atau_sumber,
                'jumlah': jumlah,
                'durasi': durasi,
                'jeda': jeda,
                'jumlah_pesan': jumlah_pesan
            }
            delay_setting[event.sender_id] = jeda
            job = scheduler.add_job(
                forward_job,
                trigger=CronTrigger(day_of_week=hari_eng, hour=jam, minute=menit),
                args=[event.sender_id, mode, source, message_id_or_text, jumlah, durasi, jumlah_pesan],
                id=job_id
            )
            JOBS[job_id] = job

        daftar_hari = ', '.join([h.title() for h in hari_tokens])
        await event.respond(
            f"Jadwal forward berhasil ditambahkan untuk hari {daftar_hari} pukul {waktu}!"
        )
    except Exception as e:
        err_msg = f"Error: {e}"
        logging.error(err_msg)
        await event.respond(err_msg)

@client.on(events.NewMessage(pattern=r'^/forward\b'))
async def forward_sekarang(event):
    if not await require_allowed(event):
        return

    raw = event.message.raw_text.strip()
    parts = raw.split(maxsplit=2)
    if len(parts) < 3:
        return await event.respond(
            "Format salah:\n"
            "/forward mode sumber/id/isipesan jumlah_grup jeda durasi_menit jumlah_pesan\n\n"
            "Untuk multiple pesan, pisahkan dengan ||\n"
            "Contoh: /forward text \"Pesan 1\"||\"Pesan 2\"||\"Pesan 3\" 10 5 60 300"
        )
    try:
        mode = parts[1].lower()
        rest = parts[2].strip()
        
        if mode == 'forward':
            # Parse forward mode
            # Format: source id1||id2||id3 jumlah_grup jeda durasi_menit jumlah_pesan
            fparts = rest.split()
            if len(fparts) < 6:
                return await event.respond(
                    "Format salah:\n"
                    "/forward forward <sumber> <id_pesan> [||<id_pesan2>]... <jumlah_grup> <jeda> <durasi_menit> <jumlah_pesan>\n\n"
                    "Contoh: /forward forward @channel 123 10 5 60 300\n"
                    "Multiple pesan: /forward forward @channel 123||456||789 10 5 60 300"
                )
            
            # Parameter selalu di akhir: jumlah_grup jeda durasi_menit jumlah_pesan
            try:
                jumlah = int(fparts[-4])
                jeda_batch = int(fparts[-3])
                durasi = float(fparts[-2])  # Durasi dalam menit
                jumlah_pesan = int(fparts[-1])
                
                # Validasi parameter
                if jumlah <= 0 or jeda_batch < 0 or durasi <= 0 or jumlah_pesan <= 0:
                    return await event.respond(
                        "❌ Parameter tidak valid! Pastikan:\n"
                        "- jumlah_grup > 0\n"
                        "- jeda >= 0\n"
                        "- durasi_menit > 0\n"
                        "- jumlah_pesan > 0"
                    )
            except ValueError:
                return await event.respond(
                    "❌ Parameter numerik tidak valid!\n"
                    "Format: /forward forward <sumber> <id> <jumlah_grup> <jeda> <durasi_menit> <jumlah_pesan>"
                )
            
            # Source adalah bagian pertama
            source = fparts[0]
            
            # Message IDs adalah bagian antara source dan parameter
            # Bisa single ID atau multiple IDs dipisahkan ||
            message_parts = ' '.join(fparts[1:-4])
            message_str = message_parts  # Bisa berisi || untuk multiple messages
            
            delay_setting[event.sender_id] = jeda_batch
            # Simpan task info untuk bisa di-stop
            if event.sender_id not in active_forward_tasks:
                active_forward_tasks[event.sender_id] = {'stop_flag': False}
            task = asyncio.create_task(
                forward_job(event.sender_id, mode, source, message_str, jumlah, durasi, jumlah_pesan)
            )
            active_forward_tasks[event.sender_id]['task'] = task
            await task
            
        elif mode == 'text':
            # Parse text mode
            # Format: "pesan1"||"pesan2"||... jumlah_grup jeda durasi_menit jumlah_pesan
            # Atau: pesan1||pesan2||... jumlah_grup jeda durasi_menit jumlah_pesan
            
            # Split dari kanan untuk mendapatkan parameter numerik (4 parameters)
            # rsplit dengan maxsplit=4 akan menghasilkan maksimal 5 bagian:
            # [text_part, jumlah_grup, jeda, durasi_menit, jumlah_pesan]
            sisa = rest.rsplit(' ', 4)
            if len(sisa) != 5:
                return await event.respond(
                    "Format salah:\n"
                    "/forward text <pesan1>||<pesan2>||... <jumlah_grup> <jeda> <durasi_menit> <jumlah_pesan>\n\n"
                    "Contoh: /forward text \"Halo semua!\" 10 5 60 300\n"
                    "Multiple pesan: /forward text \"Pesan 1\"||\"Pesan 2\" 10 5 60 300"
                )
            
            text_part = sisa[0]  # Bisa berisi || untuk multiple messages
            try:
                jumlah = int(sisa[1])
                jeda_batch = int(sisa[2])
                durasi = float(sisa[3])  # Durasi dalam menit
                jumlah_pesan = int(sisa[4])
                
                # Validasi parameter
                if jumlah <= 0 or jeda_batch < 0 or durasi <= 0 or jumlah_pesan <= 0:
                    return await event.respond(
                        "❌ Parameter tidak valid! Pastikan:\n"
                        "- jumlah_grup > 0\n"
                        "- jeda >= 0\n"
                        "- durasi_menit > 0\n"
                        "- jumlah_pesan > 0"
                    )
            except ValueError:
                return await event.respond(
                    "❌ Parameter harus berupa angka!\n"
                    "Format: /forward text <pesan> <jumlah_grup> <jeda> <durasi_menit> <jumlah_pesan>"
                )
            
            # Bersihkan quotes jika ada
            text = text_part.strip().strip('"').strip("'")

            if event.message.media:
                ensure_media_dir()
                filename = os.path.join(MEDIA_DIR, f"fwd_{event.sender_id}_{int(datetime.now().timestamp())}")
                try:
                    saved = await client.download_media(event.message, file=filename)
                    text = {'text': text, 'media_path': saved}
                except Exception as e:
                    logging.error(f"Error download media /forward text: {e}")
                    return await event.respond(f"Gagal memproses media: {e}")
            
            delay_setting[event.sender_id] = jeda_batch
            pesan_simpan[event.sender_id] = text
            # Simpan task info untuk bisa di-stop
            if event.sender_id not in active_forward_tasks:
                active_forward_tasks[event.sender_id] = {'stop_flag': False}
            task = asyncio.create_task(
                forward_job(event.sender_id, mode, '', text, jumlah, durasi, jumlah_pesan)
            )
            active_forward_tasks[event.sender_id]['task'] = task
            await task
        else:
            await event.respond("Mode harus 'forward' atau 'text'")
    except Exception as e:
        err_msg = f"Error: {e}"
        logging.error(err_msg)
        await event.respond(err_msg)

@client.on(events.NewMessage(pattern=r'^/setdelay\b'))
async def set_delay(event):
    if not await require_allowed(event):
        return
    try:
        parts = event.message.raw_text.split(maxsplit=1)
        delay = int(parts[1])
        delay_setting[event.sender_id] = delay
        save_delay_settings()
        await event.respond(f"Jeda antar batch diset ke {delay} detik!")
    except Exception as e:
        logging.error(f"Error pada /setdelay: {e}")
        await event.respond("Gunakan: /setdelay <detik>")

@client.on(events.NewMessage(pattern=r'^/setdelaygroup\s+(\d+)'))
async def set_delay_group(event):
    if not await require_allowed(event):
        return
    try:
        delay = int(event.pattern_match.group(1))
        delay_per_group_setting[event.sender_id] = delay
        save_delay_settings()
        await event.respond(f"Delay antar grup diset ke {delay} detik!")
    except Exception as e:
        logging.error(f"Error pada /setdelaygroup: {e}")
        await event.respond("Gunakan: /setdelaygroup <detik>")

@client.on(events.NewMessage(pattern=r'^/cekdelaygroup\b'))
async def cek_delay_group(event):
    if not await require_allowed(event):
        return
    delay = delay_per_group_setting.get(event.sender_id, 0)
    await event.respond(f"Delay antar grup saat ini: {delay} detik")

@client.on(events.NewMessage(pattern=r'^/resetdelaygroup\b'))
async def reset_delay_group(event):
    if not await require_allowed(event):
        return
    delay_per_group_setting.pop(event.sender_id, None)
    save_delay_settings()
    await event.respond("Delay antar grup telah direset ke default (0 detik)")

@client.on(events.NewMessage(pattern=r'^/review_pesan\b'))
async def review_pesan(event):
    if not await require_allowed(event):
        return
    user_id = event.sender_id
    pesan = pesan_simpan.get(user_id, "Belum ada pesan default yang disimpan.")
    await event.respond(f"Pesan default Anda:\n\n{pesan}\n\nUntuk mengubah: /ubah_pesan <pesan baru>")

@client.on(events.NewMessage(pattern=r'^/ubah_pesan(?:\s+(.+))?$'))
async def ubah_pesan(event):
    if not await require_allowed(event):
        return
    try:
        raw_text = event.message.raw_text.strip()
        parts = raw_text.split(maxsplit=1)
        if len(parts) < 2:
            return await event.respond("Format salah. Gunakan: /ubah_pesan <pesan baru>")
        pesan_baru = parts[1]
        pesan_simpan[event.sender_id] = pesan_baru.strip()
        save_pesan_simpan()
        await event.respond(f"Pesan default berhasil diubah menjadi:\n\n{pesan_baru.strip()}")
    except Exception as e:
        logging.error(f"Error pada /ubah_pesan: {e}")
        await event.respond("Format salah. Gunakan: /ubah_pesan <pesan baru>")

@client.on(events.NewMessage(pattern=r'^/simpan_preset\b'))
async def simpan_preset(event):
    if not await require_allowed(event):
        return
    try:
        raw_text = event.message.raw_text.strip()
        parts = raw_text.split(maxsplit=2)
        if len(parts) < 3:
            return await event.respond("Format salah. Gunakan: /simpan_preset <nama_preset> <isi_pesan>")
        
        nama_preset = parts[1]
        isi_pesan = parts[2]
        
        user_id = event.sender_id
        if user_id not in preset_pesan:
            preset_pesan[user_id] = {}
        
        preset_pesan[user_id][nama_preset] = isi_pesan.strip()
        save_preset()
        await event.respond(f"Preset '{nama_preset}' berhasil disimpan!")
    except Exception as e:
        logging.error(f"Error pada /simpan_preset: {e}")
        await event.respond("Format salah. Gunakan: /simpan_preset <nama_preset> <isi_pesan>")

@client.on(events.NewMessage(pattern=r'^/pakai_preset\s+(\S+)'))
async def pakai_preset(event):
    if not await require_allowed(event):
        return
    try:
        nama_preset = event.pattern_match.group(1)
        user_id = event.sender_id
        
        if user_id not in preset_pesan or nama_preset not in preset_pesan[user_id]:
            return await event.respond(f"Preset '{nama_preset}' tidak ditemukan!")
        
        pesan_simpan[user_id] = preset_pesan[user_id][nama_preset]
        save_pesan_simpan()
        await event.respond(f"Preset '{nama_preset}' telah dipilih sebagai pesan default:\n\n{preset_pesan[user_id][nama_preset]}")
    except Exception as e:
        logging.error(f"Error pada /pakai_preset: {e}")
        await event.respond("Format salah. Gunakan: /pakai_preset <nama_preset>")

@client.on(events.NewMessage(pattern=r'^/list_preset\b'))
async def list_preset(event):
    if not await require_allowed(event):
        return
    user_id = event.sender_id
    
    if user_id not in preset_pesan or not preset_pesan[user_id]:
        return await event.respond("Anda belum memiliki preset pesan.")
    
    teks = "Daftar preset pesan Anda:\n\n"
    for idx, (nama, isi) in enumerate(preset_pesan[user_id].items(), 1):
        preview = isi[:50] + "..." if len(isi) > 50 else isi
        teks += f"{idx}. {nama}\n   Preview: {preview}\n"
        teks += f"   Pakai: /pakai_preset {nama}\n"
        teks += f"   Hapus: /hapus_preset {nama}\n\n"
    
    await event.respond(teks)

@client.on(events.NewMessage(pattern=r'^/edit_preset\b'))
async def edit_preset(event):
    if not await require_allowed(event):
        return
    try:
        raw_text = event.message.raw_text.strip()
        parts = raw_text.split(maxsplit=2)
        if len(parts) < 3:
            return await event.respond("Format salah. Gunakan: /edit_preset <nama_preset> <isi_baru>")
        
        nama_preset = parts[1]
        isi_baru = parts[2]
        
        user_id = event.sender_id
        if user_id not in preset_pesan or nama_preset not in preset_pesan[user_id]:
            return await event.respond(f"Preset '{nama_preset}' tidak ditemukan!")
        
        preset_pesan[user_id][nama_preset] = isi_baru.strip()
        save_preset()
        await event.respond(f"Preset '{nama_preset}' berhasil diubah!")
    except Exception as e:
        logging.error(f"Error pada /edit_preset: {e}")
        await event.respond("Format salah. Gunakan: /edit_preset <nama_preset> <isi_baru>")

@client.on(events.NewMessage(pattern=r'^/hapus_preset\s+(\S+)'))
async def hapus_preset(event):
    if not await require_allowed(event):
        return
    try:
        nama_preset = event.pattern_match.group(1)
        user_id = event.sender_id
        
        if user_id not in preset_pesan or nama_preset not in preset_pesan[user_id]:
            return await event.respond(f"Preset '{nama_preset}' tidak ditemukan!")
        
        del preset_pesan[user_id][nama_preset]
        save_preset()
        await event.respond(f"Preset '{nama_preset}' berhasil dihapus!")
    except Exception as e:
        logging.error(f"Error pada /hapus_preset: {e}")
        await event.respond("Format salah. Gunakan: /hapus_preset <nama_preset>")

@client.on(events.NewMessage(pattern=r'^/review\b'))
async def review_jobs(event):
    if not await require_allowed(event):
        return
    user_id = event.sender_id
    teks = "== Jadwal Aktif ==\n"
    if not job_data:
        teks += "Tidak ada jadwal."
    else:
        user_jobs = {jid: info for jid, info in job_data.items() if info.get('user') == user_id}
        if not user_jobs:
            teks += "Tidak ada jadwal untuk Anda."
        else:
            for job_id, info in user_jobs.items():
                durasi_jam = info['durasi'] / 60.0
                teks += (
                    f"- ID: {job_id}\n"
                    f"  Mode: {info['mode']}\n"
                    f"  Grup: {info['jumlah']}\n"
                    f"  Durasi: {info['durasi']} menit ({durasi_jam:.2f} jam)\n"
                    f"  Hapus: /deletejob {job_id}\n\n"
                )
    await event.respond(teks)

@client.on(events.NewMessage(pattern=r'^/deletejob\b'))
async def delete_job(event):
    if not await require_allowed(event):
        return
    try:
        parts = event.message.raw_text.split()
        if len(parts) < 2:
            return await event.respond("Format salah. Gunakan: /deletejob <job_id>\nLihat ID jadwal dengan /review")
        job_id = parts[1]
        if job_id not in job_data:
            return await event.respond(f"Job ID '{job_id}' tidak ditemukan.")
        if job_data[job_id].get('user') != event.sender_id:
            return await event.respond("Job ini bukan milik Anda!")
        scheduler.remove_job(job_id)
        job_data.pop(job_id, None)
        JOBS.pop(job_id, None)
        await event.respond("Jadwal berhasil dihapus!")
    except Exception as e:
        logging.error(f"Error pada /deletejob: {e}")
        await event.respond("Gagal menghapus. Pastikan ID yang dimasukkan benar.")

@client.on(events.NewMessage(pattern=r'^/stopforward\b'))
async def stop_forward(event):
    if not await require_allowed(event):
        return
    user_id = event.sender_id
    removed = []
    
    # Hentikan scheduled jobs
    for job in scheduler.get_jobs():
        if str(user_id) in job.id:
            try:
                scheduler.remove_job(job.id)
                job_data.pop(job.id, None)
                JOBS.pop(job.id, None)
                removed.append(job.id)
            except Exception as e:
                logging.error(f"Error menghapus job {job.id}: {e}")
    
    # Hentikan active forward tasks
    stopped_active = False
    if user_id in active_forward_tasks:
        active_forward_tasks[user_id]['stop_flag'] = True
        stopped_active = True
        logging.info(f"Stop flag diaktifkan untuk user {user_id}")
    
    if removed or stopped_active:
        msg = []
        if removed:
            msg.append(f"Job terjadwal dihapus: {', '.join(removed)}")
        if stopped_active:
            msg.append("⏹️ Pengiriman pesan aktif sedang dihentikan...")
        await event.respond("\n".join(msg))
    else:
        await event.respond("Tidak ditemukan job forward aktif untuk Anda.")

@client.on(events.NewMessage(pattern=r'^/stop\b'))
async def stop_cmd(event):
    """Alias untuk /stopforward - menghentikan forward yang sedang berlangsung dengan segera"""
    if not await require_allowed(event):
        return
    # Panggil fungsi yang sama dengan /stopforward
    await stop_forward(event)

@client.on(events.NewMessage(pattern=r'^/blacklist_add\b'))
async def add_blacklist(event):
    if not await require_allowed(event):
        return
    try:
        nama = " ".join(event.message.raw_text.split()[1:]).strip()
        if not nama:
            return await event.respond("Format salah. Gunakan: /blacklist_add <nama grup>")
        blacklisted_groups.add(nama)
        save_blacklist()
        await event.respond(f"'{nama}' berhasil masuk ke blacklist!")
    except Exception as e:
        logging.error(f"Error pada /blacklist_add: {e}")
        await event.respond("Format salah. Gunakan: /blacklist_add <nama grup>")

@client.on(events.NewMessage(pattern=r'^/blacklist_remove\b'))
async def remove_blacklist(event):
    if not await require_allowed(event):
        return
    try:
        nama = " ".join(event.message.raw_text.split()[1:]).strip()
        if not nama:
            return await event.respond("Format salah. Gunakan: /blacklist_remove <nama grup>")
        if nama not in blacklisted_groups:
            return await event.respond(f"'{nama}' tidak ada di blacklist!")
        blacklisted_groups.discard(nama)
        save_blacklist()
        await event.respond(f"'{nama}' telah dihapus dari blacklist!")
    except Exception as e:
        logging.error(f"Error pada /blacklist_remove: {e}")
        await event.respond("Format salah. Gunakan: /blacklist_remove <nama grup>")

@client.on(events.NewMessage(pattern=r'^/list_blacklist\b'))
async def list_blacklist(event):
    if not await require_allowed(event):
        return
    if not blacklisted_groups:
        await event.respond("Blacklist kosong!")
    else:
        teks = "== Grup dalam blacklist ==\n\n"
        for idx, nama in enumerate(sorted(blacklisted_groups), 1):
            teks += f"{idx}. {nama}\n"
            teks += f"   Hapus: /blacklist_remove {nama}\n\n"
        await event.respond(teks)

@client.on(events.NewMessage(pattern=r'^/status\b'))
async def cek_status(event):
    now = datetime.now()
    uptime = now - start_time
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    await event.respond(
        "Status: aktif\n"
        f"Uptime: {days} hari {hours} jam {minutes} menit\n"
        f"Total pesan terkirim (session): {TOTAL_SENT_MESSAGES}"
    )

@client.on(events.NewMessage(pattern=r'^/ping\b'))
async def ping(event):
    await event.respond("Bot aktif dan siap melayani! Ketik /help untuk lihat semua perintah.")

@client.on(events.NewMessage(pattern=r'^/debug_grup\b'))
async def debug_grup(event):
    if not await require_allowed(event):
        return
    try:
        teks = "=== Debug Grup ===\n\n"
        total_dialog = 0
        total_grup = 0
        total_channel = 0
        total_private = 0
        grup_list = []
        
        async for dialog in client.iter_dialogs(limit=100):  # Limit untuk tidak terlalu lama
            total_dialog += 1
            dialog_name = dialog.name or "Tanpa Nama"
            
            try:
                from telethon.tl.types import Channel, Chat
                
                if dialog.is_group:
                    total_grup += 1
                    is_blacklisted = "❌ BLACKLIST" if dialog_name in blacklisted_groups else "✅"
                    grup_list.append(f"{is_blacklisted} {dialog_name}")
                elif isinstance(dialog.entity, Channel):
                    if hasattr(dialog.entity, 'megagroup') and dialog.entity.megagroup:
                        total_grup += 1
                        is_blacklisted = "❌ BLACKLIST" if dialog_name in blacklisted_groups else "✅"
                        grup_list.append(f"{is_blacklisted} {dialog_name} (Supergroup)")
                    elif hasattr(dialog.entity, 'broadcast') and dialog.entity.broadcast:
                        total_channel += 1
                    else:
                        total_grup += 1
                        is_blacklisted = "❌ BLACKLIST" if dialog_name in blacklisted_groups else "✅"
                        grup_list.append(f"{is_blacklisted} {dialog_name} (Channel Group)")
                elif isinstance(dialog.entity, Chat):
                    total_grup += 1
                    is_blacklisted = "❌ BLACKLIST" if dialog_name in blacklisted_groups else "✅"
                    grup_list.append(f"{is_blacklisted} {dialog_name} (Chat)")
                else:
                    total_private += 1
            except Exception as e:
                logging.error(f"Error debug dialog {dialog_name}: {e}")
                continue
        
        teks += f"Total Dialog: {total_dialog}\n"
        teks += f"Total Grup: {total_grup}\n"
        teks += f"Total Channel: {total_channel}\n"
        teks += f"Total Private: {total_private}\n"
        teks += f"Grup di Blacklist: {len(blacklisted_groups)}\n\n"
        teks += "=== Daftar Grup (10 pertama) ===\n"
        
        for grup in grup_list[:10]:
            teks += f"{grup}\n"
        
        if len(grup_list) > 10:
            teks += f"\n... dan {len(grup_list) - 10} grup lainnya"
        
        await event.respond(teks)
    except Exception as e:
        logging.error(f"Error pada /debug_grup: {e}")
        await event.respond(f"Error: {str(e)}")

@client.on(events.NewMessage(pattern=r'^/restart\b'))
async def restart(event):
    if not await require_allowed(event):
        return
    await event.respond("Bot akan restart...")
    logging.info("Restarting bot upon command...")
    os.execv(sys.executable, [sys.executable] + sys.argv)

@client.on(events.NewMessage(pattern=r'^/log\b'))
async def log_handler(event):
    if not await require_allowed(event):
        return
    try:
        with open('bot.log','r',encoding='utf-8') as log_file:
            logs = log_file.read()
            if len(logs) > 4000:
                logs = logs[-4000:]
            await event.respond(f"Log Terbaru!:\n{logs}")
    except FileNotFoundError:
        await event.respond("Log tidak ditemukan bub :(")
    except Exception as e:
        logging.error(f"Error membaca log: {e}")
        await event.respond(f"Yahh ada masalah dalam membaca log: {str(e)}")

PENGEMBANG_USERNAME = "explicist"
@client.on(events.NewMessage(pattern=r'^/feedback(?:\s+(.*))?'))
async def feedback_handler(event):
    msg = event.pattern_match.group(1)

    if not msg:
        return await event.reply(
            "Kirim saran/kritik atau pesan lain kepada pengembangku: /feedback <pesan>"
        )

    logging.info(f"Feedback dari {event.sender_id}: {msg}")

    # Balasan ke pengirim
    await event.reply(
        "Terima kasih atas feedback-nya!\n"
        "Masukanmu sangat berarti dan akan kami baca dengan penuh cinta!"
    )

    # Buat format feedback
    sender = await event.get_sender()
    name = sender.first_name if sender else "Tidak diketahui"
    username = f"@{sender.username}" if sender and sender.username else "Tidak ada"
    user_id = sender.id if sender else "Tidak diketahui"
    message = msg

    feedback_text = (
        "Feedback Baru!\n\n"
        f"Dari: {name}\n"
        f"Username: {username}\n"
        f"ID: {user_id}\n"
        f"Pesan: {message}"
    )

    # Kirim feedback ke pengembang
    try:
        await client.send_message(PENGEMBANG_USERNAME, feedback_text)
    except Exception as e:
        await event.reply("Ups! Gagal mengirim feedback ke pengembang. Coba lagi nanti yaaw.")
        print(f"[Feedback Error] {e}")

@client.on(events.NewMessage(pattern=r'^/reply (\d+)\s+([\s\S]+)', from_users=PENGEMBANG_USERNAME))
async def reply_to_user(event):
    match = event.pattern_match
    user_id = int(match.group(1))
    reply_message = match.group(2).strip()
    try:
        await client.send_message(user_id, f"Pesan dari pengembang:\n\n{reply_message}")
        await event.reply("Balasanmu sudah dikirim ke pengguna!")
    except Exception as e:
        await event.reply("Gagal mengirim balasan ke pengguna. Mungkin user sudah block bot?")
        print(f"[Reply Error] {e}")

@client.on(events.NewMessage(pattern=r'^/help\b'))
async def help_cmd(event):
    teks = """
PANDUAN USERBOT HEARTIE
Hai, sayang! Aku Heartie, userbot-mu yang siap membantu menyebarkan pesan cinta ke semua grup-grup favoritmu. Berikut daftar perintah yang bisa kamu gunakan:
============================
/forward
Kirim pesan langsung ke grup.
— Mode forward (dari channel):
 /forward forward @namachannel id_pesan jumlah_grup jeda durasi_menit jumlah_pesan_perhari
 Contoh: /forward forward @usnchannel 27 50 5 180 300
 (kirim pesan ID 27 ke 50 grup, jeda 5 detik, durasi 180 menit/3 jam, max 300 pesan/hari)
 Untuk multiple pesan: /forward forward @channel id1||id2||id3 50 5 180 300
— Mode text (kirim teks langsung):
 /forward text "Halo semua!" jumlah_grup jeda durasi_menit jumlah_pesan_perhari
 Contoh: /forward text "Halo semua!" 10 5 60 300
 (kirim ke 10 grup, jeda 5 detik, durasi 60 menit/1 jam, max 300 pesan/hari)
 Untuk multiple pesan: /forward text "Pesan 1"||"Pesan 2"||"Pesan 3" 10 5 60 300
 Untuk kirim media (foto/video/file): attach media saat mengirim command /forward text. Caption akan ikut.
============================
2. /scheduleforward
 Jadwalkan pesan mingguan otomatis.
 — Format:
 /scheduleforward mode pesan/sumber jumlah_grup durasi_menit jeda jumlah_pesan hari1,day2 jam:menit
 — Contoh:
 /scheduleforward forward @usnchannel 20 120 5 300 senin,jumat 08:00
 /scheduleforward text "Halo dari bot!" 30 180 5 300 selasa,rabu 10:00
 Untuk multiple pesan: /scheduleforward text "Pesan 1"||"Pesan 2" 30 180 5 300 senin 08:00
 Untuk kirim media terjadwal: attach media saat mengirim command /scheduleforward text.
============================
3. Manajemen Preset & Pesan
 — /review_pesan — Lihat pesan default
 — /ubah_pesan — Ubah pesan default
 — /simpan_preset — Simpan preset pesan
 — /pakai_preset — Pilih preset sebagai pesan default
 — /list_preset — Tampilkan daftar preset
 — /edit_preset — Edit preset pesan
 — /hapus_preset — Hapus preset
============================
4. Pengaturan Job Forward & Delay
 — /review — Tampilkan jadwal aktif
 — /deletejob — Hapus jadwal forward
 — /setdelay — Atur jeda antar batch kirim
 — /stop — Menghentikan forward yang sedang berlangsung SEGERA (alias /stopforward)
 — /stopforward — Hentikan semua job forward aktif kamu (termasuk yang sedang berjalan)
 — /setdelaygroup 5 — Set delay antar grup ke 5 detik (bisa diubah)
 — /cekdelaygroup — Cek delay antar grup kamu saat ini
 — /resetdelaygroup — Reset delay antar grup ke default
 — /setrandombatch <min> <max> — Random jeda per-batch
 — /setrandomgroup <min> <max> — Random jeda per-grup
 — /clearrandom — Hapus random jeda
 — /setmemberfilter <min|-> <max|-> — Filter grup berdasarkan jumlah member
 — /clearmemberfilter — Hapus filter member
============================
5. Blacklist Grup
 — /blacklist_add — Tambahkan grup ke blacklist
 — /blacklist_remove — Hapus grup dari blacklist
 — /list_blacklist — Lihat daftar grup dalam blacklist
============================
6. User Allowed
 — /adduser <id> — Menambahkan user yang diizinkan memakai userbot
  — /removeuser <id> — Menghapus user dari daftar yang diizinkan
  — /listuser — Menampilkan daftar user yang diizinkan memakai userbot
============================
7. Info & Lain-lain
 — /status — Cek status bot
  — /ping — Periksa apakah bot aktif
  — /previewgrup [jumlah] — Preview target grup sebelum kirim
  — /listaktif [jam] [limit] — Lihat grup aktif berdasarkan pesan terakhir
  — /failedgrup — Daftar grup gagal menerima pesan (session terakhir)
  — /cekresiko <jumlah_grup> <jeda_grup> <jeda_batch> — Heuristik cek risiko flood
  — /join @username — Join grup/channel publik
  — /leave @username — Leave grup/channel publik
  — /log — Tampilkan log aktivitas bot
  — /clearlog — Hapus isi log
  — /feedback — Kirim feedback ke pengembang
  — /stats — Lihat statistik penggunaan forward
  — /info — Lihat informasi tentang bot
  — /debug_grup — Debug info grup yang terdeteksi
  — /myid — Lihat ID Telegram kamu
  — /restart — Restart bot
============================
Cara mendapatkan ID pesan channel:
Klik kanan bagian kosong (atau tap lama) pada pesan di channel → Salin link.
Misal, jika linknya https://t.me/channel/19 maka id pesan adalah 19.
Selamat mencoba dan semoga hari-harimu penuh cinta! Kalau masih ada yang bingung bisa chat pengembangku/kirimkan feedback ya!
"""
    await event.respond(teks)

@client.on(events.NewMessage(pattern=r'^/info\b'))
async def info_handler(event):
    if not await is_allowed(event):
        return
    now = datetime.now()
    uptime = now - start_time
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)

    text = (
        "Tentang Heartie Bot\n\n"
        "Haiii! Aku Heartie, sahabatmu yang selalu setia meneruskan pesan penuh cinta ke grup-grup kesayanganmu~\n\n"
        "Dibuat oleh: @explicist\n"
        "Versi: 1.2.0\n"
        "Ditenagai oleh: Python + Telethon\n"
        "Fungsi: Kirim & jadwalkan pesan otomatis ke banyak grup\n\n"
        f"Uptime: {hours} jam, {minutes} menit\n"
        f"Aktif sejak: {start_time.strftime('%d %B %Y pukul %H:%M')}\n\n"
        "Panduan: /help\n"
        "Statistik: /stats\n"
        "Hubungi pembuat: https://t.me/explicist"
    )

    await event.respond(text)



@client.on(events.NewMessage(pattern=r'^/stats\b'))
async def stats_handler(event):
    if not await is_allowed(event):
        return
    try:
        global TOTAL_SENT_MESSAGES

        sender = await event.get_sender()
        name = sender.first_name or "Pengguna"
        username = f"@{sender.username}" if sender.username else "(tanpa username)"

        try:
            with open('allowed_users.txt', 'r') as f:
                total_users = len(f.readlines())
        except FileNotFoundError:
            total_users = 1

        stats_text = (
            f"Haii {name} ({username})!\n\n"
            "Statistik Heartie Bot:\n"
            f"Total job aktif: {len(JOBS)}\n"
            f"Total pesan terkirim: {TOTAL_SENT_MESSAGES}\n"
            f"Total pengguna terdaftar: {total_users}\n"
            f"Waktu server sekarang: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            "Refresh: /stats\n"
            "Download log: /log"
        )

        await event.respond(stats_text)
    except Exception as e:
        await event.respond(f"Terjadi kesalahan: {e}")

@client.on(events.NewMessage(pattern=r'^/adduser(?:\s+(\d+))?'))
async def add_user_cmd(event):
    if not await require_allowed(event):
        return

    match = event.pattern_match.group(1)
    if not match:
        me = await client.get_me()
        return await event.reply(
            f"Format: /adduser <user_id>\n"
            f"Contoh: /adduser {me.id}\n\n"
            f"ID kamu sendiri: {me.id}"
        )

    add_id = int(match)

    if add_id in ALLOWED_USERS:
        return await event.reply("User ini sudah diizinkan sebelumnya kok!")

    ALLOWED_USERS.add(add_id)
    save_allowed_users()

    try:
        user = await client.get_entity(add_id)
        nama = user.first_name or "Tanpa Nama"
        username = f"@{user.username}" if user.username else "(tanpa username)"
    except Exception:
        nama = "Tidak diketahui"
        username = "(tanpa username)"

    await event.reply(
        f"Yey! User ini sekarang sudah jadi bagian dari Heartie Club!\n\n"
        f"Nama: {nama}\n"
        f"Username: {username}\n"
        f"ID: {add_id}"
    )

@client.on(events.NewMessage(pattern=r'^/listuser\b'))
async def list_users(event):
    if not await is_allowed(event):
        return

    if not ALLOWED_USERS:
        return await event.reply("Belum ada user yang diizinkan, sayang...")

    teks = "Daftar pengguna yang diizinkan:\n\n"

    for uid in sorted(ALLOWED_USERS):
        try:
            user = await client.get_entity(uid)
            nama = user.first_name or "Tanpa Nama"
            username = f"@{user.username}" if user.username else "(tanpa username)"
        except Exception:
            nama = "Tidak diketahui"
            username = "(tanpa username)"

        teks += f"{nama} | {username} | {uid}\n"
        teks += f"   Hapus: /removeuser {uid}\n\n"

    await event.reply(teks)

@client.on(events.NewMessage(pattern=r'^/removeuser(?:\s+(\d+))?'))
async def remove_user_cmd(event):
    if not await require_allowed(event):
        return

    match = event.pattern_match.group(1)
    if not match:
        return await event.reply("Format: /removeuser <user_id>")

    remove_id = int(match)

    if remove_id not in ALLOWED_USERS:
        return await event.reply("User sudah tidak ada di daftar!")

    ALLOWED_USERS.remove(remove_id)
    save_allowed_users()
    try:
        user = await client.get_entity(remove_id)
        nama = user.first_name or "Tanpa Nama"
        username = f"@{user.username}" if user.username else "(tanpa username)"
    except Exception:
        nama = "Tidak diketahui"
        username = "(tanpa username)"

    await event.reply(
        f"{remove_id} telah dihapus dari daftar user yang diizinkan.\n"
        f"Nama: {nama}\n"
        f"Username: {username}"
    )

# === COMMAND TAMBAHAN ===
@client.on(events.NewMessage(pattern=r'^/myid\b'))
async def myid_cmd(event):
    sender = await event.get_sender()
    nama = sender.first_name or "Tanpa Nama"
    username = f"@{sender.username}" if sender.username else "(tanpa username)"
    await event.respond(
        f"Info ID kamu:\n\n"
        f"Nama: {nama}\n"
        f"Username: {username}\n"
        f"ID: {sender.id}\n\n"
        f"Gunakan ID ini untuk /adduser {sender.id}"
    )

@client.on(events.NewMessage(pattern=r'^/clearlog\b'))
async def clearlog_cmd(event):
    if not await require_allowed(event):
        return
    try:
        with open('bot.log', 'w', encoding='utf-8') as f:
            f.write('')
        logging.info(f"Log dibersihkan oleh user {event.sender_id}")
        await event.respond("Log berhasil dibersihkan!")
    except Exception as e:
        logging.error(f"Error pada /clearlog: {e}")
        await event.respond(f"Gagal membersihkan log: {e}")

# === SETUP FLASK UNTUK KEEP ALIVE ===
app = Flask(__name__)

@app.route('/')
def home():
    return "Heartie Bot is alive!"

@app.route('/ping')
def flask_ping():
    return "Xixi! Bot masih hidup."

def keep_alive():
    port = int(os.environ.get('PORT', '8000'))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=keep_alive, daemon=True).start()

# === JALANKAN BOT ===
async def main():
    await client.start()
    scheduler.start()
    me = await client.get_me()
    welcome_msg = f"💖 Bot aktif, kamu masuk sebagai {me.first_name}. Menunggu perintahmu, sayang!"
    print(welcome_msg)
    logging.info(welcome_msg)
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())