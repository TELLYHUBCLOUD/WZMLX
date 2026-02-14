from asyncio import gather, iscoroutinefunction
from html import escape
from re import findall
from time import time

from psutil import cpu_percent, disk_usage, virtual_memory

from ... import (
    DOWNLOAD_DIR,
    bot_cache,
    bot_start_time,
    status_dict,
    task_dict,
    task_dict_lock,
)
from ...core.config_manager import Config
from ..telegram_helper.bot_commands import BotCommands
from ..telegram_helper.button_build import ButtonMaker

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


class MirrorStatus:
    STATUS_UPLOAD = "Upload"
    STATUS_DOWNLOAD = "Download"
    STATUS_CLONE = "Clone"
    STATUS_QUEUEDL = "QueueDl"
    STATUS_QUEUEUP = "QueueUp"
    STATUS_PAUSED = "Pause"
    STATUS_ARCHIVE = "Archive"
    STATUS_EXTRACT = "Extract"
    STATUS_SPLIT = "Split"
    STATUS_CHECK = "CheckUp"
    STATUS_SEED = "Seed"
    STATUS_SAMVID = "SamVid"
    STATUS_CONVERT = "Convert"
    STATUS_FFMPEG = "FFmpeg"


class EngineStatus:
    def __init__(self):
        eng_vers = bot_cache.get('eng_versions', {})
        self.STATUS_ARIA2 = f"Aria2 v{eng_vers.get('aria2', 'N/A')}"
        self.STATUS_AIOHTTP = f"AioHttp v{eng_vers.get('aiohttp', 'N/A')}"
        self.STATUS_GDAPI = f"Google-API v{eng_vers.get('gapi', 'N/A')}"
        self.STATUS_QBIT = f"qBit v{eng_vers.get('qBittorrent', 'N/A')}"
        self.STATUS_TGRAM = f"Pyro v{eng_vers.get('pyrofork', 'N/A')}"
        self.STATUS_MEGA = f"MegaAPI v{eng_vers.get('mega', 'N/A')}"
        self.STATUS_YTDLP = f"yt-dlp v{eng_vers.get('yt-dlp', 'N/A')}"
        self.STATUS_FFMPEG = f"ffmpeg v{eng_vers.get('ffmpeg', 'N/A')}"
        self.STATUS_7Z = f"7z v{eng_vers.get('7z', 'N/A')}"
        self.STATUS_RCLONE = f"RClone v{eng_vers.get('rclone', 'N/A')}"
        self.STATUS_QUEUE = "QSystem v2"
        self.STATUS_JD = "JDownloader v2"


STATUSES = {
    "ALL": "All",
    "DL": MirrorStatus.STATUS_DOWNLOAD,
    "UP": MirrorStatus.STATUS_UPLOAD,
    "QD": MirrorStatus.STATUS_QUEUEDL,
    "QU": MirrorStatus.STATUS_QUEUEUP,
    "AR": MirrorStatus.STATUS_ARCHIVE,
    "EX": MirrorStatus.STATUS_EXTRACT,
    "SD": MirrorStatus.STATUS_SEED,
    "CL": MirrorStatus.STATUS_CLONE,
    "CM": MirrorStatus.STATUS_CONVERT,
    "SP": MirrorStatus.STATUS_SPLIT,
    "SV": MirrorStatus.STATUS_SAMVID,
    "FF": MirrorStatus.STATUS_FFMPEG,
    "PA": MirrorStatus.STATUS_PAUSED,
    "CK": MirrorStatus.STATUS_CHECK,
}


async def get_task_by_gid(gid: str):
    async with task_dict_lock:
        for tk in task_dict.values():
            if hasattr(tk, "seeding") and callable(getattr(tk, "update", None)):
                try:
                    await tk.update()
                except Exception:
                    pass  # Skip update errors to avoid breaking gid search
            if tk.gid() == gid:
                return tk
        return None


async def get_specific_tasks(status, user_id):
    if status == "All":
        if user_id:
            return [tk for tk in task_dict.values() if hasattr(tk, 'listener') and tk.listener.user_id == user_id]
        else:
            return [tk for tk in task_dict.values() if hasattr(tk, 'listener')]
    
    tasks_to_check = (
        [tk for tk in task_dict.values() if hasattr(tk, 'listener') and tk.listener.user_id == user_id]
        if user_id
        else [tk for tk in task_dict.values() if hasattr(tk, 'listener')]
    )
    
    coro_tasks = [tk for tk in tasks_to_check if iscoroutinefunction(tk.status)]
    coro_statuses = await gather(*[tk.status() for tk in coro_tasks], return_exceptions=True)
    
    result = []
    coro_index = 0
    for tk in tasks_to_check:
        if tk in coro_tasks:
            st = coro_statuses[coro_index]
            coro_index += 1
            if isinstance(st, Exception):
                continue  # Skip tasks with status errors
        else:
            try:
                st = tk.status()
            except Exception:
                continue
        
        # Handle download status special case (includes unknown statuses)
        if (st == status) or (
            status == MirrorStatus.STATUS_DOWNLOAD 
            and st not in STATUSES.values() 
            and st != "All"
        ):
            result.append(tk)
    return result


async def get_all_tasks(req_status: str, user_id):
    async with task_dict_lock:
        return await get_specific_tasks(req_status, user_id)


def get_raw_file_size(size):
    try:
        num, unit = size.split()
        return int(float(num) * (1024 ** SIZE_UNITS.index(unit.upper())))
    except (ValueError, KeyError, AttributeError):
        return 0


def get_readable_file_size(size_in_bytes):
    if not size_in_bytes or size_in_bytes <= 0:
        return "0B"

    index = 0
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1

    return f"{size_in_bytes:.2f}{SIZE_UNITS[index]}"


def get_readable_time(seconds: int):
    if seconds <= 0:
        return "0s"
    
    periods = [("d", 86400), ("h", 3600), ("m", 60), ("s", 1)]
    result = ""
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f"{int(period_value)}{period_name}"
    return result or "0s"


def get_raw_time(time_str: str) -> int:
    time_units = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    return sum(
        int(value) * time_units[unit]
        for value, unit in findall(r"(\d+)([dhms])", time_str)
    )


def time_to_seconds(time_duration):
    try:
        parts = time_duration.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(float, parts)
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = map(float, parts)
        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = float(parts[0])
        else:
            return 0
        return hours * 3600 + minutes * 60 + seconds
    except Exception:
        return 0


def speed_string_to_bytes(size_text: str):
    size = 0
    size_text = size_text.lower().strip()
    if not size_text:
        return 0
        
    if "k" in size_text:
        size += float(size_text.split("k")[0]) * 1024
    elif "m" in size_text:
        size += float(size_text.split("m")[0]) * 1048576
    elif "g" in size_text:
        size += float(size_text.split("g")[0]) * 1073741824
    elif "t" in size_text:
        size += float(size_text.split("t")[0]) * 1099511627776
    elif "b" in size_text:
        size += float(size_text.split("b")[0])
    return size


def get_progress_bar_string(pct):
    try:
        pct = float(str(pct).strip("%"))
        p = min(max(pct, 0), 100)
    except (ValueError, TypeError):
        p = 0

    total_blocks = 12
    filled_blocks = int(p / (100 / total_blocks))
    remaining_pct = p % (100 / total_blocks) if total_blocks > 0 else 0

    p_str = "𒊹" * filled_blocks

    # Only add partial block if there's remaining progress and space available
    if remaining_pct > 0 and filled_blocks < total_blocks:
        block_unit = 100 / total_blocks
        # Corrected thresholds: 25%, 50%, 75% of a single block's percentage value
        if remaining_pct < block_unit * 0.25:
            p_str += "◔"
        elif remaining_pct < block_unit * 0.5:
            p_str += "◑"
        elif remaining_pct < block_unit * 0.75:
            p_str += "◕"
        else:
            p_str += "𒊹"  # Near-complete block
            filled_blocks += 1  # Count as full block since it's >75% filled
    
    # Calculate remaining empty blocks
    empty_blocks = total_blocks - len(p_str)
    p_str += "❍" * max(0, empty_blocks)
    
    return f"[{p_str}]"


async def get_readable_message(sid, is_user, page_no=1, status="All", page_step=1):
    msg = ""
    button = None

    bot_header = Config.CUSTOM_BOT_HEADER or "TellY Mirror"
    bot_header_link = (Config.CUSTOM_BOT_HEADER_LINK or "https://t.me/tellY_mirror").strip()
    msg += f"<blockquote><b><i><a href='{bot_header_link}'>Powered By {bot_header}</a></i></b>\n\n</blockquote>"

    # Ensure status_dict has entry for sid
    if sid not in status_dict:
        status_dict[sid] = {"page_no": 1}
    
    tasks = await get_specific_tasks(status, sid if is_user else None)

    STATUS_LIMIT = Config.STATUS_LIMIT or 10  # Fallback to prevent division by zero
    tasks_no = len(tasks)
    pages = max((tasks_no + STATUS_LIMIT - 1) // STATUS_LIMIT, 1)
    
    # Normalize page number
    if page_no > pages:
        page_no = (page_no - 1) % pages + 1
    elif page_no < 1:
        page_no = pages - (abs(page_no) % pages)
    
    status_dict[sid]["page_no"] = page_no
    start_position = (page_no - 1) * STATUS_LIMIT

    # CRITICAL FIX: Corrected slice syntax (was using invalid » character)
    for index, task in enumerate(tasks[start_position:start_position + STATUS_LIMIT], start=1):
        try:
            if status != "All":
                tstatus = status
            elif iscoroutinefunction(task.status):
                tstatus = await task.status()
            else:
                tstatus = task.status()
            
            msg += f"<b>{index + start_position}.</b> <b>{escape(str(task.name()))}</b>"
            
            if hasattr(task.listener, 'subname') and task.listener.subname:
                msg += f"\n╰ <b>Sub Name</b> » <i>{escape(str(task.listener.subname))}</i>"
            
            elapsed = time() - getattr(task.listener.message, 'date', type('obj', (object,), {'timestamp': lambda: time()}())).timestamp()

            msg += f"\n<blockquote>╭ <b>Task By {task.listener.message.from_user.mention(style='html')} </b>"

            if (
                tstatus not in [MirrorStatus.STATUS_SEED, MirrorStatus.STATUS_QUEUEUP]
                and hasattr(task.listener, 'progress') and task.listener.progress
            ):
                try:
                    progress = task.progress()
                except Exception:
                    progress = "0%"
                
                msg += f"\n┊ <b>{get_progress_bar_string(progress)}</b> <i>{progress}</i>"
                
                subsize = ""
                count = ""
                if hasattr(task.listener, 'subname') and task.listener.subname:
                    if hasattr(task.listener, 'subsize'):
                        subsize = f" / {get_readable_file_size(task.listener.subsize)}"
                    if hasattr(task.listener, 'files_to_proceed') and hasattr(task.listener, 'proceed_count'):
                        ac = len(task.listener.files_to_proceed) if task.listener.files_to_proceed else 0
                        count = f"( {task.listener.proceed_count} / {ac or '?'} )"
                
                if getattr(task.listener, 'is_super_chat', False):
                    msg += f"\n┊ <b>Status »</b> <b><a href='{task.listener.message.link}'>{tstatus}</a> » {task.speed()}</b>"
                else:
                    msg += f"\n┊ <b>Status »</b> <b>{tstatus} » {task.speed()}</b>"
                
                msg += f"\n┊ <b>Done »</b> <i>{task.processed_bytes()}{subsize}</i>"
                msg += f"\n┊ <b>Total »</b> <i>{task.size()}</i>"
                if count:
                    msg += f"\n┊ <b>Count »</b> <b>{count}</b>"
                msg += f"\n┊ <b>ETA »</b> <i>{task.eta()}</i>"
                msg += f"\n┊ <b>Past »</b> <i>{get_readable_time(elapsed + get_raw_time(task.eta()))} ({get_readable_time(elapsed)})</i>"
                
                if tstatus == MirrorStatus.STATUS_DOWNLOAD and (
                    getattr(task.listener, 'is_torrent', False) or getattr(task.listener, 'is_qbit', False)
                ):
                    try:
                        msg += f"\n┊ <b>S/L »</b> {task.seeders_num()} / {task.leechers_num()} "
                    except Exception:
                        pass
            elif tstatus == MirrorStatus.STATUS_SEED:
                msg += f"\n┊ <b>Status »</b> <b>{tstatus} » {task.seed_speed()}</b>"
                msg += f"\n┊ <b>Done »</b> <i>{task.uploaded_bytes()}</i>"
                msg += f"\n┊ <b>Total »</b> <i>{task.size()}</i>"
                msg += f"\n┊ <b>Ratio »</b> <i>{task.ratio()}</i>"
                msg += f"\n┊ <b>ETA »</b> <i>{task.seeding_time()}</i>"
                msg += f"\n┊ <b>Past »</b> <i>{get_readable_time(elapsed)}</i>"
            else:
                msg += f"\n┊ <b>Size »</b> <i>{task.size()}</i>"
            
            msg += f"\n┊ <b>Engine »</b> <i>{getattr(task, 'engine', 'N/A')}</i>"
            msg += f"\n╰ <b>Mode »</b> <i>{getattr(task.listener, 'mode', ('', ''))[1]}</i></blockquote>"
            msg += f"\n<blockquote>⋗ <b>Stop »</b> <i>/{BotCommands.CancelTaskCommand[1]}_{task.gid()}</i></blockquote>\n\n"
        except Exception as e:
            # Skip problematic tasks but log error (in production would use logger)
            msg += f"<b>{index + start_position}.</b> <i>Error loading task: {type(e).__name__}</i>\n\n"
            continue

    if not msg.strip():
        if status == "All":
            return None, None
        else:
            msg = f"No Active {status} Tasks!\n\n"

    buttons = ButtonMaker()
    if not is_user:
        buttons.data_button("☲", f"status {sid} ov", position="header")
    
    if tasks_no > STATUS_LIMIT:
        msg += f"<b>Page:</b> {page_no}/{pages} | <b>Tasks:</b> {tasks_no} | <b>Step:</b> {page_step}\n"
        buttons.data_button("❰", f"status {sid} pre", position="header")
        buttons.data_button("❱", f"status {sid} nex", position="header")
        if tasks_no > 30:
            for i in [1, 2, 4, 6, 8, 10, 15]:
                buttons.data_button(str(i), f"status {sid} ps {i}", position="footer")
    
    if status != "All" or tasks_no > 20:
        for label, status_value in list(STATUSES.items()):
            if status_value != status and status_value != "All":
                buttons.data_button(label, f"status {sid} st {status_value}")
    
    buttons.data_button("♻", f"status {sid} ref", position="header")
    button = buttons.build_menu(8)

    msg += "\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬✘▬\n"
    msg += f"╭<b>CPU »</b> {cpu_percent()}% | <b>FREE »</b> {get_readable_file_size(disk_usage(DOWNLOAD_DIR).free)}\n"
    msg += f"╰<b>RAM »</b> {virtual_memory().percent}% | <b>UP »</b> {get_readable_time(time() - bot_start_time)}\n"
    return msg, button
