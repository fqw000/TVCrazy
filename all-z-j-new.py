import re
import csv
import requests
import concurrent.futures
import asyncio
import aiohttp
import os
import threading
from queue import Queue
import argparse
from collections import defaultdict

# ================== 频道分类规则（参考第二个 py） ==================
GROUP_RULES = [
    (r'(CCTV[-]?14|哈哈炫动|卡酷|宝宝|幼教|贝瓦|巧虎|新科动漫|小猪佩奇|汪汪队|海底小纵队|米老鼠|迪士尼|熊出没|猫和老鼠|哆啦A梦|喜羊羊|青少|儿童|动画|动漫|少儿|卡通|金鹰|disney|cartoon|nickelodeon|kids|kika|cbbc|哈哈炫动)', "🧸 儿童动画"),
    (r'(央视|CCTV[0-9]*[高清]?|CGTN|CCTV|风云音乐|第一剧场|怀旧剧场|女性时尚|风云足球|世界地理|兵器科技|电视指南)', "🇨🇳 央视频道"),
    (r'(卫视|湖南|浙江|江苏|北京|广东|深圳|东方|安徽|山东|河南|湖北|四川|辽宁|东南|天津|四川|内蒙古|云南)', "📺 卫视频道"),
    (r'(翡翠|明珠|凤凰|鳳凰东森|莲花|AMC|龙华|澳亚|港台|寰宇|TVB|华语|中天|东森|年代|民视|三立|星空|民视|台视|美亚|美亞|千禧|无线|無線|VIUTV|HOY|RTHK|Now|靖天|星卫|香港|澳门|台湾)', "🇭🇰 港澳台频道"),
    (r'(体育|CCTV5|高尔夫|足球|NBA|英超|西甲|欧冠)', "⚽ 体育频道"),
    (r'(电影|影院|CHC|HBO|星空|AXN|TCM|佳片)', "🎬 影视频道"),
    (r'(AMC|BET|Discovery|CBS|BET|cine|CNN|disney|epix|espn|fox|american|boomerang|cnbc|entertainment|fs|fuse|fx|hbo|国家地理|Animal Planet|BBC|NHK|DW|France24|CNN|Al Jazeera)', "🌍 国际频道"),
    (r'(教育|课堂|空中|大学|学习|国学|书画|考试|中学|学堂)', "🎓 教育频道"),
    (r"^(?=.*[a-zA-Z])(?!.*\b(cctv|cgtn)\b)[a-zA-Z0-9\s\-\+\&\.\'\!\(\)]+$", "🌍 国际频道"),
]

GROUP_OUTPUT_ORDER = [
    "🇨🇳 央视频道", "📺 卫视频道", "🎬 影视频道", "⚽ 体育频道",
    "🧸 儿童动画", "🌍 国际频道", "🎓 教育频道", "🇭🇰 港澳台频道", "📺 其他频道"
]


# 归一化频道名称（参考第二个 py 的严格逻辑 + 保留原映射）
def channel_name_normalize(name):
    if not name or not isinstance(name, str):
        return "Unknown"

    original = name.strip()
    if not original:
        return "Unknown"

    # 多频道拼接取主频道 (A-B-C -> A)
    if "-" in original and len(original.split("-")) >= 3:
        original = original.split("-", 1)[0].strip()

    name = original

    # 移除括号及内容
    name = re.sub(r'\s*[\(（【\[][^)）】\]]*[\)）】\]]\s*', '', name)

    # 统一连接符为空格
    name = re.sub(r'[\s\-·•_\|]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # 移除冗余后缀
    suffix_pattern = (
        r'(?:'
        r'HD|SD|FHD|UHD|4K|超高清|高清|蓝光|标清|'
        r'综合频道?|电视频道?|直播频道?|官方频道?|'
        r'频道|TV|台|官方|正版|流畅|备用|测试|'
        r'Ch|CH|Channel|咪咕|真|极速|'
        r')$'
    )
    name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()

    # 智能标准化 CCTV 编号
    def cctv_replacer(m):
        num_part = m.group(1)
        digits = re.search(r'[0-9]+', num_part)
        if not digits:
            return m.group(0)
        num_int = int(digits.group())
        suffix = ''
        if '+' in num_part:
            suffix = '+'
        elif 'k' in num_part.lower():
            suffix = 'K'
        return f"CCTV{num_int}{suffix}"

    name = re.sub(
        r'^CCTV[-\s]*([0-9][0-9\s\+\-kK]*)',
        cctv_replacer,
        name,
        flags=re.IGNORECASE
    )

    # 标准化 CGTN 前缀
    name = re.sub(r'^CGTN[-\s]+', 'CGTN ', name, flags=re.IGNORECASE).strip()

    # 常见频道别名映射
    name_map = {
        "CCTV1综合": "CCTV1", "CCTV2财经": "CCTV2", "CCTV3综艺": "CCTV3",
        "CCTV4国际": "CCTV4", "CCTV4中文国际": "CCTV4", "CCTV4欧洲": "CCTV4",
        "CCTV5体育": "CCTV5", "CCTV6电影": "CCTV6", "CCTV7军事": "CCTV7",
        "CCTV7军农": "CCTV7", "CCTV7农业": "CCTV7", "CCTV7国防军事": "CCTV7",
        "CCTV8电视剧": "CCTV8", "CCTV9记录": "CCTV9", "CCTV9纪录": "CCTV9",
        "CCTV10科教": "CCTV10", "CCTV11戏曲": "CCTV11", "CCTV12社会与法": "CCTV12",
        "CCTV13新闻": "CCTV13", "CCTV新闻": "CCTV13", "CCTV14少儿": "CCTV14",
        "CCTV15音乐": "CCTV15", "CCTV16奥林匹克": "CCTV16",
        "CCTV17农业农村": "CCTV17", "CCTV17农业": "CCTV17",
        "CCTV5+体育赛视": "CCTV5+", "CCTV5+体育赛事": "CCTV5+", "CCTV5+体育": "CCTV5+"
    }
    name = name_map.get(name, name)

    return name if name else original


def guess_group(title):
    """根据频道标题猜测所属分组"""
    for pat, grp in GROUP_RULES:
        if re.search(pat, title, re.IGNORECASE):
            return grp
    return "📺 其他频道"


def natural_sort_key(s):
    """自然排序：数字优先，例如 CCTV2 在 CCTV10 前面"""
    def convert(text):
        return int(text) if text.isdigit() else text.lower()

    return tuple(convert(p) for p in re.split(r'(\d+)', s))


# 获取频道名称中的数字
def channel_key(channel_name):
    match = re.search(r'\d+', channel_name)
    if match:
        return int(match.group())
    return float('inf')


# 生成同一C段的所有IP的URL
def generate_ip_range_urls(base_url, ip_address, port, suffix=None):
    ip_parts = ip_address.split('.')
    if len(ip_parts) < 3:
        return []
    c_prefix = '.'.join(ip_parts[:3])
    return [f"{base_url}{c_prefix}.{i}{port}{suffix if suffix else ''}" for i in range(1, 256)]


# 固定并发数，移除对psutil的依赖
def adjust_concurrency():
    return 100


# 增加超时重试机制
def is_url_accessible(url, retries=3):
    for _ in range(retries):
        try:
            response = requests.get(url, timeout=1)
            return url if response.status_code == 200 else None
        except requests.RequestException:
            continue
    return None


# 并发检测URL可用性
def check_urls_concurrent(urls, timeout=1, print_valid=True):
    max_workers = adjust_concurrency()

    def check_url(url):
        return is_url_accessible(url)

    valid_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(check_url, url) for url in urls]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                valid_urls.append(result)
                if print_valid:
                    print(result)
    return valid_urls


# jsmpeg模式获取频道
def get_channels_alltv(csv_file):
    urls = set()
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if 'host' not in reader.fieldnames:
            raise ValueError('CSV文件缺少host列')
        for row in reader:
            host = row['host'].strip()
            if host:
                urls.add(host if host.startswith(('http://', 'https://')) else f"http://{host}")

    ip_range_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        port = url[ip_end:]
        ip_range_urls.extend(generate_ip_range_urls(base_url, ip_address, port))

    valid_urls = check_urls_concurrent(set(ip_range_urls))
    channels = []
    for url in valid_urls:
        json_url = f"{url.rstrip('/')}/streamer/list"
        try:
            json_data = requests.get(json_url, timeout=1).json()
            host = url.rstrip('/')
            for item in json_data:
                name = item.get('name', '').strip()
                key = item.get('key', '').strip()
                if name and key:
                    channel_url = f"{host}/hls/{key}/index.m3u8"
                    channels.append((channel_name_normalize(name), channel_url))
        except Exception:
            continue
    return channels


# txiptv模式获取频道（异步）
async def get_channels_newnew(csv_file):
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        urls = list(set(row.get('link', '').strip() for row in reader if row.get('link')))

    async def modify_urls(url):
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        ip_parts = ip_address.split('.')
        if len(ip_parts) < 3:
            return []
        c_prefix = '.'.join(ip_parts[:3])
        port = url[ip_end:]
        ip_end = "/iptv/live/1000.json?key=txiptv"
        return [f"{base_url}{c_prefix}.{i}{port}{ip_end}" for i in range(1, 256)]

    async def is_url_accessible(session, url, semaphore):
        async with semaphore:
            try:
                async with session.get(url, timeout=1) as response:
                    return url if response.status == 200 else None
            except (aiohttp.ClientError, asyncio.TimeoutError):
                return None

    async def check_urls(session, urls, semaphore):
        tasks = []
        for url in urls:
            modified_urls = await modify_urls(url)
            tasks.extend(asyncio.create_task(is_url_accessible(session, modified_url, semaphore)) for modified_url in modified_urls)
        results = await asyncio.gather(*tasks)
        valid_urls = [result for result in results if result]
        for url in valid_urls:
            print(url)
        return valid_urls

    async def fetch_json(session, url, semaphore):
        async with semaphore:
            try:
                ip_start = url.find("//") + 2
                ip_index = url.find("/", url.find(".") + 1)
                base_url = url[:ip_start]
                ip_address = url[ip_start:ip_index]
                url_x = f"{base_url}{ip_address}"
                json_data = await session.get(url, timeout=1).json()
                channels = []
                for item in json_data.get('data', []):
                    if isinstance(item, dict):
                        name = item.get('name')
                        urlx = item.get('url')
                        if not name or not urlx:
                            continue
                        if ',' in urlx:
                            urlx = "aaaaaaaa"
                        urld = urlx if 'http' in urlx else f"{url_x}{urlx}"
                        channels.append((channel_name_normalize(name), urld))
                return channels
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                return []

    x_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        ip_dot = url.find(".") + 1
        ip_address = url[ip_start:url.find(".", ip_dot, url.find(".", ip_dot + 1)) + 1]
        port = url[ip_end:]
        x_urls.append(f"{url[:ip_start]}{ip_address}1{port}")

    unique_urls = set(x_urls)
    semaphore = asyncio.Semaphore(500)
    async with aiohttp.ClientSession() as session:
        valid_urls = await check_urls(session, unique_urls, semaphore)
        tasks = [asyncio.create_task(fetch_json(session, url, semaphore)) for url in valid_urls]
        results = await asyncio.gather(*tasks)
        return [channel for sublist in results for channel in sublist]


# zhgxtv模式获取频道
def get_channels_hgxtv(csv_file):
    urls = set()
    with open(csv_file, 'r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            host = row['host'].strip()
            if host:
                url = host if host.startswith(('http://', 'https://')) else f"http://{host}{':80' if ':' not in host else ''}"
                urls.add(url)

    ip_range_urls = []
    for url in urls:
        ip_start = url.find("//") + 2
        ip_end = url.find(":", ip_start)
        base_url = url[:ip_start]
        ip_address = url[ip_start:ip_end]
        port = url[ip_end:]
        ip_range_urls.extend(generate_ip_range_urls(base_url, ip_address, port, "/zhgxtv/Public/json/live_interface.txt"))

    valid_urls = check_urls_concurrent(set(ip_range_urls))
    channels = []
    for url in valid_urls:
        try:
            json_data = requests.get(url, timeout=1).content.decode('utf-8')
            for line in json_data.split('\n'):
                line = line.strip()
                if line:
                    name, channel_url = line.split(',')
                    urls_parts = channel_url.split('/', 3)
                    url_data_parts = url.split('/', 3)
                    urld = f"{urls_parts[0]}//{url_data_parts[2]}/{urls_parts[3]}" if len(urls_parts) >= 4 else f"{urls_parts[0]}//{url_data_parts[2]}"
                    channels.append((channel_name_normalize(name), urld))
        except:
            continue
    return channels


# 测试频道速度并输出结果
def test_speed_and_output(channels, output_prefix="itvlist"):
    task_queue = Queue()
    speed_results = []
    error_channels = []

    def worker():
        while True:
            channel_name, channel_url = task_queue.get()
            try:
                channel_url_t = channel_url.rstrip(channel_url.split('/')[-1])
                lines = requests.get(channel_url, timeout=1).text.strip().split('\n')
                ts_lists = [line for line in lines if not line.startswith('#')]
                if not ts_lists:
                    raise Exception("No valid TS files found.")
                ts_url = channel_url_t + ts_lists[0].split('/')[-1]
                start_time = os.times()[0]
                content = requests.get(ts_url, timeout=5).content
                end_time = os.times()[0]
                response_time = end_time - start_time
                if response_time <= 0:
                    response_time = 0.001
                if content:
                    file_size = len(content)
                    download_speed = file_size / response_time / 1024
                    normalized_speed = min(max(download_speed / 1024, 0.001), 100)
                    speed_results.append((channel_name, channel_url, f"{normalized_speed:.3f} MB/s"))
            except:
                error_channels.append((channel_name, channel_url))
            finally:
                total = len(channels)
                progress = (len(speed_results) + len(error_channels)) / total * 100 if total else 100
                print(f"可用频道：{len(speed_results)} 个 , 不可用频道：{len(error_channels)} 个 , 总频道：{total} 个 ,总进度：{progress:.2f} %。")
                task_queue.task_done()

    num_threads = 50
    for _ in range(num_threads):
        threading.Thread(target=worker, daemon=True).start()

    for channel in channels:
        task_queue.put(channel)
    task_queue.join()

    # ========== 相同频道只保留质量最好的一个 ==========
    best_by_channel = {}
    for channel_name, channel_url, speed_str in speed_results:
        try:
            speed_val = float(speed_str.split()[0])
        except Exception:
            speed_val = 0.0

        old = best_by_channel.get(channel_name)
        if old is None or speed_val > old[3]:
            best_by_channel[channel_name] = (channel_name, channel_url, speed_str, speed_val)

    unique_channels = [
        (name, url, speed)
        for name, (_, url, speed, _) in best_by_channel.items()
    ]

    # ========== 按参考 py 的规则分类 ==========
    group_to_channels = defaultdict(list)
    for item in unique_channels:
        group = guess_group(item[0])
        group_to_channels[group].append(item)

    # 组内自然排序
    for group in group_to_channels:
        group_to_channels[group].sort(key=lambda x: natural_sort_key(x[0]))

    ordered_groups = []
    for group_name in GROUP_OUTPUT_ORDER:
        if group_name in group_to_channels:
            ordered_groups.append((group_name, group_to_channels[group_name]))

    for group_name, chs in group_to_channels.items():
        if group_name not in GROUP_OUTPUT_ORDER:
            ordered_groups.append((group_name, chs))

    # 写 TXT
    with open(f"{output_prefix}.txt", 'w', encoding='utf-8') as txt_file:
        for group_name, chs in ordered_groups:
            txt_file.write(f"{group_name},#genre#\n")
            for channel_name, channel_url, _ in chs:
                txt_file.write(f"{channel_name},{channel_url}\n")

    # 写 M3U
    with open(f"{output_prefix}.m3u", 'w', encoding='utf-8') as m3u_file:
        m3u_file.write('#EXTM3U\n')
        for group_name, chs in ordered_groups:
            for channel_name, channel_url, _ in chs:
                m3u_file.write(f'#EXTINF:-1 group-title="{group_name}",{channel_name}\n')
                m3u_file.write(f'{channel_url}\n')

    # 写 speed.txt
    with open("speed.txt", 'w', encoding='utf-8') as speed_file:
        for group_name, chs in ordered_groups:
            for channel_name, channel_url, speed in chs:
                speed_file.write(f"{channel_name},{channel_url},{speed}\n")


# 主入口函数
def main():
    parser = argparse.ArgumentParser(description='多模式IPTV频道批量探测与测速')
    parser.add_argument('--jsmpeg', help='jsmpeg-streamer模式csv文件')
    parser.add_argument('--txiptv', help='txiptv模式csv文件')
    parser.add_argument('--zhgxtv', help='zhgxtv模式csv文件')
    parser.add_argument('--output', default='itvlist', help='输出文件前缀')
    args = parser.parse_args()

    channels = []
    if args.jsmpeg:
        channels.extend(get_channels_alltv(args.jsmpeg))
    if args.zhgxtv:
        channels.extend(get_channels_hgxtv(args.zhgxtv))
    if args.txiptv:
        channels.extend(asyncio.run(get_channels_newnew(args.txiptv)))

    if not channels:
        print('请至少指定一个csv文件')
        return

    test_speed_and_output(channels, args.output)


if __name__ == "__main__":
    main()
