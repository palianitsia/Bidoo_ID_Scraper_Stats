# Nota. Io utilizzo webshare per scraping massivo, tu puoi usare uno tuo basta che modifichi la configurazione. Per estrarre i dati come inizio, fine, id del prodotto, titolo del prodotto, id del vincitore, costo. 
# Utilizzo endp get_auction_details che a volte ritorna i dati df invece del asta, usa cookie dess  per gestire questo caso. 
# Script e utile per creare un db delle aste chiuse, come in questo caso lo uso per le statistiche in una   estensione.
# 


import aiohttp
import asyncio
import json
import re
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import signal
import sys
from fake_useragent import UserAgent
from aiohttp_socks import ProxyConnector
import random
import html

DESS_COOKIE = ""

START_ID = 97100000
END_ID = 97200000 
BASE_URL = "https://it.bidoo.com"
OUTPUT_FILE = "aste_bidoo.txt"
MAX_CONCURRENT = 50
MAX_RETRIES = 10
RATE_LIMIT_DELAY = 0.05
BATCH_SIZE = 50

WEBSHARE_API_URL = ""

SKIP_TITLES = {
    "Puntate",
    "Pacco",
    "Kit",
    "Set",
    "Lotto", 
    "Bundle",
    "Omaggio",
    "Campione",
    "Prova",
}

WEBSHARE_PROXIES = []
ua = UserAgent()
stats_lock = None
risultati_lock = None
semaphore = None
running = True
stats = {
    "successi": 0,
    "fallimenti": 0,
    "skippati_titolo": 0,
    "skippati_senza_vincitore": 0,
    "totali": 0,
    "processed": 0,
    "motivi_skip_titolo": {},
    "proxy_utilizzati": {},
}
risultati = []


def safe_log(level: str, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def clean_title(title: str) -> str:
    if not title:
        return "N/D"
    title = html.unescape(title)
    title = re.sub(r'<[^>]+>', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip() if title else "N/D"

def should_skip_product(title: str) -> Tuple[bool, str]:
    if not title or title == "N/D":
        return False, ""
    title_lower = clean_title(title).lower()
    for skip_word in SKIP_TITLES:
        if skip_word.lower() in title_lower:
            return True, skip_word
    return False, ""

def has_valid_winner(auction_data: Dict[str, Any]) -> bool:
    winner = auction_data.get("winner")
    if winner is None or winner == 0:
        return False
    if isinstance(winner, int) and winner > 0:
        return True
    if isinstance(winner, str) and winner.strip() and winner != "0":
        return True
    return False

async def fetch_webshare_proxies() -> bool:
    global WEBSHARE_PROXIES
    safe_log("info", f"🔄 Fetching proxy da Webshare API...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(WEBSHARE_API_URL, timeout=30) as response:
                if response.status == 200:
                    text = await response.text()
                    proxies = []
                    for line in text.split('\n'):
                        line = line.strip()
                        if line:
                            parts = line.split(':')
                            if len(parts) >= 4:
                                ip = parts[0]
                                porta = parts[1]
                                username = parts[2]
                                password = parts[3]
                                proxy = f"http://{username}:{password}@{ip}:{porta}"
                                proxies.append(proxy)
                    WEBSHARE_PROXIES = proxies
                    safe_log("info", f"✅ Caricati {len(WEBSHARE_PROXIES)} proxy da Webshare API")
                    return True
                else:
                    safe_log("error", f"❌ Errore nel fetch dei proxy: {response.status}")
                    return False
    except Exception as e:
        safe_log("error", f"❌ Errore durante il fetch dei proxy: {e}")
        return False

def get_random_proxy() -> Optional[str]:
    if WEBSHARE_PROXIES:
        proxy = random.choice(WEBSHARE_PROXIES)
        global stats
        if proxy not in stats["proxy_utilizzati"]:
            stats["proxy_utilizzati"][proxy] = 0
        stats["proxy_utilizzati"][proxy] += 1
        return proxy
    return None

def get_random_headers(dess: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "DNT": "1",
        "X-Requested-With": "XMLHttpRequest",
    }
    if dess:
        headers["Cookie"] = f"dess={dess}"
    return headers

def create_proxy_connector(proxy_url: Optional[str] = None) -> Optional[ProxyConnector]:
    if proxy_url:
        try:
            return ProxyConnector.from_url(proxy_url)
        except Exception:
            return None
    return None

async def get_auction_details(session: aiohttp.ClientSession, auction_id: int) -> Optional[Dict[str, Any]]:
    url = f"{BASE_URL}/get_auction_details.php"
    data = {"id": str(auction_id)}
    
    try:
        async with session.post(url, data=data, timeout=30) as response:
            if response.status != 200:
                return None
            json_data = await response.json()
            
            if json_data.get("status") == "200":
                auction = json_data.get("auction", [])
                if isinstance(auction, list) and len(auction) == 0:
                    return None
                # Se auction è un dizionario con id, è valida
                if isinstance(auction, dict) and auction.get("id"):
                    return json_data
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
        return None


async def get_bid_stats(session: aiohttp.ClientSession, auction_id: int) -> Optional[Dict[str, Any]]:
    url = f"{BASE_URL}/data.php?ALL={auction_id}&LISTID=0"
    
    try:
        async with session.get(url, timeout=30) as response:
            if response.status != 200:
                return None
            
            text = await response.text()
            text = text.strip()
            
            if re.match(r'^\d+\*?\d*$', text):
                return None
            
            match = re.search(r'\[([^\]]+)\]', text)
            if not match:
                return None
            
            data_string = match[1]
            parts = data_string.split(',')
            
            for item in parts:
                fields = item.split(';')
                if fields[0] == str(auction_id) and len(fields) >= 8:

                    bid_stats_str = fields[7] if len(fields) > 7 else ''
                    bid_stats_str = bid_stats_str.replace('^', '').strip('|')
                    bid_parts = bid_stats_str.split('|') if bid_stats_str else []
                    
                    paid_bids = 0
                    free_bids = 0
                    
                    if len(bid_parts) >= 2:
                        paid_bids = int(bid_parts[0]) if bid_parts[0].isdigit() else 0
                        free_bids = int(bid_parts[1]) if bid_parts[1].isdigit() else 0
                    
                    total_bids = paid_bids + free_bids
                    
                    vinc_auct = fields[4] if len(fields) > 4 and fields[4] else ''
                    
                    price_auct = fields[3] if len(fields) > 3 and fields[3] else ''
                    
                    durata = fields[6] if len(fields) > 6 and fields[6] else ''
                    
                    return {
                        "paid_bids": paid_bids,
                        "free_bids": free_bids,
                        "total_bids": total_bids,
                        "vinc_auct": vinc_auct,
                        "price_auct": price_auct,
                        "durata": durata
                    }
            
            return None
            
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
        return None


async def parse_auction_data(session: aiohttp.ClientSession, auction_id: int) -> Optional[Tuple[str, str, str]]:
    details = await get_auction_details(session, auction_id)
    if not details:
        return None
    
    auction = details.get("auction", {})
    if not auction or not isinstance(auction, dict):
        return None
    
    if not has_valid_winner(auction):
        return None, "", "senza_vincitore"
    
    product = auction.get("product", {})
    
    raw_title = product.get("title", "N/D")
    title = clean_title(raw_title)
    
    skip, motivo = should_skip_product(title)
    if skip:
        return None, motivo, ""
    
    start = auction.get("start", "N/D")
    expire = auction.get("expire", "N/D")
    winner = auction.get("winner", "N/D")
    product_id = product.get("id", "N/D")
    cost = product.get("cost_fmt", "N/D")
    
    start_str = datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S") if isinstance(start, int) and start > 0 else "N/D"
    expire_str = datetime.fromtimestamp(expire).strftime("%Y-%m-%d %H:%M:%S") if isinstance(expire, int) and expire > 0 else "N/D"
    
    bid_stats = await get_bid_stats(session, auction_id)
    
    vinc_auct = "N/D"
    price_auct = "N/D"
    durata = "N/D"
    paid_bids = 0
    free_bids = 0
    total_bids = 0
    
    if bid_stats:
        vinc_auct = bid_stats.get("vinc_auct", "N/D")
        price_auct = bid_stats.get("price_auct", "N/D")
        durata = bid_stats.get("durata", "N/D")
        paid_bids = bid_stats.get("paid_bids", 0)
        free_bids = bid_stats.get("free_bids", 0)
        total_bids = bid_stats.get("total_bids", 0)
    
    if vinc_auct == "N/D" and winner != "N/D":
        vinc_auct = str(winner)

    if durata == "N/D" and isinstance(start, int) and isinstance(expire, int) and start > 0 and expire > 0:
        durata_secondi = expire - start
        minuti = durata_secondi // 60
        secondi = durata_secondi % 60
        ore = minuti // 60
        minuti = minuti % 60
        if ore > 0:
            durata = f"{ore}h {minuti}m {secondi}s"
        else:
            durata = f"{minuti}m {secondi}s"

    if price_auct != "N/D" and price_auct.isdigit():
        prezzo_euro = int(price_auct) / 100
        price_auct = f"{prezzo_euro:.2f}"
    
    # Formato: id_asta|title|start|expire|winner|id|cost|vinc_auct|price_auct|durata|paid_bids|free_bids|total_bids studiato per bidoo search product by palianitsia da chrome brows
    output_line = f"{auction_id}|{title}|{start_str}|{expire_str}|{winner}|{product_id}|{cost}|{vinc_auct}|{price_auct}|{durata}|{paid_bids}|{free_bids}|{total_bids}"
    
    return output_line, "", ""


async def process_auction(session: aiohttp.ClientSession, auction_id: int) -> Optional[str]:
    global stats
    
    for tentativo in range(MAX_RETRIES):
        try:
            result = await parse_auction_data(session, auction_id)
            
            if result is None:
                if tentativo < MAX_RETRIES - 1:
                    await asyncio.sleep(1 * (tentativo + 1))
                else:
                    async with stats_lock:
                        stats["fallimenti"] += 1
                    return None
            
            output_line, motivo_skip_titolo, motivo_skip_vincitore = result
            
            if output_line is None and motivo_skip_titolo:
                async with stats_lock:
                    stats["skippati_titolo"] += 1
                    stats["motivi_skip_titolo"][motivo_skip_titolo] = stats["motivi_skip_titolo"].get(motivo_skip_titolo, 0) + 1
                return None
            
            if output_line is None and motivo_skip_vincitore:
                async with stats_lock:
                    stats["skippati_senza_vincitore"] += 1
                return None
            
            if output_line:
                async with stats_lock:
                    stats["successi"] += 1
                return output_line
            
        except Exception as e:
            if tentativo < MAX_RETRIES - 1:
                await asyncio.sleep(2 * (tentativo + 1))
            else:
                async with stats_lock:
                    stats["fallimenti"] += 1
                return None
    
    return None


async def worker(session: aiohttp.ClientSession, auction_id: int):
    global semaphore, stats, risultati
    
    async with semaphore:
        await asyncio.sleep(RATE_LIMIT_DELAY)
        result = await process_auction(session, auction_id)
        
        if result:
            async with risultati_lock:
                risultati.append(result)
        
        async with stats_lock:
            stats["processed"] += 1
            if stats["processed"] % 10 == 0:
                print(f"📊 Progresso: {stats['processed']}/{stats['totali']} - "
                      f"✅ Successi: {stats['successi']}, "
                      f"⏭️ Skip titolo: {stats['skippati_titolo']}, "
                      f"⏭️ Skip no winner: {stats['skippati_senza_vincitore']}, "
                      f"❌ Fallimenti: {stats['fallimenti']}")
        
        if len(risultati) % BATCH_SIZE == 0 and len(risultati) > 0:
            await save_results_async()


async def save_results_async():
    global risultati, risultati_lock
    async with risultati_lock:
        try:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                for line in risultati:
                    f.write(line + "\n")
        except Exception as e:
            print(f"⚠️ Errore durante il salvataggio: {e}")


async def process_with_session(auction_id: int, connector: Optional[ProxyConnector], headers: Dict[str, str]):
    timeout = aiohttp.ClientTimeout(total=45, connect=15)
    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers,
        timeout=timeout
    ) as session:
        await worker(session, auction_id)


def signal_handler(signum, frame):
    global running
    print("\n\n⚠️ Interruzione rilevata, salvataggio dati in corso...")
    running = False


async def main():
    global semaphore, stats, risultati, running, WEBSHARE_PROXIES
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 80)
    print("🐱 BOT PARSING ASTE BIDOO (ASYNC)")
    print("=" * 80)
    print(f"📋 Cookie DESS: {DESS_COOKIE}")
    print(f"📋 Range ID: {START_ID} - {END_ID}")
    print(f"📋 Output file: {OUTPUT_FILE}")
    print(f"📋 Concorrenza: {MAX_CONCURRENT} richieste parallele")
    print(f"📋 Skip titoli: {', '.join(SKIP_TITLES)}")
    print(f"📋 Skip aste senza vincitore: ATTIVO")
    print("=" * 80)
    print()
    
    proxy_success = await fetch_webshare_proxies()
    if not proxy_success or not WEBSHARE_PROXIES:
        safe_log("warning", "⚠️ Nessun proxy disponibile, continuo senza proxy...")
    else:
        safe_log("info", f"🚀 Utilizzo {len(WEBSHARE_PROXIES)} proxy per le richieste")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    global stats_lock, risultati_lock
    stats_lock = asyncio.Lock()
    risultati_lock = asyncio.Lock()
    
    totali = END_ID - START_ID + 1
    stats["totali"] = totali
    stats["processed"] = 0
    
    tasks = []
    safe_log("info", f"🚀 Avvio {totali} task in parallelo...\n")
    
    for auction_id in range(START_ID, END_ID + 1):
        if not running:
            break
        
        proxy_url = get_random_proxy() if WEBSHARE_PROXIES else None
        connector = create_proxy_connector(proxy_url) if proxy_url else None
        headers = get_random_headers(DESS_COOKIE)
        
        task = asyncio.create_task(process_with_session(auction_id, connector, headers))
        tasks.append(task)
    
    for future in asyncio.as_completed(tasks):
        if not running:
            for t in tasks:
                if not t.done():
                    t.cancel()
            break
        try:
            await future
        except asyncio.CancelledError:
            pass
        except Exception as e:
            safe_log("error", f"⚠️ Errore in un task: {e}")
    
    safe_log("info", "\n💾 Salvataggio finale...")
    await save_results_async()
    
    print()
    print("=" * 80)
    print("📊 RIEPILOGO FINALE")
    print("=" * 80)
    print(f"📋 ID processati: {stats['totali']}")
    print(f"✅ Successi: {stats['successi']}")
    print(f"⏭️ Skippati per titolo: {stats['skippati_titolo']}")
    if stats['motivi_skip_titolo']:
        print("   📌 Motivi skip titolo:")
        for motivo, count in sorted(stats['motivi_skip_titolo'].items(), key=lambda x: -x[1]):
            print(f"      - '{motivo}': {count} prodotti")
    print(f"⏭️ Skippati senza vincitore: {stats['skippati_senza_vincitore']}")
    print(f"❌ Fallimenti: {stats['fallimenti']}")
    print(f"📁 File salvato: {OUTPUT_FILE}")
    print(f"📊 Record salvati: {len(risultati)}")
    
    if WEBSHARE_PROXIES and stats["proxy_utilizzati"]:
        print(f"\n📊 STATISTICHE PROXY:")
        print(f"   Totale proxy disponibili: {len(WEBSHARE_PROXIES)}")
        print(f"   Proxy utilizzati: {len(stats['proxy_utilizzati'])}")
        sorted_proxies = sorted(stats["proxy_utilizzati"].items(), key=lambda x: -x[1])
        for i, (proxy, count) in enumerate(sorted_proxies[:5]):
            proxy_display = re.sub(r':[^:]+@', ':***@', proxy)
            print(f"      {i+1}. {proxy_display} - {count} richieste")
        if len(sorted_proxies) > 5:
            print(f"      ... e altri {len(sorted_proxies) - 5} proxy")
    
    print("=" * 80)


if __name__ == "__main__":
    try:
        import aiohttp_socks
    except ImportError:
        print("❌ Errore: aiohttp_socks non installato. Esegui:")
        print("   pip install aiohttp_socks")
        sys.exit(1)
    
    try:
        from fake_useragent import UserAgent
    except ImportError:
        print("❌ Errore: fake_useragent non installato. Esegui:")
        print("   pip install fake-useragent")
        sys.exit(1)
    
    asyncio.run(main())
