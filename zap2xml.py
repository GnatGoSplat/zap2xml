#!/usr/bin/env python3

import argparse
import os
import sys
import re
import json
import gzip
import time
from datetime import datetime, timezone
import urllib.parse
import urllib.request
import requests
import shutil
import urllib3
from functools import cmp_to_key
from pathlib import Path
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import traceback

VERSION = "2025-05-31"
DEFAULT_DAYS = 7
DEFAULT_NCDAYS = 0
DEFAULT_NCSDAYS = 0
DEFAULT_NCMDAY = -1
DEFAULT_RETRIES = 3
DEFAULT_OUTFILE = 'xmltv.xml'
DEFAULT_CACHE_DIR = 'cache'
DEFAULT_LANG = 'en'
DEFAULT_SLEEPTIME = 0

class Zap2XML:
    # ... __init__ and other methods ...  
    def pout(self, msg):
        if not getattr(self, 'quiet', False):
            print(msg, end='')

    def perr(self, msg):
        print(msg, file=sys.stderr, end='')
        
    def hour_to_millis(self):
        now = datetime.now()
        hour = 0 if hasattr(self, 'start') and self.start != 0 else (now.hour // self.gridHours) * self.gridHours
        dt = datetime(now.year, now.month, now.day, hour, 0, 0)
        timestamp = dt.timestamp()
        
        # Apply timezone adjustment ONLY if -g is NOT set
        if not hasattr(self, 'g') or not self.g:
            timestamp -= self.tz_offset() * 3600
        
        return int(timestamp * 1000)

    def tz_offset(self, t=None):
        """Calculate timezone offset in hours (UTC-aware)."""
        t = t or time.time()
        local = datetime.fromtimestamp(t).astimezone()  # Local time with TZ
        utc = datetime.fromtimestamp(t, tz=timezone.utc)  # UTC time
        return (local - utc).total_seconds() / 3600
        
    def write_output_file(self):
        self.pout(f"Writing XML file: {self.outFile}\n")
        encoding = 'utf-8' if getattr(self, 'utf8', False) else 'iso-8859-1'

        try:
            with Path(self.outFile).open('w', encoding=encoding) as f:
                if getattr(self, 'outputXTVD', False):
                    self.print_header_xtvd(f, encoding)
                    self.print_stations_xtvd(f)
                    self.print_lineups_xtvd(f)
                    self.print_schedules_xtvd(f)
                    self.print_programs_xtvd(f)
                    self.print_genres_xtvd(f)
                    self.print_footer_xtvd(f)
                else:
                    self.print_header(f, encoding)
                    self.print_channels(f)

                    if getattr(self, 'includeXMLTV', None):
                        self.pout(f"Reading XML file: {self.includeXMLTV}")
                        self.inc_xml("<channel", "<programme", f)

                    self.print_programmes(f)

                    if getattr(self, 'includeXMLTV', None):
                        self.inc_xml("<programme", "</tv", f)

                    self.print_footer(f)
        except OSError as e:
            self.perr(f"Failed to write output file: {e}")
            raise

    def conv_time(self, t):
        dt_utc = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()
        return dt_local.strftime('%Y%m%d%H%M%S')

    def conv_time_xtvd(self, t):
        dt_utc = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()
        return dt_local.strftime('%Y-%m-%dT%H:%M:%SZ')

    def conv_oad(self, t):
        dt_utc = datetime.fromtimestamp(t // 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()
        return dt_local.strftime('%Y%m%d')

    def conv_oad_xtvd(self, t):
        dt_utc = datetime.fromtimestamp(t // 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone()
        return dt_local.strftime('%Y-%m-%d')
    
    def get_timezone_offset_str(self, t=None):
        """
        Returns the local time zone offset in ±HHMM format for the given time in milliseconds.
        If `t` is None, uses the current time.
        """
        if t is None:
            dt = datetime.now()
        else:
            dt = datetime.fromtimestamp(t // 1000)

        # Get offset as timedelta
        offset = dt.astimezone().utcoffset()
        if offset is None:
            return "+0000"  # fallback, shouldn't happen

        total_minutes = int(offset.total_seconds() // 60)
        sign = "+" if total_minutes >= 0 else "-"
        hours = abs(total_minutes) // 60
        minutes = abs(total_minutes) % 60
        return f"{sign}{hours:02d}{minutes:02d}"

    def __init__(self):
        # Initialize all instance variables
        self.start = 0
        self.days = DEFAULT_DAYS
        self.ncdays = DEFAULT_NCDAYS
        self.ncsdays = DEFAULT_NCSDAYS
        self.ncmday = DEFAULT_NCMDAY
        self.retries = DEFAULT_RETRIES
        self.outFile = DEFAULT_OUTFILE
        self.cacheDir = DEFAULT_CACHE_DIR
        self.lang = DEFAULT_LANG
        self.userEmail = ''
        self.password = ''
        self.proxy = None
        self.postalcode = None
        self.country = None
        self.lineupId = None
        self.device = None
        self.sleeptime = DEFAULT_SLEEPTIME
        self.allChan = False
        self.shiftMinutes = 0
        self.outputXTVD = False
        self.lineuptype = None
        self.lineupname = None
        self.lineuplocation = None
        self.zapToken = None
        self.zapPref = '-'
        self.zapFavorites = {}
        self.sidCache = {}
        self.sTBA = r"\bTBA\b|To Be Announced"
        self.tvgfavs = {}
        self.programs = {}
        self.stations = {}
        self.schedule = {}
        self.logos = {}
        self.coNum = 0
        self.tb = 0
        self.treq = 0
        self.tsocks = set()
        self.expired = 0
        self.ua = None
        self.tba = 0
        self.exp = 0
        self.XTVD_startTime = None
        self.XTVD_endTime = None
        self.gridHours = 3
        self.urlRoot = 'https://tvlistings.gracenote.com/'
        self.urlAssets = 'https://zap2it.tmsimg.com/assets/'
        self.tvgurlRoot = 'http://mobilelistings.tvguide.com/'
        self.tvgMapiRoot = 'http://mapi.tvguide.com/'
        self.tvgurl = 'https://www.tvguide.com/'
        self.tvgspritesurl = 'http://static.tvgcdn.net/sprites/'
        self.useTVGuide = False  # Default to gracenote.com unless -z is used
        self.zlineupId = None  # Add this with other attributes
        self.session = None  # Add this line with other attribute initializations
        self.zipcode = None
        
        # Determine home directory and config file path
        self.homeDir = os.path.expanduser('~')
        if not self.homeDir:
            self.homeDir = '.'
        self.confFile = os.path.join(self.homeDir, '.zap2xmlrc')

    def run(self):
        print(f"zap2xml ({VERSION})")
        print(f"Command line: {' '.join(sys.argv)}")

        self.parse_options()
        self.read_config_file()
        self.process_options()
        
        if not os.path.exists(self.cacheDir):
            os.makedirs(self.cacheDir)
        else:
            self.clean_old_cache_files()
            
        self.process_data()
        self.write_output_file()
        
        # Print completion stats
        ts = sum(len(sched) for sched in self.schedule.values())
        self.pout(f"Completed with {len(self.stations)} stations, {len(self.programs)} programs, {ts} scheduled\n")
        
        if hasattr(self, 'waitOnExit') and self.waitOnExit:
            input("Press ENTER to exit:")
        elif sys.platform == 'win32':
            time.sleep(3)

    def parse_options(self):
        parser = argparse.ArgumentParser(description="zap2xml EPG fetcher")
        
        # User/authentication options
        parser.add_argument("-u", "--user", dest="userEmail", help="username")
        parser.add_argument("-p", "--password", dest="password", help="password")
        
        # Time/cache options
        parser.add_argument("-d", "--days", type=int, dest="days", help=f"number of days (default={DEFAULT_DAYS})")
        parser.add_argument("-n", "--nocache-days-end", type=int, dest="ncdays", help=f"no-cache days from end (default={DEFAULT_NCDAYS})")
        parser.add_argument("-N", "--nocache-days-start", type=int, dest="ncsdays", help=f"no-cache days from start (default={DEFAULT_NCSDAYS})")
        parser.add_argument("-B", "--nocache-day", type=int, dest="ncmday", help=f"specific no-cache day (default={DEFAULT_NCMDAY})")
        parser.add_argument("-s", "--start", type=int, dest="start", help="start day offset (default=0)")
        parser.add_argument("-g", action="store_true", dest="g", help="disable timezone adjustment (use raw UTC)")

        # Output options
        parser.add_argument("-o", "--output", dest="outFile", help=f"output xml filename (default={DEFAULT_OUTFILE})")
        parser.add_argument("-c", "--cache", dest="cacheDir", help=f"cache directory (default={DEFAULT_CACHE_DIR})")
        parser.add_argument("-x", "--xtvd", action="store_true", dest="outputXTVD", help="output XTVD xml file format")
        parser.add_argument("-F", "--channel-names-first", action="store_true", dest="channelNamesFirst", help='output channel names first (rather than "number name")')
        parser.add_argument("-T", "--no-tba-cache", action="store_true", dest="noTbaCache", help=f"don't cache files containing programs with \"{self.sTBA}\" titles")
        
        # Behavior flags
        parser.add_argument("-a", "--all-channels", action="store_true", dest="allChan", help="output all channels")
        parser.add_argument("-b", "--retain-order", action="store_true", dest="retainOrder", help="retain website channel order")
        parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="quiet mode")
        
        # Network/technical options
        parser.add_argument("-m", "--time-offset", type=int, dest="shiftMinutes", help="offset program times by # minutes")
        parser.add_argument("-P", "--proxy", dest="proxy", help="http proxy (http://proxyhost:port)")
        parser.add_argument("-r", "--retries", type=int, dest="retries", help=f"connection retries (default={DEFAULT_RETRIES}, max 20)")
        parser.add_argument("-S", "--sleep", type=int, dest="sleeptime", help=f"sleep between requests (default={DEFAULT_SLEEPTIME})")
        
        # Content options
        parser.add_argument("-l", "--lang", dest="lang", help=f"language (default={DEFAULT_LANG})")
        parser.add_argument("-i", "--icons", dest="iconDir", help="icon directory")
        parser.add_argument("-D", "--details", action="store_true", dest="includeDetails", help="include details")
        parser.add_argument("-I", "--include-icons", action="store_true", dest="includeIcons", help="include icons (image URLs)")
        
        # Formatting options
        parser.add_argument("-e", "--encode-entities", action="store_true", dest="encodeEntities", help="hex encode entities")
        parser.add_argument("-E", "--encode", dest="encodeSelective", help='selectively encode standard XML entities ("amp apos quot lt gt")')
        parser.add_argument("-U", "--utf8", action="store_true", dest="utf8", help="UTF-8 encoding")
        
        # Special modes
        parser.add_argument("-z", "--tvguide", action="store_true", dest="useTVGuide", help="use tvguide.com instead of gracenote.com")
        parser.add_argument("-j", "--series-category", action="store_true", dest="seriesCategory", help="add 'series' category to non-movie programs")
        
        # Hidden/undocumented options (for backward compatibility)
        parser.add_argument("-8", "--opt8", action="store_true", dest="opt8", help=argparse.SUPPRESS)
        parser.add_argument("-9", "--opt9", action="store_true", dest="opt9", help=argparse.SUPPRESS)
        parser.add_argument("-R", "--R", action="store_true", dest="R", help=argparse.SUPPRESS)
        parser.add_argument("-W", "--W", action="store_true", dest="W", help=argparse.SUPPRESS)
            
        args = parser.parse_args()
        
        # Map options to instance variables
        for key, value in vars(args).items():
            if value is not None:
                setattr(self, key, value)
        
        # Show help if no options provided and no userEmail set
        if not any(vars(args).values()) and not hasattr(self, 'userEmail'):
            parser.print_help()
            sys.exit(1)
        
        return args  # Return parsed args if needed elsewhere

    def read_config_file(self):
        try:
            with open(self.confFile, 'r') as f:
                self.pout(f"Reading config file: {self.confFile}\n")
                for line in f:
                    line = line.split('#')[0].strip()  # Remove comments
                    if not line:
                        continue
                    
                    match = re.match(r'^\s*start\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.start = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*days\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.days = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*ncdays\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.ncdays = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*ncsdays\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.ncsdays = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*ncmday\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.ncmday = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*retries\s*=\s*(\d+)', line, re.I)
                    if match:
                        self.retries = int(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*user[\w\s]*=\s*(.+)', line, re.I)
                    if match:
                        self.userEmail = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*pass[\w\s]*=\s*(.+)', line, re.I)
                    if match:
                        self.password = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*cache\s*=\s*(.+)', line, re.I)
                    if match:
                        self.cacheDir = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*icon\s*=\s*(.+)', line, re.I)
                    if match:
                        self.iconDir = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*trailer\s*=\s*(.+)', line, re.I)
                    if match:
                        self.trailerDir = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*lang\s*=\s*(.+)', line, re.I)
                    if match:
                        self.lang = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*outfile\s*=\s*(.+)', line, re.I)
                    if match:
                        self.outFile = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*proxy\s*=\s*(.+)', line, re.I)
                    if match:
                        self.proxy = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*outformat\s*=\s*(.+)', line, re.I)
                    if match:
                        if re.search(r'xtvd', match.group(1), re.I):
                            self.outputXTVD = True
                        continue
                        
                    match = re.match(r'^\s*lineupid\s*=\s*(.+)', line, re.I)
                    if match:
                        self.lineupId = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*lineupname\s*=\s*(.+)', line, re.I)
                    if match:
                        self.lineupname = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*lineuptype\s*=\s*(.+)', line, re.I)
                    if match:
                        self.lineuptype = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*lineuplocation\s*=\s*(.+)', line, re.I)
                    if match:
                        self.lineuplocation = self.rtrim(match.group(1))
                        continue
                        
                    match = re.match(r'^\s*postalcode\s*=\s*(.+)', line, re.I)
                    if match:
                        self.postalcode = self.rtrim(match.group(1))
                        continue
                        
                    raise Exception(f"Odd line in config file \"{self.confFile}\".\n\t{line}")
        except IOError:
            pass
            
        if not self.userEmail and not self.zlineupId:
            self.help_message()

    def process_options(self):
        if self.retries > 20:
            self.retries = 20
            
        if self.outputXTVD:
            self.outFile = 'xtvd.xml'
            
        self.ncdays = self.days - self.ncdays  # Make relative to the end

    def clean_old_cache_files(self):
        for filename in os.listdir(self.cacheDir):
            if filename.endswith('.html') or filename.endswith('.js'):
                filepath = os.path.join(self.cacheDir, filename)
                atime = os.path.getatime(filepath)
                if atime + ((self.days + 2) * 86400) < time.time():
                    self.pout(f"Deleting old cached file: {filepath}\n")
                    try:
                        os.unlink(filepath)
                    except OSError as e:
                        self.perr(f"Failed to delete '{filepath}': {e}\n")

    def process_data(self):
        if self.useTVGuide:
            if not self.allChan:
                self.login()  # Get favorites
            if hasattr(self, 'iconDir') and self.iconDir:
                self.parse_tvg_icons()
                
            max_count = self.days * (24 // self.gridHours)
            offset = self.start * 3600 * 24 * 1000
            ms = self.hour_to_millis() + offset
            
            for count in range(max_count):
                curday = (count // (24 // self.gridHours)) + 1
                if count == 0:
                    self.XTVD_startTime = ms
                elif count == max_count - 1:
                    self.XTVD_endTime = ms + (self.gridHours * 3600000) - 1
                    
                fn = os.path.join(self.cacheDir, f"{ms}.js.gz")
                if (not os.path.exists(fn)) or (curday > self.ncdays) or (curday <= self.ncsdays) or (curday == self.ncmday):
                    if not self.zlineupId:
                        self.login()
                    duration = self.gridHours * 60
                    tvgstart = str(ms)[:-3]
                    rs = self.get_url(f"{self.tvgurlRoot}Listingsweb/ws/rest/schedules/{self.zlineupId}/start/{tvgstart}/duration/{duration}", True)
                    if not rs:
                        break
                    self.write_binary_file(fn, gzip.compress(rs.encode('utf-8')))
                
                self.pout(f"[{count+1}/{max_count}] Parsing: {fn}\n")
                self.parse_tvg_grid(fn)
                
                if hasattr(self, 'noTbaCache') and self.noTbaCache and self.tba:
                    self.pout(f"Deleting: {fn} (contains \"{self.sTBA}\")\n")
                    self.unlink_file(fn)
                if self.exp:
                    self.pout(f"Deleting: {fn} (expired)\n")
                    self.unlink_file(fn)
                    
                self.exp = 0
                self.tba = 0
                ms += self.gridHours * 3600 * 1000
        else:
            if not self.allChan:
                self.login()  # Get favorites
                
            max_count = self.days * (24 // self.gridHours)
            offset = self.start * 3600 * 24 * 1000
            ms = self.hour_to_millis() + offset
            
            for count in range(max_count):
                curday = (count // (24 // self.gridHours)) + 1
                if count == 0:
                    self.XTVD_startTime = ms
                elif count == max_count - 1:
                    self.XTVD_endTime = ms + (self.gridHours * 3600000) - 1
                    
                fn = os.path.join(self.cacheDir, f"{ms}.js.gz")
                if (not os.path.exists(fn)) or (curday > self.ncdays) or (curday <= self.ncsdays) or (curday == self.ncmday):
                    zstart = str(ms)[:-3]
                    params = f"?time={zstart}&timespan={self.gridHours}&pref={self.zapPref}&"
                    params += self.get_zap_g_params()
                    params += '&TMSID=&AffiliateID=gapzap&FromPage=TV%20Grid'
                    params += '&ActivityID=1&OVDID=&isOverride=true'
                    rs = self.get_url(f"{self.urlRoot}api/grid{params}", True)
                    if not rs:
                        break
                    self.write_binary_file(fn, gzip.compress(rs.encode('utf-8')))
                
                self.pout(f"[{count+1}/{max_count}] Parsing: {fn}\n")
                self.parse_json(fn)
                
                if hasattr(self, 'noTbaCache') and self.noTbaCache and self.tba:
                    self.pout(f"Deleting: {fn} (contains \"{self.sTBA}\")\n")
                    self.unlink_file(fn)
                if self.exp:
                    self.pout(f"Deleting: {fn} (expired)\n")
                    self.unlink_file(fn)
                    
                self.exp = 0
                self.tba = 0
                ms += self.gridHours * 3600 * 1000

    def login(self):
        if self.session is None:
            # Initialize session with SSL verification disabled
            self.session = requests.Session()
            self.session.verify = False
            self.session.headers.update({
                'User-Agent': 'Mozilla/4.0', 
                'Accept-Encoding': 'gzip'
            })
            if self.proxy:
                self.session.proxies = {
                    'http': self.proxy,
                    'https': self.proxy
                }
        
        # login logic
        if (not self.userEmail or not self.password) and not hasattr(self, 'zlineupId'):
            raise Exception("Unable to login: Unspecified username or password")

        if self.userEmail and self.password:
            self.pout(f"Logging in as \"{self.userEmail}\" ({time.strftime('%c')})\n")
            try:
                if hasattr(self, 'useTVGuide') and self.useTVGuide:
                    return self.login_tvg()
                return self.login_zap()
            except Exception as e:
                self.perr(f"Login failed: {str(e)}\n")
                raise

    def login_tvg(self):
        rc = 0
        while rc < self.retries:
            rc += 1
            try:
                # Get login token
                r = self.session.get(f"{self.tvgurl}signin/")
                r.raise_for_status()
                
                match = re.search(r'<input.+name="_token".+?value="(.*?)"', r.text, re.I)
                if not match:
                    raise Exception("Login token not found")
                
                token = match.group(1)
                
                # Attempt login
                r = self.session.post(
                    f"{self.tvgurl}user/attempt/",
                    data={'_token': token, 'email': self.userEmail, 'password': self.password},
                    headers={'X-Requested-With': 'XMLHttpRequest'}
                )
                r.raise_for_status()
                
                if 'success' in r.text:
                    # Extract lineup ID from cookies if not already set
                    if not hasattr(self, 'zlineupId') or not self.zlineupId:
                        for cookie in self.session.cookies:
                            if cookie.name == "ServiceID":
                                self.zlineupId = cookie.value
                                self.pout(f"Discovered lineup ID: {self.zlineupId}\n")
                                break
                    
                    if not hasattr(self, 'zlineupId') or not self.zlineupId:
                        raise Exception("Could not determine lineup ID")
                    
                    if not hasattr(self, 'allChan') or not self.allChan:
                        r = self.session.get(
                            f"{self.tvgurl}user/favorites/?provider={self.zlineupId}",
                            headers={'X-Requested-With': 'XMLHttpRequest'}
                        )
                        r.raise_for_status()
                        if '{"code":200' in r.text:
                            self.parse_tvg_favs(r.text)
                    return r.text
                else:
                    self.pout(f"[Attempt {rc}] {r.status_code}: {r.text}\n")
                    
            except requests.exceptions.RequestException as e:
                self.perr(f"[Attempt {rc}] Error: {str(e)}\n")
            
            time.sleep(self.sleeptime + 1)
        
        raise Exception(f"Failed to login within {self.retries} retries.")

    def login_zap(self):
        rc = 0
        while rc < self.retries:
            rc += 1
            try:
                r = self.session.post(
                    f"{self.urlRoot}api/user/login",
                    data={
                        'emailid': self.userEmail,
                        'password': self.password,
                        'usertype': '0',
                        'facebookuser': 'false'
                    }
                )
                r.raise_for_status()
                
                t = r.json()
                self.zapToken = t.get('token')
                
                # Process preferences
                self.zapPref = ''
                if t.get('isMusic'):
                    self.zapPref += 'm'
                if t.get('isPPV'):
                    self.zapPref += 'p'
                if t.get('isHD'):
                    self.zapPref += 'h'
                self.zapPref = self.zapPref or '-'
                
                # Get properties
                prs = t.get('properties', {})
                self.postalcode = prs.get('2002')
                self.country = prs.get('2003')
                lineup = prs.get('2004', '').split(':')
                if len(lineup) >= 2:
                    self.lineupId, self.device = lineup[0], lineup[1]
                elif lineup:
                    self.lineupId = lineup[0]
                    self.device = '-'
                
                if not hasattr(self, 'allChan') or not self.allChan:
                    r = self.session.post(
                        f"{self.urlRoot}api/user/favorites",
                        data={'token': self.zapToken},
                        headers={'X-Requested-With': 'XMLHttpRequest'}
                    )
                    r.raise_for_status()
                    self.parse_z_favs(r.text)
                    
                return r.text
                
            except requests.exceptions.RequestException as e:
                self.pout(f"[Attempt {rc}] Error: {str(e)}\n")
                time.sleep(self.sleeptime + 1)
        
        raise Exception(f"Failed to login within {self.retries} retries.")

    def get_zap_g_params(self):
        params = self.get_zap_params()
        params['country'] = params.pop('countryCode', '')
        return '&'.join([f"{k}={v}" for k, v in params.items()])

    def get_zap_p_params(self):
        params = self.get_zap_params()
        params.pop('lineupId', None)
        return params

    def get_zap_params(self):
        params = {}
        if self.zlineupId or self.zipcode:
            self.postalcode = self.zipcode
            self.country = "USA"
            if self.zipcode and re.search(r'[A-Za-z]', self.zipcode):
                self.country = "CAN"
                
            if self.zlineupId and ':' in self.zlineupId:
                self.lineupId, self.device = self.zlineupId.split(':', 1)
            else:
                self.lineupId = self.zlineupId
                self.device = "-"
                
            params['postalCode'] = self.postalcode
        else:
            params['token'] = self.get_z_token()
            
        params['lineupId'] = f"{self.country}-{self.lineupId}-DEFAULT"
        params['postalCode'] = self.postalcode
        params['countryCode'] = self.country
        params['headendId'] = self.lineupId
        params['device'] = self.device
        params['aid'] = 'gapzap'
        return params

    def get_z_token(self):
        if not self.zapToken:
            self.login()
        return self.zapToken

    def parse_tvg_favs(self, buffer):
        try:
            t = json.loads(buffer)
            if 'message' in t:
                for f in t['message']:
                    source = f.get('source', '')
                    channel = f.get('channel', '')
                    self.tvgfavs[f"{channel}.{source}"] = 1
                self.pout(f"Lineup {self.zlineupId} favorites: {len(self.tvgfavs)}\n")
        except json.JSONDecodeError:
            pass

    def parse_z_favs(self, buffer):
        try:
            t = json.loads(buffer)
            if 'channels' in t:
                for f in t['channels']:
                    if hasattr(self, 'R') and self.R:
                        # Remove favorite
                        data = urllib.parse.urlencode({
                            'token': self.zapToken,
                            'prgsvcid': f,
                            'addToFav': 'false'
                        }).encode('utf-8')
                        
                        req = urllib.request.Request(
                            f"{self.urlRoot}api/user/ChannelAddtofav",
                            data=data,
                            headers={'X-Requested-With': 'XMLHttpRequest'}
                        )
                        r = self.ua.open(req)
                        if r.getcode() == 200:
                            self.pout(f"Removed favorite {f}\n")
                        else:
                            self.perr(f"RF{r.getcode()}: {r.read().decode('utf-8')}\n")
                    else:
                        self.zapFavorites[f] = 1
                        
                if hasattr(self, 'R') and self.R:
                    self.pout("Removed favorites, exiting\n")
                    sys.exit(0)
                    
                self.pout(f"Lineup favorites: {len(self.zapFavorites)}\n")
        except json.JSONDecodeError:
            pass

    def parse_tvg_icons(self):
        try:
            import requests
            from PIL import Image
        except ImportError:
            self.perr("Required modules (requests, PIL) not found for icon parsing\n")
            return
            
        css_url = f"{self.tvgspritesurl}{self.zlineupId}.css"
        r = requests.get(css_url)
        if r.status_code != 200:
            return
            
        css_content = r.text
        match = re.search(r'background-image:.+?url\((.+?)\)', css_content)
        if not match:
            return
            
        sprite_url = f"{self.tvgspritesurl}{match.group(1)}"
        
        if not os.path.exists(self.iconDir):
            os.makedirs(self.iconDir)
            
        filename = os.path.basename(sprite_url)
        sprite_path = os.path.join(self.iconDir, f"sprites-{filename}")
        
        # Download sprite image
        r = requests.get(sprite_url)
        if r.status_code != 200:
            return
            
        with open(sprite_path, 'wb') as f:
            f.write(r.content)
            
        try:
            im = Image.open(sprite_path)
            iconw, iconh = 30, 20
            
            for match in re.finditer(r'listings-channel-icon-(.+?)\{.+?position:.*?-(\d+).+?(\d+).*?\}', css_content, re.I):
                cid = match.group(1)
                iconx = int(match.group(2))
                icony = int(match.group(3))
                
                # Extract icon from sprite
                icon = im.crop((iconx, icony, iconx + iconw, icony + iconh))
                
                self.logos[cid] = {
                    'logo': f"sprite-{cid}",
                    'logoExt': os.path.splitext(filename)[1]
                }
                
                icon_path = os.path.join(self.iconDir, f"{self.logos[cid]['logo']}{self.logos[cid]['logoExt']}")
                icon.save(icon_path)
        except Exception as e:
            self.perr(f"Error processing icons: {e}\n")

    def parse_tvg_grid(self, filename):
        try:
            with gzip.open(filename, 'rb') as f:
                content = f.read().decode('utf-8')
                t = json.loads(content)
                
                for e in t:
                    cjs = e.get('Channel', {})
                    src = cjs.get('SourceId', '')
                    num = cjs.get('Number', '')
                    cs = f"{num}.{src}"
                    
                    # Skip if not in favorites
                    if self.tvgfavs and cs not in self.tvgfavs:
                        continue
                        
                    # Add station if not exists
                    if cs not in self.stations:
                        self.stations[cs] = {
                            'stnNum': src,
                            'number': num,
                            'name': cjs.get('Name', ''),
                            'order': self.coNum if hasattr(self, 'retainOrder') and self.retainOrder else num
                        }
                        self.coNum += 1
                        
                        fullname = cjs.get('FullName', '')
                        if fullname and fullname != cjs.get('Name', ''):
                            self.stations[cs]['fullname'] = fullname
                            
                    # Process program schedules
                    for pe in e.get('ProgramSchedules', []):
                        if not pe.get('ProgramId'):
                            continue
                            
                        cp = pe['ProgramId']
                        catid = pe.get('CatId', 0)
                        
                        # Set genre based on category
                        if catid == 1:
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['movie'] = 1
                        elif catid == 2:
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['sports'] = 1
                        elif catid == 3:
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['family'] = 1
                        elif catid == 4:
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['news'] = 1
                            
                        # Set series genre if needed
                        if pe.get('ParentProgramId', 0) != 0 or (hasattr(self, 'seriesCategory') and self.seriesCategory and catid != 1):
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['series'] = 99
                            
                        # Basic program info
                        self.programs.setdefault(cp, {})['title'] = pe.get('Title', '')
                        if re.search(self.sTBA, self.programs[cp]['title'], re.I):
                            self.tba = 1
                            
                        episode_title = pe.get('EpisodeTitle', '')
                        if episode_title:
                            self.programs[cp]['episode'] = episode_title
                            if re.search(self.sTBA, episode_title, re.I):
                                self.tba = 1
                                
                        description = pe.get('CopyText', '')
                        if description:
                            self.programs[cp]['description'] = description
                            
                        rating = pe.get('Rating', '')
                        if rating:
                            self.programs[cp]['rating'] = rating
                            
                        # Schedule info
                        sch = pe.get('StartTime', 0) * 1000
                        self.schedule.setdefault(cs, {})[sch] = {
                            'time': sch,
                            'endtime': pe.get('EndTime', 0) * 1000,
                            'program': cp,
                            'station': cs
                        }
                        
                        # Airing attributes
                        airat = pe.get('AiringAttrib', 0)
                        if airat & 1:
                            self.schedule[cs][sch]['live'] = 1
                        elif airat & 4:
                            self.schedule[cs][sch]['new'] = 1
                            
                        # TV object info
                        tvo = pe.get('TVObject', {})
                        if tvo:
                            season_num = tvo.get('SeasonNumber', 0)
                            if season_num != 0:
                                self.programs[cp]['seasonNum'] = season_num
                                episode_num = tvo.get('EpisodeNumber', 0)
                                if episode_num != 0:
                                    self.programs[cp]['episodeNum'] = episode_num
                                    
                            ead = tvo.get('EpisodeAirDate', '')
                            if ead:
                                ead = re.sub(r'[^0-9-]', '', ead)
                                if ead:
                                    self.programs[cp]['originalAirDate'] = ead
                                    
                            url = None
                            if tvo.get('EpisodeSEOUrl', ''):
                                url = tvo['EpisodeSEOUrl']
                            elif tvo.get('SEOUrl', ''):
                                url = tvo['SEOUrl']
                                if catid == 1 and 'movies' not in url:
                                    url = f"/movies{url}"
                                    
                            if url:
                                self.programs[cp]['url'] = f"{self.tvgurl[:-1]}{url}"
                                
                        # Get details if needed
                        if (hasattr(self, 'includeIcons') and self.includeIcons) or \
                           (hasattr(self, 'includeDetails') and self.includeDetails and self.programs[cp].get('genres', {}).get('movie')) or \
                           (hasattr(self, 'W') and self.W and self.programs[cp].get('genres', {}).get('movie')):
                            self.get_details(self.parse_tvg_details, cp, f"{self.tvgMapiRoot}listings/details?program={cp}", "")
        except Exception as e:
            self.perr(f"Error parsing TVG grid: {e}\n")

    def parse_tvg_details(self, filename):
        try:
            with gzip.open(filename, 'rb') as f:
                content = f.read().decode('utf-8')
                t = json.loads(content)
                
                prog = t.get('program', {})
                if 'release_year' in prog:
                    self.programs[self.cp]['movie_year'] = prog['release_year']
                    
                if 'rating' in prog and 'rating' not in self.programs[self.cp]:
                    if prog['rating'] != 'NR':
                        self.programs[self.cp]['rating'] = prog['rating']
                        
                tvo = t.get('tvobject', {})
                if 'photos' in tvo:
                    phash = {}
                    for ph in tvo['photos']:
                        w = ph.get('width', 0) * ph.get('height', 0)
                        u = ph.get('url', '')
                        if w and u:
                            phash[w] = u
                            
                    if phash:
                        biggest = max(phash.keys())
                        self.programs[self.cp]['imageUrl'] = phash[biggest]
        except Exception as e:
            self.perr(f"Error parsing TVG details: {e}\n")

    def parse_json(self, filename):
        try:
            with gzip.open(filename, 'rb') as f:
                content = f.read().decode('utf-8')
                t = json.loads(content)
                
                zapStarred = {}
                for s in t.get('channels', []):
                    channelId = s.get('channelId')
                    if not channelId:
                        continue
                        
                    # Skip if not in favorites
                    if not self.allChan and self.zapFavorites:
                        if channelId in self.zapFavorites:
                            if hasattr(self, 'opt8') and self.opt8:
                                if channelId in zapStarred:
                                    continue
                                zapStarred[channelId] = 1
                        else:
                            continue
                            
                    # Add station info
                    cs = f"{s.get('channelNo', '')}.{channelId}"
                    if cs not in self.stations:
                        self.stations[cs] = {
                            'stnNum': channelId,
                            'name': s.get('callSign', ''),
                            'number': s.get('channelNo', '').lstrip('0'),
                            'order': self.coNum if hasattr(self, 'retainOrder') and self.retainOrder else s.get('channelNo', '').lstrip('0')
                        }
                        self.coNum += 1
                        
                        # Handle station logo
                        thumbnail = s.get('thumbnail', '')
                        if thumbnail:
                            thumbnail = re.sub(r'\?.*', '', thumbnail)
                            if not thumbnail.startswith('http'):
                                thumbnail = f"https:{thumbnail}"
                            self.stations[cs]['logoURL'] = thumbnail
                            if hasattr(self, 'iconDir') and self.iconDir:
                                self.handle_logo(thumbnail)
                                
                    # Process events
                    for e in s.get('events', []):
                        program = e.get('program', {})
                        cp = program.get('id')
                        if not cp:
                            continue
                            
                        # Basic program info
                        self.programs.setdefault(cp, {})['title'] = program.get('title', '')
                        if re.search(self.sTBA, self.programs[cp]['title'], re.I):
                            self.tba = 1
                            
                        episode_title = program.get('episodeTitle', '')
                        if episode_title:
                            self.programs[cp]['episode'] = episode_title
                            
                        description = program.get('shortDesc', '')
                        if description:
                            self.programs[cp]['description'] = description
                            
                        duration = int(e.get('duration', 0))
                        if duration > 0:
                            self.programs[cp]['duration'] = duration
                            
                        release_year = program.get('releaseYear', '')
                        if release_year:
                            self.programs[cp]['movie_year'] = release_year
                            
                        season = program.get('season', '')
                        if season:
                            self.programs[cp]['seasonNum'] = season
                            
                        episode = program.get('episode', '')
                        if episode:
                            self.programs[cp]['episodeNum'] = episode
                            
                        # Program image
                        thumbnail = e.get('thumbnail', '')
                        if thumbnail:
                            self.programs[cp]['imageUrl'] = f"{self.urlAssets}{thumbnail}.jpg"
                            
                        # Program URL
                        series_id = program.get('seriesId', '')
                        tms_id = program.get('tmsId', '')
                        if series_id and tms_id:
                            self.programs[cp]['url'] = f"{self.urlRoot}overview-affiliates.html?programSeriesId={series_id}&tmsId={tms_id}"
                            
                        # Schedule info
                        start_time = self.str2time1(e.get('startTime', '')) * 1000
                        self.schedule.setdefault(cs, {})[start_time] = {
                            'time': start_time,
                            'endTime': self.str2time1(e.get('endTime', '')) * 1000,
                            'program': cp,
                            'station': cs
                        }
                        
                        # Genres
                        genres = e.get('filter', [])
                        if genres:
                            for i, g in enumerate(genres, 1):
                                g = re.sub(r'filter-', '', g, flags=re.I)
                                self.programs.setdefault(cp, {}).setdefault('genres', {})[g.lower()] = i
                                
                        # Rating
                        rating = e.get('rating', '')
                        if rating:
                            self.programs[cp]['rating'] = rating
                            
                        # Tags (like CC)
                        tags = e.get('tags', [])
                        if 'CC' in tags:
                            self.schedule[cs][start_time]['cc'] = 1
                            
                        # Flags (like New, Live)
                        flags = e.get('flag', [])
                        if 'New' in flags:
                            self.schedule[cs][start_time]['new'] = 'New'
                            self.set_original_air_date(cp, cs, start_time)
                        if 'Live' in flags:
                            self.schedule[cs][start_time]['live'] = 'Live'
                            self.set_original_air_date(cp, cs, start_time)
                        if 'Premiere' in flags:
                            self.schedule[cs][start_time]['premiere'] = 'Premiere'
                        if 'Finale' in flags:
                            self.schedule[cs][start_time]['finale'] = 'Finale'
                            
                        # Series category if needed
                        if hasattr(self, 'seriesCategory') and self.seriesCategory and not cp.startswith('MV'):
                            self.programs.setdefault(cp, {}).setdefault('genres', {})['series'] = 99
                            
                        # Get details if needed
                        if hasattr(self, 'includeDetails') and self.includeDetails and not program.get('isGeneric', True):
                            self.post_json_overview(cp, program.get('seriesId', ''))
        except Exception as e:
            self.perr(f"Error parsing JSON: {e}\n")

    def post_json_overview(self, cp, sid):
        fn = os.path.join(self.cacheDir, f"O{cp}.js.gz")
        
        # Try to use cached version if available
        if not os.path.exists(fn) and sid in self.sidCache and os.path.exists(self.sidCache[sid]):
            shutil.copyfile(self.sidCache[sid], fn)
            
        if not os.path.exists(fn):
            url = f"{self.urlRoot}api/program/overviewDetails"
            self.pout(f"[{self.treq}] Post {sid}: {url}\n")
            time.sleep(self.sleeptime)
            
            params = self.get_zap_p_params()
            params['programSeriesID'] = sid
            params['clickstream[FromPage]'] = 'TV%20Grid'
            
            data = urllib.parse.urlencode(params).encode('utf-8')
            req = urllib.request.Request(
                url,
                data=data,
                headers={'X-Requested-With': 'XMLHttpRequest'}
            )
            
            try:
                r = self.ua.open(req)
                if r.getcode() == 200:
                    content = r.read().decode('utf-8')
                    self.write_binary_file(fn, gzip.compress(content.encode('utf-8')))
                    self.sidCache[sid] = fn
                else:
                    self.perr(f"{cp} : {r.getcode()}\n")
            except Exception as e:
                self.perr(f"Error posting JSON overview: {e}\n")
                
        if os.path.exists(fn):
            self.pout(f"[D] Parsing: {cp}\n")
            try:
                with gzip.open(fn, 'rb') as f:
                    content = f.read().decode('utf-8')
                    t = json.loads(content)
                    
                    # Process series genres
                    series_genres = t.get('seriesGenres', '')
                    if series_genres:
                        gh = self.programs.get(cp, {}).get('genres', {})
                        max_val = max(gh.values()) if gh else 0
                        i = max_val + 1 if max_val else 2
                        
                        for sg in series_genres.split('|'):
                            sg = sg.lower()
                            if sg not in gh:
                                gh[sg] = i
                                i += 1
                                
                    # Process cast
                    i = 1
                    for c in t.get('overviewTab', {}).get('cast', []):
                        name = c.get('name', '')
                        if not name:
                            continue
                            
                        character = c.get('characterName', '')
                        role = c.get('role', '').lower()
                        
                        if role == 'host':
                            self.programs.setdefault(cp, {}).setdefault('presenter', {})[name] = i
                        else:
                            self.programs.setdefault(cp, {}).setdefault('actor', {})[name] = i
                            if character:
                                self.programs.setdefault(cp, {}).setdefault('role', {})[name] = character
                        i += 1
                        
                    # Process crew
                    i = 1
                    for c in t.get('overviewTab', {}).get('crew', []):
                        name = c.get('name', '')
                        if not name:
                            continue
                            
                        role = c.get('role', '').lower()
                        if 'producer' in role:
                            self.programs.setdefault(cp, {}).setdefault('producer', {})[name] = i
                        elif 'director' in role:
                            self.programs.setdefault(cp, {}).setdefault('director', {})[name] = i
                        elif 'writer' in role:
                            self.programs.setdefault(cp, {}).setdefault('writer', {})[name] = i
                        i += 1
                        
                    # Update image if not set
                    if 'imageUrl' not in self.programs.get(cp, {}) and t.get('seriesImage', ''):
                        self.programs[cp]['imageUrl'] = f"{self.urlAssets}{t['seriesImage']}.jpg"
                        
                    # Update description for movies and shows
                    if cp.startswith(('MV', 'SH')):
                        series_desc = t.get('seriesDescription', '')
                        if series_desc and len(series_desc) > len(self.programs.get(cp, {}).get('description', '')):
                            self.programs[cp]['description'] = series_desc
                            
                    # Original air date for episodes
                    if cp.startswith('EP'):
                        ue = t.get('overviewTab', {}).get('upcomingEpisode', {})
                        if (ue.get('tmsID', '').lower() == cp.lower() and 
                            ue.get('originalAirDate', '') not in ('', '1000-01-01T00:00Z')):
                            oad = self.str2time2(ue['originalAirDate']) * 1000
                            self.programs[cp]['originalAirDate'] = oad
                        else:
                            for ue in t.get('upcomingEpisodeTab', []):
                                if (ue.get('tmsID', '').lower() == cp.lower() and 
                                    ue.get('originalAirDate', '') not in ('', '1000-01-01T00:00Z')):
                                    oad = self.str2time2(ue['originalAirDate']) * 1000
                                    self.programs[cp]['originalAirDate'] = oad
                                    break
            except Exception as e:
                self.perr(f"Error parsing overview JSON: {e}\n")
        else:
            self.pout(f"Skipping: {sid}\n")

    def set_original_air_date(self, cp, cs, sch):
        if not cp.startswith(('EP', 'SH', 'MV')) or len(cp) < 10 or cp[10:14] == '0000':
            return
            
        if ('originalAirDate' not in self.programs.get(cp, {}) or 
            sch < self.programs[cp].get('originalAirDate', float('inf'))):
            self.programs.setdefault(cp, {})['originalAirDate'] = sch

    def str2time1(self, s):
        try:
            dt = datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
            dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except:
            return 0

    def str2time2(self, s):
        try:
            dt = datetime.strptime(s, '%Y-%m-%dT%H:%MZ')
            dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except:
            return 0

    def get_details(self, func, cp, url, prefix):
        fn = os.path.join(self.cacheDir, f"{prefix}{cp}.js.gz")
        if not os.path.exists(fn):
            rs = self.get_url(url, True)
            if rs:
                self.write_binary_file(fn, gzip.compress(rs.encode('utf-8')))
                
        if os.path.exists(fn):
            l = prefix if prefix else "D"
            self.pout(f"[{l}] Parsing: {cp}\n")
            self.cp = cp
            func(fn)
        else:
            self.pout(f"Skipping: {cp}\n")

    def get_url(self, url, er):
        if not hasattr(self, 'session') or self.session is None:
            self.login()  # Ensure session exists
            
        rc = 0
        while rc < self.retries:
            rc += 1
            self.pout(f"[{self.treq}] Getting: {url}\n")
            time.sleep(self.sleeptime)
            
            try:
                r = self.session.get(url)
                self.treq += 1
                content = r.text
                self.tb += len(content)
                
                if r.status_code == 200 and content:
                    return content
                elif r.status_code == 500 and "Could not load details" in content:
                    self.pout(f"{content}\n")
                    return ""
                else:
                    self.perr(f"[Attempt {rc}] {len(content)}:{r.status_code}\n")
                    self.perr(f"{content}\n")
                    time.sleep(self.sleeptime + 2)
            except Exception as e:
                self.perr(f"[Attempt {rc}] Error: {str(e)}\n")
                time.sleep(self.sleeptime + 2)
        
        self.perr(f"Failed to download within {self.retries} retries.\n")
        if er:
            self.perr("Server out of data? Temporary server error? Normal exit anyway.\n")
            return ""
        raise Exception("Failed to download URL")

    def ua_open(self, url, data=None, headers=None):
        req = urllib.request.Request(url, data=data)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        return self.ua.open(req)

    def ua_get(self, url, headers=None):
        return self.ua_open(url, headers=headers)

    def write_binary_file(self, filename, data):
        try:
            with open(filename, 'wb') as f:
                f.write(data)
        except IOError as e:
            raise Exception(f"Failed to write '{filename}': {e}")

    def unlink_file(self, filename):
        try:
            os.unlink(filename)
        except OSError as e:
            self.perr(f"Failed to delete '{filename}': {e}\n")

    def handle_logo(self, url):
        if not hasattr(self, 'iconDir') or not self.iconDir:
            return
            
        if not os.path.exists(self.iconDir):
            os.makedirs(self.iconDir)
            
        filename = os.path.basename(url)
        name, ext = os.path.splitext(filename)
        self.logos[self.cs] = {
            'logo': name,
            'logoExt': ext
        }
        
        filepath = os.path.join(self.iconDir, filename)
        if not os.path.exists(filepath):
            try:
                r = self.ua_open(url)
                with open(filepath, 'wb') as f:
                    f.write(r.read())
            except Exception as e:
                self.perr(f"Failed to download logo: {e}\n")

    def copy_logo(self, key):
        cid = key
        if cid not in self.logos:
            cid = key.split('.')[-1]
            
        if hasattr(self, 'iconDir') and cid in self.logos:
            num = self.stations[key]['number']
            src = os.path.join(self.iconDir, f"{self.logos[cid]['logo']}{self.logos[cid]['logoExt']}")
            dest1 = os.path.join(self.iconDir, f"{num}{self.logos[cid]['logoExt']}")
            dest2 = os.path.join(self.iconDir, f"{num} {self.stations[key]['name']}{self.logos[cid]['logoExt']}")
            dest3 = os.path.join(self.iconDir, f"{num}_{self.stations[key]['name']}{self.logos[cid]['logoExt']}")
            
            try:
                shutil.copyfile(src, dest1)
                shutil.copyfile(src, dest2)
                shutil.copyfile(src, dest3)
            except IOError as e:
                self.perr(f"Failed to copy logo: {e}\n")

    def sort_chan(self, a, b):
        a_order = self.stations[a].get('order')
        b_order = self.stations[b].get('order')
        
        if a_order is not None and b_order is not None:
            cmp = (a_order > b_order) - (a_order < b_order)
            if cmp == 0:
                a_num = self.stations[a].get('stnNum', '')
                b_num = self.stations[b].get('stnNum', '')
                return (a_num > b_num) - (a_num < b_num)
            return cmp
        else:
            a_name = self.stations[a].get('name', '')
            b_name = self.stations[b].get('name', '')
            return (a_name > b_name) - (a_name < b_name)

    def station_to_channel(self, s):
        if hasattr(self, 'useTVGuide') and self.useTVGuide:
            return f"I{self.stations[s]['number']}.{self.stations[s]['stnNum']}.tvguide.com"
        elif hasattr(self, 'oldStyleIds') and self.oldStyleIds:
            return f"C{self.stations[s]['number']}{self.stations[s]['name'].lower()}.gracenote.com"
        elif hasattr(self, 'opt9') and self.opt9:
            return f"I{self.stations[s]['stnNum']}.labs.gracenote.com"
        else:
            return f"I{self.stations[s]['number']}.{self.stations[s]['stnNum']}.gracenote.com"

    def enc(self, t):
        if t is None:
            return ''
        if not isinstance(t, str):
            t = str(t)
        t = t.strip()
            
        if not hasattr(self, 'utf8') or not self.utf8:
            try:
                t = t.encode('utf-8').decode('latin-1')
            except:
                pass
                
        if not hasattr(self, 'encodeSelective') or self.encodeSelective is None or 'amp' in self.encodeSelective:
            t = t.replace('&', '&amp;')
        if not hasattr(self, 'encodeSelective') or self.encodeSelective is None or 'quot' in self.encodeSelective:
            t = t.replace('"', '&quot;')
        if not hasattr(self, 'encodeSelective') or self.encodeSelective is None or 'apos' in self.encodeSelective:
            t = t.replace("'", '&apos;')
        if not hasattr(self, 'encodeSelective') or self.encodeSelective is None or 'lt' in self.encodeSelective:
            t = t.replace('<', '&lt;')
        if not hasattr(self, 'encodeSelective') or self.encodeSelective is None or 'gt' in self.encodeSelective:
            t = t.replace('>', '&gt;')
            
        if hasattr(self, 'encodeEntities') and self.encodeEntities:
            t = ''.join([f'&#{ord(c)};' if ord(c) > 127 else c for c in t])
            
        return t

    def append_asterisk(self, title, station, s):
        if hasattr(self, 'appendAsterisk') and self.appendAsterisk:
            if ('new' in self.appendAsterisk and 'new' in self.schedule[station][s]) or \
               ('live' in self.appendAsterisk and 'live' in self.schedule[station][s]):
                title += " *"
        return title

    def print_header(self, fh, enc):
        fh.write('<?xml version="1.0" encoding="{}"?>\n'.format(enc))
        fh.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n\n')
        
        if hasattr(self, 'useTVGuide') and self.useTVGuide:
            fh.write('<tv source-info-url="http://tvguide.com/" source-info-name="tvguide.com"')
        else:
            fh.write('<tv source-info-url="http://tvlistings.gracenote.com/" source-info-name="gracenote.com"')
            
        fh.write(' generator-info-name="zap2xml" generator-info-url="zap2xml@gmail.com">\n')

    def print_footer(self, fh):
        fh.write('</tv>\n')

    def print_channels(self, fh):
        for key in sorted(self.stations.keys(), key=cmp_to_key(self.sort_chan)):
            sname = self.enc(self.stations[key].get('name'))
            fname = self.enc(self.stations[key].get('fullname'))
            snum = self.stations[key].get('number')
            
            fh.write('\t<channel id="{}">\n'.format(self.station_to_channel(key)))
            
            if hasattr(self, 'channelNamesFirst') and self.channelNamesFirst and sname:
                fh.write('\t\t<display-name>{}</display-name>\n'.format(sname))
                
            if snum:
                self.copy_logo(key)
                if snum:
                    fh.write('\t\t<display-name>{} {}</display-name>\n'.format(snum, sname))
                    fh.write('\t\t<display-name>{}</display-name>\n'.format(snum))
                    
            if not hasattr(self, 'channelNamesFirst') or not self.channelNamesFirst:
                if sname:
                    fh.write('\t\t<display-name>{}</display-name>\n'.format(sname))
                    
            if fname:
                fh.write('\t\t<display-name>{}</display-name>\n'.format(fname))
                
            if 'logoURL' in self.stations[key]:
                fh.write('\t\t<icon src="{}" />\n'.format(self.stations[key]['logoURL']))
                
            fh.write('\t</channel>\n')

    def print_programmes(self, fh):
        for station in sorted(self.schedule.keys(), key=cmp_to_key(self.sort_chan)):
            i = 0
            # Keep original keys exactly as they are
            original_keys = list(self.schedule[station].keys())

            # Sort keys by the 'time' field of their associated dict
            key_array = sorted(original_keys, key=lambda k: self.schedule[station][k]['time'])

            while i < len(key_array):
                s = key_array[i]  # s is the original key (str or int or whatever)

                # Remove last key if no 'endtime'
                if i == len(key_array) - 1 and 'endtime' not in self.schedule[station][s]:
                    del self.schedule[station][s]
                    i += 1
                    continue

                p = self.schedule[station][s]['program']
                start_time = self.conv_time(self.schedule[station][s]['time'])
                start_tz = self.get_timezone_offset_str(self.schedule[station][s]['time'])

                if 'endtime' in self.schedule[station][s]:
                    end_time = self.schedule[station][s]['endtime']
                else:
                    end_time = self.schedule[station][key_array[i+1]]['time']

                stop_time = self.conv_time(end_time)
                stop_tz = self.get_timezone_offset_str(end_time)

                fh.write('\t<programme start="{} {}" stop="{} {}" channel="{}">\n'.format(
                    start_time, start_tz, stop_time, stop_tz, 
                    self.station_to_channel(self.schedule[station][s]['station'])))

                if 'title' in self.programs[p]:
                    title = self.enc(self.programs[p]['title'])
                    title = self.append_asterisk(title, station, s)
                    fh.write('\t\t<title lang="{}">{}</title>\n'.format(self.lang, title))

                if 'episode' in self.programs[p] or (hasattr(self, 'movieSubtitle') and self.movieSubtitle and 'movie_year' in self.programs[p]):
                    fh.write('\t\t<sub-title lang="{}">'.format(self.lang))
                    if 'episode' in self.programs[p]:
                        fh.write(self.enc(self.programs[p]['episode']))
                    elif 'movie_year' in self.programs[p]:
                        fh.write("Movie ({})".format(self.programs[p]['movie_year']))
                    fh.write('</sub-title>\n')

                if 'description' in self.programs[p]:
                    fh.write('\t\t<desc lang="{}">{}</desc>\n'.format(
                        self.lang, self.enc(self.programs[p]['description'])))

                if ('actor' in self.programs[p] or 'director' in self.programs[p] or 
                    'writer' in self.programs[p] or 'producer' in self.programs[p] or 
                    'presenter' in self.programs[p]):
                    fh.write('\t\t<credits>\n')
                    self.print_credits(fh, p, "director")

                    if 'actor' in self.programs[p]:
                        for g in sorted(self.programs[p]['actor'].keys(), 
                                    key=lambda x: self.programs[p]['actor'][x]):
                            fh.write('\t\t\t<actor')
                            if 'role' in self.programs[p] and g in self.programs[p]['role']:
                                fh.write(' role="{}"'.format(self.enc(self.programs[p]['role'][g])))
                            fh.write('>{}</actor>\n'.format(self.enc(g)))

                    self.print_credits(fh, p, "writer")
                    self.print_credits(fh, p, "producer")
                    self.print_credits(fh, p, "presenter")
                    fh.write('\t\t</credits>\n')

                date = None
                if 'movie_year' in self.programs[p]:
                    date = self.programs[p]['movie_year']
                elif 'originalAirDate' in self.programs[p] and p.startswith(('EP', 'SH')):
                    date = self.conv_oad(self.programs[p]['originalAirDate'])

                if date:
                    fh.write('\t\t<date>{}</date>\n'.format(date))

                if 'genres' in self.programs[p]:
                    for g in sorted(self.programs[p]['genres'].keys(), 
                                key=lambda x: (self.programs[p]['genres'][x], x)):
                        fh.write('\t\t<category lang="{}">{}</category>\n'.format(
                            self.lang, self.enc(g.capitalize())))

                if 'duration' in self.programs[p]:
                    fh.write('\t\t<length units="minutes">{}</length>\n'.format(
                        self.programs[p]['duration']))

                if 'imageUrl' in self.programs[p]:
                    fh.write('\t\t<icon src="{}" />\n'.format(
                        self.enc(self.programs[p]['imageUrl'])))

                if 'url' in self.programs[p]:
                    fh.write('\t\t<url>{}</url>\n'.format(
                        self.enc(self.programs[p]['url'])))

                xs = None
                xe = None

                if 'seasonNum' in self.programs[p] and 'episodeNum' in self.programs[p]:
                    s_num = self.programs[p]['seasonNum']
                    sf = "S{:0{}d}".format(int(s_num), max(2, len(str(s_num))))
                    e_num = self.programs[p]['episodeNum']
                    ef = "E{:0{}d}".format(int(e_num), max(2, len(str(e_num))))

                    xs = int(s_num) - 1
                    xe = int(e_num) - 1

                    if int(s_num) > 0 or int(e_num) > 0:
                        fh.write('\t\t<episode-num system="common">{}{}</episode-num>\n'.format(sf, ef))

                # DD Prog ID
                if re.match(r'^..\d{8}\d{4}', p):
                    dd_prog_id = "{}.{}".format(p[:10], p[10:14])
                    fh.write('\t\t<episode-num system="dd_progid">{}</episode-num>\n'.format(dd_prog_id))

                if xs is not None and xe is not None and xs >= 0 and xe >= 0:
                    fh.write('\t\t<episode-num system="xmltv_ns">{}.{}.</episode-num>\n'.format(xs, xe))

                if 'quality' in self.schedule[station][s]:
                    fh.write('\t\t<video>\n')
                    fh.write('\t\t\t<aspect>16:9</aspect>\n')
                    fh.write('\t\t\t<quality>HDTV</quality>\n')
                    fh.write('\t\t</video>\n')

                new = 'new' in self.schedule[station][s]
                live = 'live' in self.schedule[station][s]
                cc = 'cc' in self.schedule[station][s]

                if not new and not live and (p.startswith('EP') or p.startswith('SH') or re.match(r'^\d', p)):
                    fh.write('\t\t<previously-shown ')
                    if 'originalAirDate' in self.programs[p]:
                        date = self.conv_oad(self.programs[p]['originalAirDate'])
                        fh.write('start="{}000000" '.format(date))
                    fh.write('/>\n')

                if 'premiere' in self.schedule[station][s]:
                    fh.write('\t\t<premiere>{}</premiere>\n'.format(
                        self.schedule[station][s]['premiere']))

                if 'finale' in self.schedule[station][s]:
                    fh.write('\t\t<last-chance>{}</last-chance>\n'.format(
                        self.schedule[station][s]['finale']))

                if new:
                    fh.write('\t\t<new />\n')

                if hasattr(self, 'liveTag') and self.liveTag and live:
                    fh.write('\t\t<live />\n')

                if cc:
                    fh.write('\t\t<subtitles type="teletext" />\n')

                if 'rating' in self.programs[p]:
                    fh.write('\t\t<rating>\n\t\t\t<value>{}</value>\n\t\t</rating>\n'.format(
                        self.programs[p]['rating']))

                if 'starRating' in self.programs[p]:
                    fh.write('\t\t<star-rating>\n\t\t\t<value>{}/4</value>\n\t\t</star-rating>\n'.format(
                        self.programs[p]['starRating']))

                fh.write('\t</programme>\n')
                i += 1

    def print_credits(self, fh, p, role):
        if role in self.programs[p]:
            for g in sorted(self.programs[p][role].keys(), 
                           key=lambda x: self.programs[p][role][x]):
                fh.write('\t\t\t<{}>{}</{}>\n'.format(
                    role, self.enc(g), role))

    def print_header_xtvd(self, fh, enc):
        fh.write("<?xml version='1.0' encoding='{}'?>\n".format(enc))
        fh.write("<xtvd from='{}' to='{}' schemaVersion='1.3' ".format(
            self.conv_time_xtvd(self.XTVD_startTime),
            self.conv_time_xtvd(self.XTVD_endTime)))
        fh.write("xmlns='urn:TMSWebServices' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
        fh.write("xsi:schemaLocation='urn:TMSWebServices http://docs.tms.tribune.com/tech/xml/schemas/tmsxtvd.xsd'>\n")

    def print_footer_xtvd(self, fh):
        fh.write("</xtvd>\n")

    def print_stations_xtvd(self, fh):
        fh.write("<stations>\n")
        for key in sorted(self.stations.keys(), key=cmp_to_key(self.sort_chan)):
            fh.write("\t<station id='{}'>\n".format(self.stations[key]['stnNum']))
            if 'number' in self.stations[key]:
                sname = self.enc(self.stations[key]['name'])
                fh.write("\t\t<callSign>{}</callSign>\n".format(sname))
                fh.write("\t\t<name>{}</name>\n".format(sname))
                fh.write("\t\t<fccChannelNumber>{}</fccChannelNumber>\n".format(
                    self.stations[key]['number']))
                self.copy_logo(key)
            fh.write("\t</station>\n")
        fh.write("</stations>\n")

    def print_lineups_xtvd(self, fh):
        fh.write("<lineups>\n")
        fh.write("\t<lineup id='{}' name='{}' location='{}' type='{}' postalCode='{}'>\n".format(
            self.lineupId, self.lineupname, self.lineuplocation, 
            self.lineuptype, self.postalcode))
        for key in sorted(self.stations.keys(), key=cmp_to_key(self.sort_chan)):
            if 'number' in self.stations[key]:
                fh.write("\t<map station='{}' channel='{}'></map>\n".format(
                    self.stations[key]['stnNum'], self.stations[key]['number']))
        fh.write("\t</lineup>\n")
        fh.write("</lineups>\n")

    def print_schedules_xtvd(self, fh):
        fh.write("<schedules>\n")
        for station in sorted(self.schedule.keys(), key=cmp_to_key(self.sort_chan)):
            i = 0
            key_array = sorted(self.schedule[station].keys())
            
            while i < len(key_array):
                s = key_array[i]
                if i == len(key_array) - 1:
                    del self.schedule[station][s]
                    continue
                    
                p = self.schedule[station][s]['program']
                start_time = self.conv_time_xtvd(self.schedule[station][s]['time'])
                stop_time = self.conv_time_xtvd(self.schedule[station][key_array[i+1]]['time'])
                duration = self.conv_duration_xtvd(
                    self.schedule[station][key_array[i+1]]['time'] - self.schedule[station][s]['time'])
                
                fh.write("\t<schedule program='{}' station='{}' time='{}' duration='{}'".format(
                    p, self.stations[station]['stnNum'], start_time, duration))
                
                if 'quality' in self.schedule[station][s]:
                    fh.write(" hdtv='true'")
                if 'new' in self.schedule[station][s] or 'live' in self.schedule[station][s]:
                    fh.write(" new='true'")
                fh.write("/>\n")
                i += 1
        fh.write("</schedules>\n")

    def print_programs_xtvd(self, fh):
        fh.write("<programs>\n")
        for p in self.programs:
            fh.write("\t<program id='{}'>\n".format(p))
            if 'title' in self.programs[p]:
                fh.write("\t\t<title>{}</title>\n".format(
                    self.enc(self.programs[p]['title'])))
            if 'episode' in self.programs[p]:
                fh.write("\t\t<subtitle>{}</subtitle>\n".format(
                    self.enc(self.programs[p]['episode'])))
            if 'description' in self.programs[p]:
                fh.write("\t\t<description>{}</description>\n".format(
                    self.enc(self.programs[p]['description'])))
                    
            if 'movie_year' in self.programs[p]:
                fh.write("\t\t<year>{}</year>\n".format(
                    self.programs[p]['movie_year']))
            else:
                show_type = "Series"
                if 'title' in self.programs[p] and "Paid Programming" in self.programs[p]['title']:
                    show_type = "Paid Programming"
                fh.write("\t\t<showType>{}</showType>\n".format(show_type))
                fh.write("\t\t<series>EP{}</series>\n".format(p[2:10]))
                if 'originalAirDate' in self.programs[p]:
                    fh.write("\t\t<originalAirDate>{}</originalAirDate>\n".format(
                        self.conv_oad_xtvd(self.programs[p]['originalAirDate'])))
            fh.write("\t</program>\n")
        fh.write("</programs>\n")

    def print_genres_xtvd(self, fh):
        fh.write("<genres>\n")
        for p in self.programs:
            if 'genres' in self.programs[p] and 'movie' not in self.programs[p]['genres']:
                fh.write("\t<programGenre program='{}'>\n".format(p))
                for g in self.programs[p]['genres']:
                    fh.write("\t\t<genre>\n")
                    fh.write("\t\t\t<class>{}</class>\n".format(
                        self.enc(g.capitalize())))
                    fh.write("\t\t\t<relevance>0</relevance>\n")
                    fh.write("\t\t</genre>\n")
                fh.write("\t</programGenre>\n")
        fh.write("</genres>\n")

    def inc_xml(self, fh, start_tag, end_tag):
        try:
            with open(self.includeXMLTV, 'r') as xf:
                in_section = False
                for line in xf:
                    if start_tag in line:
                        in_section = True
                    if in_section and end_tag not in line:
                        fh.write(line)
                    if end_tag in line:
                        in_section = False
        except IOError as e:
            self.perr(f"Error including XML file: {e}\n")

    def help_message(self):
        help_text = f"""
zap2xml <zap2xml@gmail.com> ({VERSION})
  -u <username>
  -p <password>
  -d <# of days> (default = {DEFAULT_DAYS})
  -n <# of no-cache days> (from end)   (default = {DEFAULT_NCDAYS})
  -N <# of no-cache days> (from start) (default = {DEFAULT_NCSDAYS})
  -B <no-cache day>
  -s <start day offset> (default = 0)
  -o <output xml filename> (default = "{DEFAULT_OUTFILE}")
  -c <cacheDirectory> (default = "{DEFAULT_CACHE_DIR}")
  -l <lang> (default = "{DEFAULT_LANG}")
  -i <iconDirectory> (default = don't download channel icons)
  -m <#> = offset program times by # minutes (better to use TZ env var)
  -b = retain website channel order
  -x = output XTVD xml file format (default = XMLTV)
  -w = wait on exit (require keypress before exiting)
  -q = quiet (no status output)
  -r <# of connection retries before failure> (default = {DEFAULT_RETRIES}, max 20)
  -e = hex encode entities (html special characters like accents)
  -E "amp apos quot lt gt" = selectively encode standard XML entities
  -F = output channel names first (rather than "number name")
  -O = use old tv_grab_na style channel ids (C###nnnn.gracenote.com)
  -A "new live" = append " *" to program titles that are "new" and/or "live"
  -M = copy movie_year to empty movie sub-title tags
  -U = UTF-8 encoding (default = "ISO-8859-1")
  -L = output "<live />" tag (not part of xmltv.dtd)
  -T = don't cache files containing programs with "{self.sTBA}" titles 
  -P <http://proxyhost:port> = to use an http proxy
  -C <configuration file> (default = "{os.path.join(self.homeDir, '.zap2xmlrc')}")
  -S <#seconds> = sleep between requests to prevent flooding of server 
  -D = include details = 1 extra http request per program!
  -I = include icons (image URLs) - 1 extra http request per program!
  -J <xmltv> = include xmltv file in output
  -Y <lineupId> (if not using username/password)
  -Z <zipcode> (if not using username/password)
  -z = use tvguide.com instead of gracenote.com
  -a = output all channels (not just favorites) 
  -j = add "series" category to all non-movie programs
"""
        print(help_text)
        if sys.platform == 'win32':
            time.sleep(5)
        sys.exit(0)

if __name__ == "__main__":
    zap = Zap2XML()
    try:
        zap.run()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)