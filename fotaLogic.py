import base64
import hashlib
import hmac
import math
import random
import requests
import secrets
import ssl
import time
import urllib3
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import quote, urlparse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class LegacySSLAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.options |= getattr(ssl, 'OP_LEGACY_SERVER_CONNECT', 0x4)
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount('https://', LegacySSLAdapter())

def generate_random_token(length: int) -> str:
    byte_count = math.ceil(length / 2)
    random_bytes = secrets.token_bytes(byte_count)
    return random_bytes.hex().upper()

def make_timestamp():
    return int(time.time() * 1000)

def url_encode_with_oauth_spec(s: str) -> str:
    encoded = quote(s, safe='')
    return encoded.replace('+', '%20').replace('*', '%2A').replace('%7E', '~').replace('%25', '%')

def normalize_url_with_oauth_spec(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    authority = parsed.hostname.lower() if parsed.hostname else ''
    port = parsed.port
    if (scheme == 'http' and port == 80) or (scheme == 'https' and port == 443): pass
    elif port: authority += f':{port}'
    path = parsed.path or '/'
    query = parsed.query
    path_and_query = path + ('?' + query if query else '')
    return url_encode_with_oauth_spec(f'{scheme}://{authority}{path_and_query}')

def normalize_parameters(params: dict[str, str]) -> str:
    buffer = []
    for key, value in params.items():
        clean_value = str(value).replace('\'', '').replace('&quot;', '')
        buffer.append(f'{key}={clean_value}')
    return url_encode_with_oauth_spec('&'.join(buffer))

def compute_signature(app_secret: str, str2: str) -> bytes:
    return hmac.new(app_secret.encode('utf-8'), str2.encode('ascii'), hashlib.sha1).digest()

def generate_signature(app_secret: str, request_method: str, request_uri: str, request_body: str, oauth: dict[str, str]) -> str:
    parts = [request_method.upper(), normalize_url_with_oauth_spec(request_uri), normalize_parameters(oauth)]
    if request_body: parts.append(request_body)
    return base64.b64encode(compute_signature(app_secret, '&'.join(parts))).decode('utf-8')

def generate_oauth_header(app_id: str, app_secret: str, request_method: str, request_uri: str, request_body: str, timestamp: int | None = None) -> str:
    if timestamp is None: timestamp = make_timestamp()
    oauth = {
        'oauth_consumer_key': app_id, 'oauth_nonce': generate_random_token(10),
        'oauth_signature_method': 'HmacSHA1', 'oauth_timestamp': str(timestamp), 'oauth_version': '1.0'
    }
    oauth['oauth_signature'] = generate_signature(app_secret, request_method, request_uri, request_body, oauth)
    return ','.join(f'{k}={v}' for k, v in oauth.items())

class DesCrypt:
    MAX_CRYPT_BITS_SIZE = 64
    FP = [40, 8, 48, 16, 56, 24, 64, 32, 39, 7, 47, 15, 55, 23, 63, 31, 38, 6, 46, 14, 54, 22, 62, 30, 37, 5, 45, 13, 53, 21, 61, 29, 36, 4, 44, 12, 52, 20, 60, 28, 35, 3, 43, 11, 51, 19, 59, 27, 34, 2, 42, 10, 50, 18, 58, 26, 33, 1, 41, 9, 49, 17, 57, 25]
    IP = [58, 50, 42, 34, 26, 18, 10, 2, 60, 52, 44, 36, 28, 20, 12, 4, 62, 54, 46, 38, 30, 22, 14, 6, 64, 56, 48, 40, 32, 24, 16, 8, 57, 49, 41, 33, 25, 17, 9, 1, 59, 51, 43, 35, 27, 19, 11, 3, 61, 53, 45, 37, 29, 21, 13, 5, 63, 55, 47, 39, 31, 23, 15, 7]
    P = [16, 7, 20, 21, 29, 12, 28, 17, 1, 15, 23, 26, 5, 18, 31, 10, 2, 8, 24, 14, 32, 27, 3, 9, 19, 13, 30, 6, 22, 11, 4, 25]
    PC1_C = [57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18, 10, 2, 59, 51, 43, 35, 27, 19, 11, 3, 60, 52, 44, 36]
    PC1_D = [63, 55, 47, 39, 31, 23, 15, 7, 62, 54, 46, 38, 30, 22, 14, 6, 61, 53, 45, 37, 29, 21, 13, 5, 28, 20, 12, 4]
    PC2_C = [14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10, 23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2]
    PC2_D = [41, 52, 31, 37, 47, 55, 30, 40, 51, 45, 33, 48, 44, 49, 39, 56, 34, 53, 46, 42, 50, 36, 29, 32]
    S = [[14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7, 0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8, 4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0, 15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13], [15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10, 3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5, 0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15, 13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9], [10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8, 13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1, 13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7, 1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12], [7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15, 13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9, 10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4, 3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14], [2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9, 14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6, 4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14, 11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3], [12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11, 10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8, 9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6, 4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13], [4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1, 13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6, 1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2, 6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12], [13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7, 1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2, 7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8, 2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11]]
    E2 = [32, 1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9, 8, 9, 10, 11, 12, 13, 12, 13, 14, 15, 16, 17, 16, 17, 18, 19, 20, 21, 20, 21, 22, 23, 24, 25, 24, 25, 26, 27, 28, 29, 28, 29, 30, 31, 32, 1]
    SHIFTS = [1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1]

    def __init__(self):
        self._c = [0]*28; self._d = [0]*28; self._ks = [[0]*48 for _ in range(16)]; self._e = [0]*48; self._pre_s = [0]*48; self._crypt_crypt_byte = [0]*16

    def _init_password(self, b_arr, b_arr2):
        i = 0; i2 = 0
        while i < len(b_arr) and b_arr[i] != 0 and i2 < self.MAX_CRYPT_BITS_SIZE:
            for i3 in range(6, -1, -1): b_arr2[i2] = (b_arr[i] >> i3) & 1; i2 += 1
            i += 1; b_arr2[i2] = 0; i2 += 1
        while i2 < self.MAX_CRYPT_BITS_SIZE + 2: b_arr2[i2] = 0; i2 += 1
        return b_arr2

    def _zero_password(self, b_arr):
        for i in range(self.MAX_CRYPT_BITS_SIZE + 2): b_arr[i] = 0
        return b_arr

    def _set_key(self, b_arr):
        for i in range(28): self._c[i] = b_arr[self.PC1_C[i] - 1]; self._d[i] = b_arr[self.PC1_D[i] - 1]
        for i2 in range(16):
            for _ in range(self.SHIFTS[i2]):
                b = self._c[0]; 
                for i4 in range(27): self._c[i4] = self._c[i4 + 1]
                self._c[27] = b; b2 = self._d[0]
                for i6 in range(27): self._d[i6] = self._d[i6 + 1]
                self._d[27] = b2
            for i8 in range(24): self._ks[i2][i8] = self._c[self.PC2_C[i8] - 1]; self._ks[i2][i8 + 24] = self._d[self.PC2_D[i8] - 28 - 1]
        for i9 in range(48): self._e[i9] = self.E2[i9]

    def _e_expandsion(self, b_arr):
        i = 0; i2 = 0
        while i < 2:
            i3 = i2 + 1; b = b_arr[i2]; self._crypt_crypt_byte[i] = b; b2 = b - 59 if b > 90 else (b - 53 if b > 57 else b - 46)
            for i4 in range(6):
                if ((b2 >> i4) & 1) != 0: i5 = (i * 6) + i4; b3 = self._e[i5]; i6 = i5 + 24; self._e[i5] = self._e[i6]; self._e[i6] = b3
            i += 1; i2 = i3

    def _des_encrypt(self, b_arr):
        b_arr2 = [0]*32; b_arr3 = [0]*32; b_arr4 = [0]*32; b_arr5 = [0]*32; i = 0
        while i < 32: b_arr2[i] = b_arr[self.IP[i] - 1]; i += 1
        while i < 64: b_arr3[i - 32] = b_arr[self.IP[i] - 1]; i += 1
        for i2 in range(16):
            for i3 in range(32): b_arr4[i3] = b_arr3[i3]
            for i4 in range(48): self._pre_s[i4] = b_arr3[self._e[i4] - 1] ^ self._ks[i2][i4]
            for i5 in range(8):
                b = i5 * 6
                b2 = self.S[i5][(self._pre_s[b] << 5) + (self._pre_s[b + 1] << 3) + (self._pre_s[b + 2] << 2) + (self._pre_s[b + 3] << 1) + self._pre_s[b + 4] + (self._pre_s[b + 5] << 4)]
                b3 = i5 * 4; b_arr5[b3] = (b2 >> 3) & 1; b_arr5[b3 + 1] = (b2 >> 2) & 1; b_arr5[b3 + 2] = (b2 >> 1) & 1; b_arr5[b3 + 3] = b2 & 1
            for i6 in range(32): b_arr3[i6] = b_arr2[i6] ^ b_arr5[self.P[i6] - 1]
            for i7 in range(32): b_arr2[i7] = b_arr4[i7]
        for i8 in range(32): b4 = b_arr2[i8]; b_arr2[i8] = b_arr3[i8]; b_arr3[i8] = b4
        for i9 in range(64):
            if self.FP[i9] < 33: b_arr[i9] = b_arr2[self.FP[i9] - 1]
            else: b_arr[i9] = b_arr3[self.FP[i9] - 33]
        return b_arr

    def _encrypt(self, b_arr):
        for _ in range(25): b_arr = self._des_encrypt(b_arr)
        i2 = 0
        while i2 < 11:
            b = 0; 
            for i3 in range(6): b = (b << 1) | b_arr[(i2 * 6) + i3]
            b2 = b + 46; 
            if b2 > 57: b2 += 7
            if b2 > 90: b2 += 6
            self._crypt_crypt_byte[i2 + 2] = b2; i2 += 1
        self._crypt_crypt_byte[i2 + 2] = 0
        if self._crypt_crypt_byte[1] == 0: self._crypt_crypt_byte[1] = self._crypt_crypt_byte[0]

    def generate(self, s, b_arr):
        init_pwd = self._init_password(s.encode('utf-8'), [0] * (self.MAX_CRYPT_BITS_SIZE + 2))
        if init_pwd: self._set_key(init_pwd); zero_pwd = self._zero_password(init_pwd); self._e_expandsion(b_arr); self._encrypt(zero_pwd)
        result_bytes = bytes(self._crypt_crypt_byte)
        try: null_index = result_bytes.index(0); return result_bytes[:null_index].decode('utf-8')
        except: return result_bytes.decode('utf-8', errors='ignore').strip('\x00')

DICT = [1, 15, 5, 11, 19, 28, 23, 47, 35, 44, 2, 14, 6, 10, 18, 13, 22, 26, 32, 47, 3, 13, 7, 9, 17, 30, 21, 25, 33, 45, 4, 12, 8, 63, 16, 31, 20, 24, 34, 46]
HEX_TABLE = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

def _adp_encode_hex(b_arr):
    c_arr = [''] * (len(b_arr) * 2); i = 0
    for b in b_arr:
        i2 = i + 1; c_arr[i] = HEX_TABLE[b & 15]; c_arr[i2] = HEX_TABLE[(b >> 4) & 15]; i = i2 + 1
    return c_arr

def _adp_shuffle(var0):
    var1 = len(var0); var2 = var1 % 2; var3 = var1 // 2
    if var2 != 0: var3 += 1
    char_list = list(var0)
    while var3 < var1:
        var4 = char_list.pop(var3); var5 = var1 - var3
        if var2 == 0: var5 -= 1
        char_list.insert(var5, var4); var3 += 1
    return ''.join(char_list)

def generate_client_password(str_val, str2):
    try:
        if ':' not in str_val: return None
        substring = str_val.split(':', 1)[1]
        if not substring: return None
        c_arr = [char for char in substring if char.isalnum()]
        i = len(c_arr); j = 0; j2 = 0
        for i3 in range(i - 1):
            j3 = ord(c_arr[i3]); b_arr = DICT; j += j3 * b_arr[i3]; j2 += ord(c_arr[i3]) * ord(c_arr[(i - i3) - 1]) * b_arr[i3]
        dev_pwd_key = f'{j}{j2}'
        if not dev_pwd_key: return None
        data_to_hash = (str2 + dev_pwd_key + str_val).encode('utf-8')
        md5_hash = hashlib.md5(data_to_hash).digest()
        hex_chars = _adp_encode_hex(md5_hash)
        str_val_bytes = str_val.encode('utf-8')
        salt_bytes = bytes([str_val_bytes[len(str_val_bytes) - 2], str_val_bytes[len(str_val_bytes) - 1]])
        descrypt = DesCrypt(); des_part = descrypt.generate(str_val, salt_bytes)
        concat = ''.join([hex_chars[1], hex_chars[4], hex_chars[5], hex_chars[7]]) + des_part
        string_buffer = concat
        for _ in range(3): string_buffer = _adp_shuffle(string_buffer)
        return string_buffer
    except: return ''

TOKENS_SYNCML = {'SyncML': b'\x6d', 'SyncHdr': b'\x6c', 'SyncBody': b'\x6b', 'VerDTD': b'\x71', 'VerProto': b'\x72', 'SessionID': b'\x65', 'MsgID': b'\x5b', 'Target': b'\x6e', 'Source': b'\x67', 'LocURI': b'\x57', 'LocName': b'\x56', 'Cred': b'\x4e', 'Meta': b'\x5a', 'Data': b'\x4f', 'Alert': b'\x46', 'CmdID': b'\x4b', 'Item': b'\x54', 'Status': b'\x69', 'Results': b'\x62', 'Cmd': b'\x4a', 'CmdRef': b'\x4c', 'MsgRef': b'\x5c', 'TargetRef': b'\x6f', 'SourceRef': b'\x68', 'Final': b'\x12', 'Replace': b'\x60'}
TOKENS_METINF = {'Format': b'\x47', 'Type': b'\x53', 'MaxMsgSize': b'\x4c', 'MaxObjSize': b'\x55', 'Size': b'\x52'}
SWITCH_PAGE = b'\x00'; END = b'\x01'; STR_I = b'\x03'; CP_SYNCML = 0; CP_METINF = 1

class SyncML:
    def __init__(self):
        self.next_cmd_id = 1; self.wbxml = bytearray(); self._write_header()
        self._start_element(TOKENS_SYNCML['SyncML']); self._start_header('1.2', 'DM/1.2')
        self.body_started = False

    def _write_header(self):
        public_id = b'-//SYNCML//DTD SyncML 1.2//EN'; self.wbxml.extend(b'\x02\x00\x00j')
        self.wbxml.append(len(public_id)); self.wbxml.extend(public_id)

    def _switch_page(self, page): self.wbxml.extend(SWITCH_PAGE); self.wbxml.append(page)
    def _start_element(self, token): self.wbxml.extend(token)
    def _end_element(self): self.wbxml.extend(END)
    def _add_leaf(self, token, text = None):
        self.wbxml.extend(token)
        if text is not None: self.wbxml.extend(STR_I); self.wbxml.extend(text.encode('utf-8')); self.wbxml.append(0)
        self.wbxml.extend(END)

    def _start_header(self, ver_dtd, ver_proto):
        self._start_element(TOKENS_SYNCML['SyncHdr']); self._add_leaf(TOKENS_SYNCML['VerDTD'], ver_dtd); self._add_leaf(TOKENS_SYNCML['VerProto'], ver_proto)

    def add_header(self, session_id, msg_id, target_uri, source_uri, cred_data, max_msg_size = 5120, max_obj_size = 1048576):
        self._add_leaf(TOKENS_SYNCML['SessionID'], session_id); self._add_leaf(TOKENS_SYNCML['MsgID'], str(msg_id))
        self._start_element(TOKENS_SYNCML['Target']); self._add_leaf(TOKENS_SYNCML['LocURI'], target_uri); self._end_element()
        self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], source_uri); self._add_leaf(TOKENS_SYNCML['LocName'], source_uri); self._end_element()
        if cred_data:
            self._start_element(TOKENS_SYNCML['Cred']); self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF)
            self._add_leaf(TOKENS_METINF['Format'], 'b64'); self._add_leaf(TOKENS_METINF['Type'], 'syncml:auth-md5'); self._switch_page(CP_SYNCML); self._end_element()
            self._add_leaf(TOKENS_SYNCML['Data'], cred_data); self._end_element()
        self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['MaxMsgSize'], str(max_msg_size)); self._add_leaf(TOKENS_METINF['MaxObjSize'], str(max_obj_size)); self._switch_page(CP_SYNCML); self._end_element()
        self._end_element(); self._start_element(TOKENS_SYNCML['SyncBody']); self.body_started = True

    def add_alert(self, data, item_uri = None, item_data = None, item_type = None, item_format = 'chr'):
        self._start_element(TOKENS_SYNCML['Alert']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id)); self._add_leaf(TOKENS_SYNCML['Data'], data)
        if item_uri:
            self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], item_uri); self._end_element()
            if item_type: self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['Format'], item_format); self._add_leaf(TOKENS_METINF['Type'], item_type); self._switch_page(CP_SYNCML); self._end_element()
            if item_data is not None: self._add_leaf(TOKENS_SYNCML['Data'], item_data)
            self._end_element()
        self._end_element(); self.next_cmd_id += 1

    def add_replace(self, items):
        self._start_element(TOKENS_SYNCML['Replace']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id))
        for uri, data in items.items():
            self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], uri); self._end_element(); self._add_leaf(TOKENS_SYNCML['Data'], data); self._end_element()
        self._end_element(); self.next_cmd_id += 1

    def add_status(self, data, msg_ref = None, cmd_ref = None, cmd = None, target_ref = None, source_ref = None):
        self._start_element(TOKENS_SYNCML['Status']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id)); self._add_leaf(TOKENS_SYNCML['Data'], data)
        if msg_ref is not None: self._add_leaf(TOKENS_SYNCML['MsgRef'], str(msg_ref))
        if cmd_ref is not None: self._add_leaf(TOKENS_SYNCML['CmdRef'], str(cmd_ref))
        if cmd: self._add_leaf(TOKENS_SYNCML['Cmd'], cmd)
        if target_ref: self._add_leaf(TOKENS_SYNCML['TargetRef'], target_ref)
        if source_ref: self._add_leaf(TOKENS_SYNCML['SourceRef'], source_ref)
        self._end_element(); self.next_cmd_id += 1

    def add_results(self, loc_uri, data, msg_ref = None, cmd_ref = None, data_type = 'text/plain', data_format = 'chr'):
        self._start_element(TOKENS_SYNCML['Results']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id))
        self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], loc_uri); self._end_element()
        self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['Format'], data_format); self._add_leaf(TOKENS_METINF['Type'], data_type); self._add_leaf(TOKENS_METINF['Size'], str(len(data))); self._switch_page(CP_SYNCML); self._end_element()
        self._add_leaf(TOKENS_SYNCML['Data'], data); self._end_element()
        if msg_ref is not None: self._add_leaf(TOKENS_SYNCML['MsgRef'], str(msg_ref))
        if cmd_ref is not None: self._add_leaf(TOKENS_SYNCML['CmdRef'], str(cmd_ref))
        self._end_element(); self.next_cmd_id += 1

    def get(self): self._add_leaf(TOKENS_SYNCML['Final']); self._end_element(); return bytes(self.wbxml)

def send_wbxml(url: str, wbxml: bytes, device_model: str) -> bytes:
    headers = {'User-Agent': f'Samsung {device_model} SyncML_DM Client', 'Accept': 'application/vnd.syncml.dm+wbxml', 'Content-Type': 'application/vnd.syncml.dm+wbxml'}
    response = session.post(url, data = wbxml, headers = headers)
    if not response.ok: raise requests.HTTPError(f'HTTP {response.status_code}: {response.reason}', response = response)
    return response.content

class Client:
    def __init__(self, data: dict):
        self.Model = data.get('Model', ''); self.DeviceId = data.get('DeviceId', ''); self.CustomerCode = data.get('CustomerCode', '')
        self.SerialNumber = data.get('SerialNumber', ''); self.FirmwareVersion = data.get('FirmwareVersion', '')
        self.Mcc = data.get('Mcc', '001'); self.Mnc = data.get('Mnc', '01'); self.FotaClientVersion = data.get('FotaClientVersion', '4.4.14')
        self.Registered = data.get('Registered', False)
        now = datetime.fromtimestamp(time.time()); self.ssid = format(now.minute, 'X') + format(now.second, 'X')
        self.nonce = b''; self.CurrentMessageId = 1
        self.generate_password()

    def compute_md5_auth(self):
        if not self.nonce or self.CurrentMessageId == 1: self.nonce = base64.b64decode(base64.b64encode((str(random.randint(0, 2**31 - 1)) + 'SSNextNonce').encode('utf-8')).decode('utf-8'))
        concat_str = f'{self.DeviceId}:{self.ClientPassword}'; concat2_str = f'{base64.b64encode(hashlib.md5(concat_str.encode('utf-8')).digest()).decode('utf-8')}:'
        combined_b_arr = concat2_str.encode('utf-8') + self.nonce
        return base64.b64encode(hashlib.md5(combined_b_arr).digest()).decode('utf-8')

    def set_server_nonce(self, nonce_b64): self.nonce = base64.b64decode(nonce_b64); self.CurrentMessageId += 1
    def generate_password(self): self.ClientPassword = generate_client_password(self.DeviceId, 'x6g1q14r75')

    def build_device_request(self, url):
        b = SyncML()
        b.add_header(self.ssid, self.CurrentMessageId, url, self.DeviceId, self.compute_md5_auth())
        b.add_alert('1201')
        b.add_replace({
            './DevInfo/DevId': self.DeviceId, './DevInfo/Man': 'Samsung', './DevInfo/Mod': self.Model, './DevInfo/DmV': '1.2', './DevInfo/Lang': 'en-US', 
            './DevInfo/Ext/DevNetworkConnType': 'WIFI', 
            './DevInfo/Ext/TelephonyMcc': self.Mcc, './DevInfo/Ext/TelephonyMnc': self.Mnc, 
            './DevInfo/Ext/OmcCode': self.CustomerCode, './DevInfo/Ext/FotaClientVer': self.FotaClientVersion, 
            './DevInfo/Ext/DMClientVer': self.FotaClientVersion, './DevInfo/Ext/ModemZeroBilling': '1', 
            './DevInfo/Ext/SIMCardMcc': self.Mcc, './DevInfo/Ext/SIMCardMnc': self.Mnc, 
            './DevInfo/Ext/AidCode': self.CustomerCode, './DevInfo/Ext/CountryISOCode': 'sk'
        })
        b.add_alert('1226', './FUMO/DownloadAndUpdate', '0', 'org.openmobilealliance.dm.firmwareupdate.devicerequest')
        return b.get()

    def build_update_request(self, url, fwv):
        b = SyncML(); 
        b.add_header(self.ssid, self.CurrentMessageId, url, self.DeviceId, '')
        ref = self.CurrentMessageId - 1
        b.add_status('212', ref, 0, 'SyncHdr', self.DeviceId, url.split('?')[0])
        b.add_status('200', ref, 5, 'Get', './DevDetail/FwV'); b.add_results('./DevDetail/FwV', fwv, ref, 5)
        b.add_status('200', ref, 6, 'Get', './DevInfo/Ext/DevNetworkConnType'); b.add_results('./DevInfo/Ext/DevNetworkConnType', 'WIFI', ref, 6)
        return b.get()

    def do_auth(self):
        url = 'https://dms.ospserver.net/v1/device/magicsync/mdm'
        for _ in range(5):
            resp = send_wbxml(url, self.build_device_request(url), self.Model)
            ns = b'SyncHdr\x00'; ni = resp.find(ns)
            if ni != -1:
                ni += len(ns); nei = resp.find(b'\x00', ni); sn = resp[ni:nei].decode('utf-8')
                cs = nei + 1; ce = resp.find(b'\x00', cs); sc = resp[cs:ce].decode('utf-8')
            else: sn = None; sc = None
            
            end = b'\x00b64'; ei = resp.find(end)
            if ei != -1:
                si = resp.rfind(b'\x00', 0, ei - 1)
                r_url = resp[si+1:ei].decode('utf-8') if si != -1 else None
            else: r_url = None
            
            url = r_url
            if sn == '425': return 'AUTH BANNED'
            elif sn and sn != '401':
                self.set_server_nonce(sn)
                if sc != '401': return url
        return 'AUTH FAILED'

    def check_update(self, fvw):
        url = self.do_auth()
        if not url or 'http' not in url: 
            return url
        
        resp = send_wbxml(url, self.build_update_request(url, fvw), self.Model)
        if b'DevInfo/Ext/DeviceRegistrationStatus' in resp: return 'BAD CSC'
        
        ps = b'chr\x00'; pe = b'\x00'; pi = resp.find(ps)
        if pi == -1: return 'NO PKG MARKER'
        
        pi += len(ps); pei = resp.find(pe, pi)
        ret = resp[pi:pei].decode('utf-8')
        
        if ret == '260': return 'NO UPDATE AVAILABLE'
        if ret == '261': return 'UNKNOWN'
        if ret == '220': return 'UNKNOWN FIRMWARE'
        
        return ret
    
class DescriptorInfo:
    def __init__(self):
        self.status = '?'
        self.baseVersion = '?'
        self.targetVersion = '?'
        self.securityPatches = '?'
        self.androidVersion = '?'
        self.oneUIVersion = '?'
        self.size = '?'
        self.downloadURL = '?'

    def fetch(self, model, imei, csc, base):
        try:
            mcc, mnc = ('460', '01') if csc in ['CHC','CHM'] else ('310', '410')

            clientRet = Client({
                'Model': f'SM-{model}', 'DeviceId': f'IMEI:{imei}', 'CustomerCode': csc, 'FirmwareVersion': base, 'Registered': True, 'Mcc': mcc, 'Mnc': mnc
            }).check_update(base)

            if not clientRet:
                return self

            if not clientRet.startswith('http'):
                self.status = clientRet
                return self
            
            response = requests.get(clientRet, timeout = 7)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            ns = {'dd': 'http://www.openmobilealliance.org/xmlns/dd'}
            param_el = root.find('dd:installParam', ns)
            param_str = param_el.text if param_el is not None else ''
            param_map = {}
            if param_str:
                for item in param_str.split(';'):
                    if '=' in item:
                        k, v = item.split('=', 1)
                        param_map[k.strip().lower()] = v.strip()

            self.status = 'SUCCESS'
            self.baseVersion = base
            self.targetVersion = param_map['updatefwv']
            self.securityPatches = param_map['securitypatchversion']
            self.androidVersion = param_map['updatefwosv'].replace('B(', '').replace(')', '')
            if v := param_map.get('updateoneuiversion'):
                self.oneUIVersion = f'OneUI {v}'
            if s := root.find('dd:size', ns).text:
                self.size = f'{str(round(int(s) / (1024 * 1024), 2))}'
            self.downloadURL = root.find('dd:objectURI', ns).text
            
            return self
        except Exception:
            self.status = 'IP BANNED'
            return self